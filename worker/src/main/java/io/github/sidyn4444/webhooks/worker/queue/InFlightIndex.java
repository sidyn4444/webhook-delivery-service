package io.github.sidyn4444.webhooks.worker.queue;

import io.github.sidyn4444.webhooks.common.queue.RedisKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * Records when each in-flight job was picked up, and forgets it when the job is let go.
 *
 * <p>The whole class is two operations against one Redis sorted set — add on pickup, remove on
 * release — plus a count for metrics. Nothing else in the system knows the key exists.
 *
 * <h2>Why it has to exist</h2>
 *
 * <p>Since 13b every job has an ending: delivered and acked, scheduled for a retry, or
 * dead-lettered. Nothing is left parked by design any more. That gives {@code webhooks:processing}
 * a new and much stronger meaning — <b>a job sitting there means a worker died holding it</b> —
 * and it is the signal the recovery sweep looks for (14b).
 *
 * <p>But the list alone cannot support that conclusion. A job being delivered right now and a job
 * abandoned by a pod that died an hour ago are the same string in the same list; there is no field
 * to tell them apart. Re-queueing on the first reading would re-deliver everything currently in
 * flight, on every pass.
 *
 * <p>What is missing is a clock reading, and this class is it. Delivery is capped at ten seconds
 * (9c), so a pickup time is enough to make abandonment provable rather than guessed: anything
 * older than sixty seconds cannot still be running.
 *
 * <h2>Why it does not record WHO picked the job up</h2>
 *
 * <p>The obvious companion fact is the worker's identity, and it was deliberately left out.
 * <b>Ownership without liveness is not information.</b> Knowing that {@code worker-7} holds a job
 * says nothing about whether {@code worker-7} still exists — to use the name you would need every
 * worker publishing a heartbeat and something checking it, which is a second coordination system
 * bolted onto a design that currently needs none. Time answers the question directly and requires
 * no cooperation from anyone.
 *
 * <p>It also keeps the entries collapsible. A sorted set holds unique members, so when the sweep
 * adopts an orphan by writing a timestamp for it, the real worker's own write later simply
 * overwrites that score instead of creating a second entry (14b). Prefixing the member with a
 * worker name would break that, and the sweep would then have to clean up after itself.
 *
 * <h2>The one rule for callers</h2>
 *
 * <p><b>A failure here must never stop the work.</b> This index is a failsafe; the processing list
 * is the source of truth. That is the reverse of {@code RetryQueue.schedule} (12c) and
 * {@code DeadLetterQueue.deadLetter} (13b), where a failed write must stop the caller from
 * releasing the job — because in those two the write IS the job's only future. Here the job is
 * already safe in the list, and a missing timestamp costs nothing but a sweep that has to adopt it
 * first. So these methods report failure and the poll loop carries on regardless.
 */
@Component
public class InFlightIndex {

    private static final Logger log = LoggerFactory.getLogger(InFlightIndex.class);

    private final StringRedisTemplate redis;

    public InFlightIndex(StringRedisTemplate redis) {
        this.redis = redis;
    }

    /**
     * Notes that this exact job is now in flight, as of this moment.
     *
     * <p><b>The timestamp is read here rather than passed in</b>, unlike
     * {@code RetryQueue.schedule(job, dueAt)}. The difference is what kind of value it is. A retry
     * due-time is a <i>decision</i> — {@code RetryBackoff} computed it, and it could legitimately
     * be any of several values — so it has to come from outside or the schedule cannot be asserted
     * against an expected answer. A pickup time is an <i>observation</i>, and there is exactly one
     * correct value: the instant the write happens. Accepting it as a parameter would only create a
     * way to pass a stale one, computed before some slower step, which biases the sweep toward
     * believing a healthy job has been abandoned.
     *
     * <p>The cost is that a test cannot assert an exact score. It asserts the score falls between
     * two timestamps taken around the call instead, which is the standard way to test anything that
     * reads a clock.
     *
     * <p>⚠️ The clock is this worker's, and a <i>different</i> worker reads the value during the
     * sweep — so the measurement crosses a machine boundary. Against a sixty-second threshold on
     * NTP-synchronised nodes, where skew is milliseconds, that is irrelevant. It stops being
     * irrelevant if the threshold gets tight or the clocks stop being trustworthy, and the fix then
     * is to take the reading from Redis itself with {@code TIME} so there is a single authoritative
     * clock, at the cost of an extra round trip on every pickup.
     *
     * @param json the EXACT string that was moved into the processing list. It has to be the same
     *             bytes: the sweep uses this value to remove the job from that list, and
     *             {@code LREM} matches on byte equality (notes 9d).
     * @return {@code true} if the pickup time was recorded. A {@code false} is worth logging and is
     *         deliberately not worth aborting for — see the class comment.
     */
    public boolean record(String json) {
        try {
            long now = Instant.now().toEpochMilli();

            // ZADD key score member. The return value distinguishes "a new member was added" from
            // "an existing member's score was updated", and BOTH are success here — the same trap
            // as the ZADD in RetryQueue (12c), where treating false as failure would have produced
            // a stream of phantom errors.
            //
            // A score update is in fact the normal case for a job the sweep has already adopted:
            // the sweep wrote a placeholder timestamp, and this overwrites it with the real one.
            Boolean added = redis.opsForZSet().add(RedisKeys.INFLIGHT, json, now);

            if (added == null) {
                // Spring returns null when the command could not be completed. It does not throw,
                // so an unchecked call here would be indistinguishable from success.
                log.error("Could not record a pickup time in '{}' — Redis returned no result. The "
                                + "job is still in '{}' and the sweep will adopt it.",
                        RedisKeys.INFLIGHT, RedisKeys.PROCESSING);
                return false;
            }

            return true;

        } catch (Exception e) {
            // Deliberately broad, and deliberately not rethrown. An exception escaping this method
            // would travel up into the poll loop's catch, cost a two-second backoff, and prevent a
            // delivery that has nothing wrong with it — all to protect a bookkeeping entry the
            // sweep already knows how to reconstruct.
            log.error("FAILED to record a pickup time in '{}' — the job is still in '{}' and the "
                            + "sweep will adopt it: {}",
                    RedisKeys.INFLIGHT, RedisKeys.PROCESSING, e.toString());
            return false;
        }
    }

    /**
     * Drops the entry for a job that has left the processing list.
     *
     * <p><b>Call this only after a confirmed removal from {@code processing}</b>, never before and
     * never speculatively. The index mirrors that list, so it should change exactly when the list
     * changes. In particular, if an {@code LREM} returns 0 — a successful command that removed
     * nothing, the trap from 9d — the job is still parked, and its entry here must stay so the
     * sweep can find it.
     *
     * <p><b>Order matters, and the safe order is: remove from {@code processing} first, then call
     * this.</b> A crash between the two leaves an entry here for a job that is no longer in the
     * list — harmless bookkeeping the sweep discards, because it verifies a job is still parked
     * before re-queueing it. The other order leaves a job in the list with no timestamp, which the
     * sweep adopts and then, sixty seconds later, re-delivers. One ordering produces garbage; the
     * other produces a duplicate delivery.
     *
     * @param json the same exact string that was passed to {@link #record}
     */
    public void forget(String json) {
        try {
            Long removed = redis.opsForZSet().remove(RedisKeys.INFLIGHT, json);

            if (removed == null) {
                log.warn("Could not clear a pickup time from '{}' — Redis returned no result. A "
                                + "stale entry may be left behind.",
                        RedisKeys.INFLIGHT);
                return;
            }

            if (removed == 0) {
                // 🔴 Zero is NOT an error here, and this is the interesting difference from the
                // identical-looking check on LREM in 9d.
                //
                // There, a zero meant a delivered job had failed to be acked and would be
                // re-delivered — a real defect. Here it means only that no timestamp existed to
                // remove, which happens legitimately whenever the pickup write failed or the
                // process crashed between the pop and the record. The job is being released
                // correctly either way.
                //
                // Same return value, same shape, opposite severity — because one key is the source
                // of truth and the other is a failsafe. Logged at debug so a burst of them is
                // discoverable without being noise on a healthy system.
                log.debug("No pickup time to clear from '{}' — there was no entry for this job.",
                        RedisKeys.INFLIGHT);
            }

        } catch (Exception e) {
            // As above: a bookkeeping failure must not propagate into the delivery path. The worst
            // outcome is a stale entry, and the sweep is required to tolerate those anyway.
            log.error("FAILED to clear a pickup time from '{}' — a stale entry is left behind: {}",
                    RedisKeys.INFLIGHT, e.toString());
        }
    }

    /**
     * How many jobs are currently recorded as in flight.
     *
     * <p>Read alongside the length of {@code webhooks:processing}, this is a direct measure of how
     * healthy the pair is: the two numbers should track each other closely, and a persistent gap
     * means jobs are being picked up without their timestamp landing — orphans accumulating faster
     * than the sweep adopts them. It becomes a Prometheus gauge in Week 6.
     *
     * @return the count, or {@code -1} if Redis could not be reached
     */
    public long size() {
        try {
            Long count = redis.opsForZSet().zCard(RedisKeys.INFLIGHT);
            return count == null ? -1 : count;
        } catch (Exception e) {
            log.warn("Could not read the size of '{}': {}", RedisKeys.INFLIGHT, e.toString());
            return -1;
        }
    }
}
