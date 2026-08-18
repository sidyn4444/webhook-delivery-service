package io.github.sidyn4444.webhooks.worker.retry;

import io.github.sidyn4444.webhooks.common.model.DeliveryJob;
import io.github.sidyn4444.webhooks.common.queue.JobCodec;
import io.github.sidyn4444.webhooks.common.queue.RedisKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;

/**
 * Where a job waits between attempts.
 *
 * <p>The obvious way to implement "wait four seconds and try again" is to wait four seconds.
 * That is wrong twice over, and both reasons are worth being able to state.
 *
 * <p><b>It blocks a worker.</b> The poll loop is a single thread handling one job at a time
 * (notes 9b), so a worker sleeping through a backoff is a worker delivering nothing else.
 * Concurrency in this system comes from running more pods, and a pod asleep is a pod that was
 * paid for and is doing nothing. During a subscriber outage — precisely when the queue is
 * longest — every worker would spend most of its time asleep.
 *
 * <p><b>And the wait only exists in memory.</b> A pod killed mid-sleep takes the timer with it.
 * Kubernetes restarts pods constantly, so "we'll retry in 16 seconds" would be a promise that
 * quietly evaporates on every rollout, and the job would sit in {@code processing} with nothing
 * scheduled to touch it.
 *
 * <p>The fix is to stop treating the delay as something to <i>do</i> and start treating it as
 * something to <i>record</i>: write the job into Redis with the clock time it becomes due, and
 * move on immediately. The waiting is then a property of the data rather than the behaviour of a
 * thread — durable, and free.
 *
 * <p>This class owns the write side only. Reading due jobs back out and returning them to the
 * main queue is the scheduler's job (12d).
 */
@Component
public class RetryQueue {

    private static final Logger log = LoggerFactory.getLogger(RetryQueue.class);

    /**
     * The atomic "claim everything that is due and move it to the queue" operation.
     *
     * <p>Loaded from {@code resources/scripts/claim-due-retries.lua} rather than pasted into a
     * Java string, for the ordinary reasons a query belongs in its own file: it stays readable,
     * a reviewer can see it as Lua, and an editor can highlight it. The script itself explains
     * why the operation has to be atomic.
     *
     * <p><b>How Spring runs it:</b> the first call sends the whole script with {@code EVAL} and
     * Redis remembers it against a hash of its text. Every later call sends only that hash, with
     * {@code EVALSHA} — so the script text crosses the network once per Redis restart, not once
     * per second. Spring handles the fallback automatically if Redis has forgotten it.
     *
     * <p>Static because the script is the same for every instance and reading a classpath
     * resource once is enough.
     */
    private static final RedisScript<List> CLAIM_DUE_SCRIPT =
            RedisScript.of(new ClassPathResource("scripts/claim-due-retries.lua"), List.class);

    private final StringRedisTemplate redis;

    /**
     * Counts retries scheduled — the metric that turns "we implemented exponential backoff" into a
     * number.
     *
     * <p><b>Why this is worth an instrument of its own rather than being inferred.</b> It is tempting
     * to derive the retry volume from the delivery timer: total attempts minus distinct events ought
     * to be the retries. That arithmetic is wrong in both directions. It over-counts, because a
     * reclaimed job re-delivered after a worker crash is a second attempt that was never a
     * <i>retry</i> — nothing failed, a pod died. And it under-counts, because a scheduled retry that
     * is still sitting on backoff has produced no attempt at all yet and so appears nowhere.
     * <b>Deriving a domain number from transport metrics gives an answer that is confidently and
     * unfalsifiably wrong, which is worse than having no answer.</b>
     *
     * <p>🔴 <b>The pairing to understand — and it is the same shape as the DLQ pair.</b>
     * {@code webhook.retry.depth} is the gauge: <i>how many are waiting on backoff right now</i>.
     * This counter is <i>how many were ever scheduled</i>. A retry set that is empty right now and a
     * retry set that has never been used read <b>identically</b> on the gauge, and are opposite
     * situations — one system is coping, the other has never been tested. Only the counter separates
     * them, and it is the reason a "retries" panel needs both series.
     *
     * <p><b>Untagged, deliberately.</b> The obvious label is the attempt number, which would show the
     * backoff ladder as a distribution — genuinely interesting. It is left out because it is bounded
     * only by {@code maxRetries}, which lives on each job rather than in configuration (7b/12e), so
     * a job enqueued under a different ceiling widens the label set with no code change. <b>A label
     * whose value set is decided by data in flight is the 32a defect in a smaller costume.</b>
     */
    private final Counter retriesScheduled;

    public RetryQueue(StringRedisTemplate redis, MeterRegistry registry) {
        this.retriesScheduled = Counter.builder("webhook.retries.scheduled")
                .description("Delivery attempts that failed and were scheduled for another try")
                .register(registry);
        this.redis = redis;
    }

    /**
     * Schedules a job for a future attempt.
     *
     * <p><b>The caller supplies the due time rather than a delay</b>, and that is deliberate: a
     * method that calls {@code Instant.now()} internally cannot be tested against an expected
     * value, because its answer depends on when it ran. Taking the clock as a parameter keeps
     * the unpredictable part at the edge, which is the same reasoning that split
     * {@code RetryBackoff} into a deterministic half and a random one (notes 12b).
     *
     * <p><b>The due time is absolute, not relative.</b> Storing "4 seconds" would require the
     * reader to also know when it was stored; storing "due at 14:32:09.881" lets the scheduler
     * ask a single question — <i>anything below now?</i> — without knowing anything about how any
     * of these jobs got here.
     *
     * @param job    the job as it should be attempted next — the caller has already incremented
     *               the attempt number, because what is stored here is the <i>next</i> attempt
     * @param dueAt  when this job becomes eligible to run again
     * @return {@code true} if the schedule was recorded, {@code false} if it could not be
     */
    public boolean schedule(DeliveryJob job, Instant dueAt) {
        try {
            String json = JobCodec.toJson(job);

            // The score is the due time in epoch milliseconds — milliseconds since 1970.
            //
            // Redis scores are double-precision floats, which sounds like a problem for a number
            // this large and is not: a double represents every integer up to 2^53 exactly, and
            // epoch milliseconds are around 1.75 x 10^12 — three orders of magnitude below that
            // limit, and will be until the year 287396. No rounding occurs. Worth knowing
            // because "we stored a timestamp in a float" is the kind of thing that sounds wrong
            // and would be, in seconds-with-fractions or in nanoseconds.
            double score = dueAt.toEpochMilli();

            Boolean newlyAdded = redis.opsForZSet().add(RedisKeys.RETRY, json, score);

            if (newlyAdded == null) {
                // Spring returns null when the command could not be completed. Not an exception,
                // so an unchecked call here would look exactly like success.
                log.error("Could not schedule retry for event {} — Redis returned no result", job.eventId());
                return false;
            }

            // 🔴 false does NOT mean failure. A sorted set holds unique values, so adding one
            // that is already present updates its score instead of creating a second entry, and
            // Boolean.FALSE means exactly that. Treating it as an error would produce a stream of
            // phantom failures the first time the same job is scheduled twice — and scheduling
            // the same job twice is a normal consequence of at-least-once delivery, not a bug.
            //
            // The behaviour is also the one we want: two schedules of the same attempt collapse
            // into one entry rather than causing two deliveries. Uniqueness is doing real work
            // here, not just failing to be a list.
            if (!newlyAdded) {
                log.info("Retry for event {} attempt {} was already scheduled — due time updated to {}",
                        job.eventId(), job.attemptNumber(), dueAt);
                return true;
            }

            log.info("Scheduled event {} attempt {} of {} for retry at {} (in {}ms)",
                    job.eventId(), job.attemptNumber(), job.maxRetries(), dueAt,
                    Math.max(0, dueAt.toEpochMilli() - System.currentTimeMillis()));

            // 🔴 COUNTED ONLY ON THE newlyAdded PATH, AND THE BRANCH ABOVE IS THE REASON.
            //
            // A re-schedule of an attempt already sitting in the set updates its due time and
            // returns true WITHOUT reaching here -- the sorted set collapses the two into one entry,
            // which is the deduplication this class was built on. Counting there as well would
            // report two retries where the system will perform exactly one, so the metric would
            // over-state retry volume precisely when something upstream was scheduling duplicates.
            //
            // The counter therefore means "retries that will actually happen", which is the only
            // version of the number that can be reconciled against webhook.retry.depth or against
            // the attempt rows in Postgres. It is also placed after the Redis write and inside the
            // try, so the catch below -- which deliberately returns false so the caller leaves the
            // job parked in processing -- leaves it unincremented.
            retriesScheduled.increment();
            return true;

        } catch (Exception e) {
            // Deliberately broad, and the return value matters more than the log line. The caller
            // must not remove this job from the processing list unless the retry was genuinely
            // recorded: schedule first, then remove. A crash or failure between the two leaves
            // the job parked in processing, where the recovery sweep already looks (14b) — and a
            // duplicate delivery is a failure this system absorbs by design, whereas a job
            // removed from processing with nothing scheduled has been silently deleted.
            //
            // toString() rather than a stack trace: if Redis is unreachable this fires on every
            // failed delivery, and repeated traces bury the one line naming the cause (notes 9b).
            log.error("FAILED to schedule retry for event {} attempt {} — not removing it from '{}': {}",
                    job.eventId(), job.attemptNumber(), RedisKeys.PROCESSING, e.toString());
            return false;
        }
    }

    /**
     * Takes every job whose backoff has expired and puts it back on the main work queue.
     *
     * <p>This is the read side, and it is one Redis call — a Lua script that finds the due jobs,
     * removes them from the retry set and pushes them onto the queue, atomically. The reason it
     * must be atomic is the whole argument in the script file: every worker runs a scheduler
     * thread, they all read the same set, and neither ordering of "remove" and "push" is correct
     * when written as separate calls. One duplicates every job on every pass; the other loses
     * jobs outright on a crash.
     *
     * <p><b>What the caller gets back is a report, not a to-do list.</b> The move has already
     * happened by the time this returns. The jobs are returned only so the scheduler can log
     * which events were promoted, which is what makes "why was event X delivered twice?"
     * answerable later.
     *
     * <p><b>Never throws.</b> A Redis outage returns an empty list, so the scheduler thread
     * treats it as "nothing was due" and tries again next tick. Nothing is lost by that: the
     * jobs stay in the retry set with their due times, they simply come back late. The
     * alternative — letting the exception escape — kills the scheduler thread and leaves a
     * process that looks healthy while no retry ever fires again (notes 9b).
     *
     * @param now   the moment to judge against; anything due at or before this is claimed
     * @param limit the most jobs to move in one pass, bounding how long the script occupies
     *              Redis's single thread
     * @return the jobs that were moved, oldest due first; empty if none were due or Redis failed
     */
    @SuppressWarnings("unchecked")
    public List<String> claimDue(Instant now, int limit) {
        try {
            List<String> moved = (List<String>) redis.execute(
                    CLAIM_DUE_SCRIPT,
                    List.of(RedisKeys.RETRY, RedisKeys.QUEUE),
                    Long.toString(now.toEpochMilli()),
                    Integer.toString(limit));

            // A script returning an empty Lua table comes back as an empty list, but a failure to
            // execute at all can come back null. Normalising here means no caller has to decide
            // what null means — the same reasoning as checking the LREM return in 9d, one level up.
            return moved == null ? List.of() : moved;

        } catch (Exception e) {
            log.error("Could not claim due retries from '{}' — they stay scheduled and will be "
                            + "picked up on a later pass: {}",
                    RedisKeys.RETRY, e.toString());
            return List.of();
        }
    }

    /**
     * How many jobs are currently waiting out a backoff.
     *
     * <p>Exposed because this number is the first thing anyone asks during an incident — a retry
     * set that keeps growing means deliveries are failing faster than they are recovering, which
     * is a different problem from a growing main queue and needs a different response. It becomes
     * a Prometheus gauge in Week 6.
     *
     * @return the count, or {@code -1} if Redis could not be reached
     */
    public long size() {
        try {
            Long count = redis.opsForZSet().zCard(RedisKeys.RETRY);
            return count == null ? -1 : count;
        } catch (Exception e) {
            log.warn("Could not read the size of '{}': {}", RedisKeys.RETRY, e.toString());
            return -1;
        }
    }
}
