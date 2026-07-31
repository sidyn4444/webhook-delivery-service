package io.github.sidyn4444.webhooks.worker.dlq;

import io.github.sidyn4444.webhooks.common.model.DeliveryJob;
import io.github.sidyn4444.webhooks.common.model.DlqEntry;
import io.github.sidyn4444.webhooks.common.model.DlqReason;
import io.github.sidyn4444.webhooks.common.queue.JobCodec;
import io.github.sidyn4444.webhooks.common.queue.RedisKeys;
import io.github.sidyn4444.webhooks.worker.delivery.DeliveryResult;
import io.github.sidyn4444.webhooks.worker.queue.InFlightIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * The one place a job is given up on.
 *
 * <p>13a decided what a dead letter has to <i>carry</i>. This is the thing that writes one, and
 * the interesting part is not the write — it is the <b>order of the two operations</b>.
 *
 * <p>Giving up on a job is two separate changes to Redis: record it in the dead-letter queue, and
 * remove it from the processing list. There is no transaction spanning them, so a crash in the gap
 * is unavoidable — which means the only real decision is <b>which failure to make possible</b>.
 *
 * <pre>
 *   release first, record second   →  a crash in the gap loses the event entirely.
 *                                     Nothing in Redis, nothing in the DLQ, no trace anywhere.
 *
 *   record first, release second   →  a crash in the gap leaves the job in processing with a
 *                                     dead letter already written. The recovery sweep re-queues
 *                                     it, it is attempted again, and at worst it is dead-lettered
 *                                     a second time — a duplicate entry, which is untidy and
 *                                     recoverable.
 * </pre>
 *
 * <p><b>Record first.</b> This is the same rule as delivering before acking (notes 9d), recording
 * the attempt before acking (10d), and scheduling the retry before releasing (12e): <i>do the
 * irreversible-if-lost thing first, then remove the evidence that it still needs doing.</i> Four
 * places in this system now, all resolved the same way, and it is the single most transferable idea
 * in the project.
 *
 * <h2>What this class deliberately does NOT do</h2>
 *
 * <p><b>It does not decide why the job is being dead-lettered.</b> The reason is a parameter.
 *
 * <p>That looks like it could be worked out here — re-run the classifier, re-check the attempt
 * count — and it must not be. By the time a job reaches this class, the information is gone: a job
 * that got a 404 on its first attempt and a job that exhausted five retries look identical sitting
 * in a variable. The difference is not in the job, it is in <b>which branch of the poll loop the
 * call came from</b>, and that context only exists at the branch.
 *
 * <p>Re-deriving it here would put the retry policy in two files. The day someone changes the rule
 * in one and not the other, nothing errors — the dead letters are simply mislabelled, and every
 * number anyone triages from is quietly wrong.
 *
 * <p>So: {@code DlqReason} is the vocabulary, {@code JobPoller} is what speaks it, {@code DlqEntry}
 * is what gets written down, and this class does the writing.
 */
@Component
public class DeadLetterQueue {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterQueue.class);

    private final StringRedisTemplate redis;

    /**
     * The pickup-time index (14a). This class is the third and last place a job leaves the
     * processing list, so it is the third place the corresponding timestamp has to be cleared.
     *
     * <p>That there are three such places is the cost of the asymmetry noted in 13b — the retry
     * path lets the poller do its own release while this class does its own. It is also the reason
     * the removal is wrapped in a method here rather than written inline at each call: three copies
     * of "remove from the list, then clear the index, but only if the removal actually removed
     * something" is three chances to get the condition subtly wrong.
     */
    private final InFlightIndex inFlight;

    public DeadLetterQueue(StringRedisTemplate redis, InFlightIndex inFlight) {
        this.redis = redis;
        this.inFlight = inFlight;
    }

    /**
     * Gives up on a job that was understood but could not be delivered.
     *
     * <p>Covers two of the three roads — the immediate one ({@link DlqReason#NON_RETRIABLE_RESPONSE},
     * a response that waiting will not change) and the exhausted one
     * ({@link DlqReason#RETRIES_EXHAUSTED}, transient every time until the attempts ran out). The
     * fields written are identical; only the reason differs, which is why one method serves both and
     * the caller states which branch it came from.
     *
     * @param originalJson the EXACT string read off the queue. Two things depend on it being the
     *                     original bytes rather than a re-serialisation: a replay has to push back
     *                     exactly what was there, and the removal below matches on byte equality
     *                     (notes 9d).
     * @param job          the job as it was when we gave up — supplies the event id, the URL and the
     *                     attempt count for the human-readable summary
     * @param reason       which road this took; see the class comment for why it is a parameter
     * @param lastResult   the final attempt, so the entry explains itself without a Postgres join
     * @return {@code true} if the dead letter was durably recorded
     */
    public boolean deadLetter(String originalJson, DeliveryJob job, DlqReason reason,
                              DeliveryResult lastResult) {

        DlqEntry entry = DlqEntry.forFailedJob(
                job,
                originalJson,
                reason,
                lastResult.statusCode(),
                lastResult.failureReason(),
                Instant.now());

        if (!record(entry, job.eventId())) {
            return false;
        }

        log.warn("DEAD-LETTERED event {} after {} attempt(s): {} (last: {})",
                job.eventId(), job.attemptNumber(), reason, lastResult.describe());

        release(originalJson, job.eventId());
        return true;
    }

    /**
     * Gives up on a message that could not be turned into a job at all — the poison pill.
     *
     * <p>The third road, and the one that only became mandatory once the recovery sweep exists
     * (14b). Before the sweep, an unparseable message sat harmlessly in the processing list forever.
     * Once something scans that list and re-queues what it finds, a message nobody can parse becomes
     * an infinite loop: picked up, unparseable, parked, swept, re-queued, picked up again.
     *
     * <p><b>And the attempt ceiling structurally cannot stop it.</b> Every other runaway retry is
     * bounded by {@code attemptNumber > maxRetries} — but both of those numbers live inside the job,
     * and reading the job is exactly what failed. There is nothing to count. Dead-lettering on the
     * spot is the only available exit.
     *
     * @param rawMessage    the unreadable string exactly as it came off the queue — the only thing
     *                      that exists here, and therefore the only thing worth keeping
     * @param failureReason a short description of what the parser objected to. See the caller for
     *                      why this is the exception's <i>type</i> and not its message.
     * @return {@code true} if the dead letter was durably recorded
     */
    public boolean deadLetterUnparseable(String rawMessage, String failureReason) {

        DlqEntry entry = DlqEntry.forUnparseableMessage(rawMessage, failureReason, Instant.now());

        if (!record(entry, "<unparseable>")) {
            return false;
        }

        // The length, not the content. A message this system could not parse is a message it cannot
        // vouch for, and echoing it into the log would put unvalidated bytes of unknown origin into
        // the one place every operator reads. The bytes are preserved in the entry itself, which is
        // access-controlled; the log is not.
        log.warn("DEAD-LETTERED an unparseable message ({} chars): {}",
                rawMessage.length(), failureReason);

        release(rawMessage, "<unparseable>");
        return true;
    }

    /**
     * How many dead letters are currently waiting.
     *
     * <p>Exposed because this is the number that actually matters about a dead-letter queue. A DLQ
     * that grows and is never drained is silent data loss wearing a reliability costume — the
     * failure mode the whole design exists to avoid — and the only way anyone notices is a metric.
     * This becomes a Prometheus gauge with an alert on it in Week 6.
     *
     * @return the count, or {@code -1} if Redis could not be reached
     */
    public long size() {
        try {
            Long count = redis.opsForList().size(RedisKeys.DLQ);
            return count == null ? -1 : count;
        } catch (Exception e) {
            log.warn("Could not read the size of '{}': {}", RedisKeys.DLQ, e.toString());
            return -1;
        }
    }

    /**
     * The durable half: serialize the entry and push it onto the dead-letter queue.
     *
     * <p><b>Never throws.</b> The return value is the contract instead, for the same reason
     * {@code RetryQueue.schedule} works that way (12c): the caller must not remove a job from the
     * processing list unless the dead letter genuinely landed. A failure here leaves the job parked,
     * where the recovery sweep already looks — late and duplicated is recoverable, deleted is not.
     */
    private boolean record(DlqEntry entry, String eventIdForLog) {
        try {
            String json = JobCodec.toJson(entry);

            // LPUSH, the same direction the producer writes the main queue. Nothing consumes this
            // list in order — it is read by a human with LRANGE or by a future replay tool — so the
            // direction is chosen purely so that `LRANGE webhooks:dlq 0 9` shows the ten most recent
            // failures, which is what anyone opening it during an incident actually wants.
            Long length = redis.opsForList().leftPush(RedisKeys.DLQ, json);

            if (length == null) {
                // Spring returns null when the command could not be completed. It is not an
                // exception, so an unchecked call here would be indistinguishable from success —
                // the same trap as the LREM return value in 9d and the ZADD return in 12c.
                log.error("Could not dead-letter event {} — Redis returned no result. Job left in '{}'.",
                        eventIdForLog, RedisKeys.PROCESSING);
                return false;
            }

            return true;

        } catch (Exception e) {
            // Deliberately broad. An exception escaping here would kill the polling thread and
            // leave a process that looks healthy while consuming nothing at all (notes 9b) — and
            // it would do so on the failure path, which is exactly when the system is already
            // under stress and least able to afford a second failure.
            //
            // toString() rather than a stack trace: if Redis is unreachable this fires on every
            // dead letter, and repeated traces bury the one line naming the cause.
            log.error("FAILED to dead-letter event {} — job left in '{}' for the recovery sweep: {}",
                    eventIdForLog, RedisKeys.PROCESSING, e.toString());
            return false;
        }
    }

    /**
     * The second half: take the job out of the processing list, now that it is safely recorded.
     *
     * <p>Only reached once {@link #record} returned true, so this can no longer lose anything. The
     * worst case is a stranded copy in {@code processing} that the sweep re-queues later, producing
     * one extra delivery attempt and possibly a duplicate dead letter.
     *
     * <p>🔴 {@code count = 1}, and the string must be the ORIGINAL one read off the queue.
     * {@code LREM} matches on exact byte equality, not on any notion of "the same job", so a
     * re-serialisation differing by a single byte would match nothing, remove nothing, return 0, and
     * leave the job parked — surfacing days later as an unexplained duplicate with nothing pointing
     * here (notes 9d). The return value is checked for the same reason: {@code LREM} removing zero
     * entries is a <i>successful</i> command that did nothing.
     */
    private void release(String originalJson, String eventIdForLog) {
        try {
            Long removed = redis.opsForList().remove(RedisKeys.PROCESSING, 1, originalJson);

            if (removed != null && removed == 1) {
                // The job has left the processing list, so its pickup time is no longer meaningful
                // and is cleared (14a). Strictly inside this branch: a job that is still parked —
                // which is exactly what the error path below reports — must keep its timestamp, or
                // the recovery sweep will never see that anything is wrong with it.
                inFlight.forget(originalJson);

                log.info("Released event {} from '{}' — it is now in '{}'",
                        eventIdForLog, RedisKeys.PROCESSING, RedisKeys.DLQ);
                return;
            }

            log.error("Dead-lettered event {} but failed to remove it from '{}' (LREM removed {}). "
                            + "The recovery sweep will re-queue the stranded copy; expect one "
                            + "duplicate attempt.",
                    eventIdForLog, RedisKeys.PROCESSING, removed);

        } catch (Exception e) {
            // Same reasoning as above, one level down: the dead letter is already written, so this
            // failure costs a duplicate rather than a lost event. It must not be allowed to
            // propagate and kill the loop.
            log.error("Dead-lettered event {} but could not remove it from '{}': {}",
                    eventIdForLog, RedisKeys.PROCESSING, e.toString());
        }
    }
}
