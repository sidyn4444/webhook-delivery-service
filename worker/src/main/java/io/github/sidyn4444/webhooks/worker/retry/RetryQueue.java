package io.github.sidyn4444.webhooks.worker.retry;

import io.github.sidyn4444.webhooks.common.model.DeliveryJob;
import io.github.sidyn4444.webhooks.common.queue.JobCodec;
import io.github.sidyn4444.webhooks.common.queue.RedisKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;

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

    private final StringRedisTemplate redis;

    public RetryQueue(StringRedisTemplate redis) {
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
