package io.github.sidyn4444.webhooks.producer.queue;

import io.github.sidyn4444.webhooks.common.model.DeliveryJob;
import io.github.sidyn4444.webhooks.common.model.Event;
import io.github.sidyn4444.webhooks.common.queue.JobCodec;
import io.github.sidyn4444.webhooks.common.queue.RedisKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

/**
 * Turns an accepted {@link Event} into queued work.
 *
 * <p>Three steps, each of which was built and tested on its own in Task 7:
 * <ol>
 *   <li>wrap the caller's event in a {@link DeliveryJob}, adding the retry bookkeeping a
 *       worker needs — attempt number, ceiling, and enqueue time;
 *   <li>serialize that job to the JSON string the queue stores, via {@link JobCodec};
 *   <li>LPUSH the string onto {@link RedisKeys#QUEUE}.
 * </ol>
 *
 * <p>Keeping this out of the controller is deliberate. The controller's job is HTTP —
 * receiving, validating, replying. Enqueuing is a separate concern with its own collaborators
 * (the Redis client, the retry policy), and separating it means this logic can be unit-tested
 * without standing up a web layer, and the controller stays a thin edge.
 */
@Service
public class QueueService {

    private static final Logger log = LoggerFactory.getLogger(QueueService.class);

    private final StringRedisTemplate redis;
    private final int maxRetries;

    /**
     * <p>{@code StringRedisTemplate} is injected rather than the more general
     * {@code RedisTemplate}, and the choice matters. The default {@code RedisTemplate} serializes
     * values with Java's binary serialization, which would wrap our clean JSON string in an
     * opaque binary envelope — unreadable with {@code redis-cli}, and a format the worker would
     * have to match exactly to read back. {@code StringRedisTemplate} stores the string as-is, so
     * what lands in Redis is precisely the JSON {@link JobCodec} produced: inspectable by hand and
     * read by the worker with no extra decoding.
     *
     * <p>{@code maxRetries} comes from configuration with a default of 5, so the retry ceiling is
     * a tunable value rather than a number buried in code. The retry logic that consumes it is
     * Session 3; the job carries the ceiling from the moment it is created (see the design note on
     * capturing policy at enqueue time).
     */
    public QueueService(StringRedisTemplate redis,
                        @Value("${webhook.delivery.max-retries:5}") int maxRetries) {
        this.redis = redis;
        this.maxRetries = maxRetries;
    }

    /**
     * Wraps, serializes, and pushes the event onto the queue. Returns the job that was enqueued
     * so the caller can read back the id, attempt number, and enqueue time it was given.
     *
     * <p>The push is a LEFT push. Workers take from the right end (RPOPLPUSH), so left-in /
     * right-out makes the queue FIFO — the oldest job is served next.
     */
    public DeliveryJob enqueue(Event event) {
        DeliveryJob job = DeliveryJob.firstAttempt(event, maxRetries);
        String json = JobCodec.toJson(job);
        redis.opsForList().leftPush(RedisKeys.QUEUE, json);
        log.info("Enqueued event {} (attempt {} of {})",
                job.eventId(), job.attemptNumber(), job.maxRetries());
        return job;
    }
}
