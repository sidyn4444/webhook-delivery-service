package io.github.sidyn4444.webhooks.worker.queue;

import io.github.sidyn4444.webhooks.common.model.DeliveryJob;
import io.github.sidyn4444.webhooks.common.queue.JobCodec;
import io.github.sidyn4444.webhooks.common.queue.RedisKeys;
import io.github.sidyn4444.webhooks.worker.delivery.DeliveryResult;
import io.github.sidyn4444.webhooks.worker.delivery.WebhookSender;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * The worker's heart: a loop that takes jobs off the queue, one at a time, forever.
 *
 * <p>This class implements the <b>reliable-queue pattern</b> (notes 2c). The naive way to
 * consume a queue is a destructive pop — take the job off the list and hold it in memory. That
 * loses data: the instant the job leaves Redis, the only copy lives inside a process that can
 * die at any moment, and in Kubernetes processes die constantly through rollouts, autoscaling
 * and node maintenance. A worker killed mid-delivery would take the job with it, with no
 * record it ever existed.
 *
 * <p>The fix is to never let a job exist in only one place. {@code RPOPLPUSH} removes a job
 * from the queue and appends it to a second list in <b>one atomic step</b> — atomic meaning it
 * either fully happens or does not happen at all, so no crash can catch it half-done. A job is
 * therefore always in {@code webhooks:queue} or in {@code webhooks:processing}, never in
 * neither. If this process dies mid-delivery the job is still sitting in Redis, and a recovery
 * sweep can hand it to a healthy worker (Session 3).
 *
 * <p>That is what makes at-least-once delivery a mechanical property of the system rather than
 * an intention (notes 1e).
 *
 * <p><b>The full cycle, as of 9d:</b> atomically move the job to {@code processing} → deliver it
 * → on success remove it from {@code processing} (the ack). A job is therefore in exactly one of
 * three states at any instant: waiting on the queue, in flight in {@code processing}, or done and
 * gone. There is no state in which it exists nowhere, which is what makes at-least-once delivery
 * a mechanical property rather than an intention.
 *
 * <p><b>Failed deliveries are deliberately left in {@code processing}.</b> That is not an
 * omission — an unfinished job is exactly what a recovery sweep needs to find. Retrying,
 * dead-lettering and the sweep itself are Session 3; until then a failure simply parks.
 */
@Component
public class JobPoller {

    private static final Logger log = LoggerFactory.getLogger(JobPoller.class);

    /**
     * How long Redis holds a request open waiting for a job before returning empty-handed.
     *
     * <p>The alternative to blocking is asking repeatedly in a tight loop, which on an idle
     * system means thousands of pointless round trips per minute — burning CPU on both sides
     * and buying nothing. Blocking inverts it: Redis answers the moment a job arrives, so
     * pickup latency is near-zero AND an idle worker costs almost nothing.
     *
     * <p>It returns after five seconds rather than waiting forever so the loop regains control
     * regularly and can notice a shutdown request. That is the only reason this value is finite,
     * and it sets the worst-case shutdown delay.
     *
     * <p>⚠️ This must stay below {@code spring.data.redis.timeout} (10s), which caps how long
     * any single Redis command may take. A command timeout shorter than this block would abort
     * every wait as a failure on a perfectly healthy system.
     */
    private static final Duration BLOCK_TIMEOUT = Duration.ofSeconds(5);

    /**
     * Pause after an unexpected error before looping again.
     *
     * <p>Without it, a persistent failure — Redis down, for instance — becomes a tight loop
     * retrying thousands of times a second, producing gigabytes of identical log lines and
     * saturating a CPU while achieving nothing. A short sleep turns a runaway into a steady,
     * readable heartbeat of retries.
     */
    private static final Duration ERROR_BACKOFF = Duration.ofSeconds(2);

    private final StringRedisTemplate redis;
    private final WebhookSender sender;

    /**
     * The shutdown flag.
     *
     * <p>{@code volatile} is essential and easy to omit. Each CPU core may cache a copy of a
     * field, so a write from one thread is not guaranteed to be visible to another; without
     * this keyword the polling thread could keep reading a stale {@code true} indefinitely and
     * never stop. {@code volatile} forces every read to come from main memory.
     */
    private volatile boolean running = true;

    private Thread pollThread;

    public JobPoller(StringRedisTemplate redis, WebhookSender sender) {
        this.redis = redis;
        this.sender = sender;
    }

    /**
     * Verifies Redis is genuinely reachable, then starts the polling thread.
     *
     * <p>{@code @PostConstruct} runs once after this bean is constructed and injected, during
     * application startup.
     *
     * <p>The PING is here rather than in a startup probe elsewhere because this is the class
     * that cannot function without Redis, and an assertion belongs next to the thing it
     * protects. It matters because Spring Data Redis connects <b>lazily</b> — it opens no
     * socket until a command is issued, so a wrong host or password produces an application
     * that starts perfectly, reports healthy, and silently does nothing (proven in 8a and
     * again in 9a). Throwing from {@code @PostConstruct} fails context startup, converting a
     * silent misconfiguration into an immediate, loud crash.
     *
     * <p>Note the deliberate asymmetry with the loop below: <b>startup is strict, runtime is
     * forgiving.</b> Redis unreachable at boot means the configuration is probably wrong and
     * the process should refuse to start; Redis unreachable an hour later is probably a blip
     * worth surviving. Same failure, different meaning, because the timing carries information.
     */
    @PostConstruct
    void start() {
        String pong = redis.execute((RedisCallback<String>) connection -> connection.ping());
        log.info("Redis reachable (PING -> {}). Starting poll loop on '{}'.", pong, RedisKeys.QUEUE);

        // A thread created here inherits the daemon status of the thread that creates it, and
        // startup runs on the main (non-daemon) thread — so this is a NON-DAEMON thread, and
        // the JVM will not exit while it runs. THIS is now what keeps the worker alive; the
        // spring.main.keep-alive property from 9a is a safety net rather than the mechanism.
        pollThread = new Thread(this::pollForever, "webhook-poller");
        pollThread.setDaemon(false);
        pollThread.start();
    }

    /**
     * The loop. Runs on its own thread until shutdown is requested.
     */
    private void pollForever() {
        log.info("Poll loop started.");

        while (running) {
            try {
                // RPOPLPUSH, in its blocking form: atomically take the job from the RIGHT of
                // the queue and push it onto the LEFT of the processing list. The producer
                // LPUSHes onto the left of the queue, so taking from the right makes the queue
                // FIFO — the oldest job is served next (notes 2c).
                //
                // Returns null if the block expired with the queue still empty.
                String json = redis.opsForList()
                        .rightPopAndLeftPush(RedisKeys.QUEUE, RedisKeys.PROCESSING, BLOCK_TIMEOUT);

                if (json == null) {
                    continue; // No work. Re-check the shutdown flag and wait again.
                }

                handle(json);

            } catch (Exception e) {
                // Deliberately catching everything. An uncaught exception here would kill this
                // thread, and because nothing restarts it the process would stay alive while
                // consuming nothing at all — a worker that looks healthy and silently ignores
                // the queue forever. That is the worst possible failure mode, and it is exactly
                // what an unguarded loop produces.
                //
                // toString() rather than the full stack trace: a Redis outage would otherwise
                // print a large trace every two seconds. The message identifies the cause.
                log.error("Poll loop error, retrying in {}s: {}", ERROR_BACKOFF.toSeconds(), e.toString());
                sleepQuietly(ERROR_BACKOFF);
            }
        }

        log.info("Poll loop stopped.");
    }

    /**
     * Turns one queued JSON string back into a job and delivers it.
     *
     * <p>9d adds the ack — removing the job from {@code processing} once delivery succeeded.
     * Until then every job stays parked there regardless of outcome.
     */
    private void handle(String json) {
        DeliveryJob job;
        try {
            job = JobCodec.fromJson(json);
        } catch (IllegalArgumentException e) {
            // Unparseable content on the queue. It has already been moved into processing by
            // the atomic pop, and it deliberately stays there: silently discarding data nobody
            // can read destroys the only evidence of a bug. Parking it keeps the evidence and
            // keeps the loop running. Session 3 routes this to the dead-letter queue, which is
            // where a permanently unprocessable message belongs.
            log.error("Unparseable job left parked in '{}': {}", RedisKeys.PROCESSING, e.toString());
            return;
        }

        // event id and attempt only — never the payload, which may carry personal data
        // (notes 2b). This rule holds at every layer: the producer logs the same way.
        log.info("Picked up event {} (attempt {} of {}) for {}",
                job.eventId(), job.attemptNumber(), job.maxRetries(), job.subscriberUrl());

        // The delivery itself. This call is guaranteed to return within the configured timeout
        // and never to throw, so a hostile or hanging subscriber cannot stall the loop or kill
        // the polling thread (notes 9c).
        DeliveryResult result = sender.send(job);

        if (result.succeeded()) {
            log.info("Delivered event {} -> {}", job.eventId(), result.describe());
            ack(json, job.eventId());
        } else {
            // Not an error in this sub-task's sense: a failed delivery is an expected outcome
            // with a decision attached to it, and that decision is Session 3's. The job stays in
            // the processing list, which is exactly where an unfinished job belongs — a job
            // sitting there past a staleness threshold is what the recovery sweep looks for.
            log.warn("Delivery unsuccessful for event {} -> {} (job left in '{}')",
                    job.eventId(), result.describe(), RedisKeys.PROCESSING);
        }
    }

    /**
     * The acknowledgement: removes a successfully-delivered job from the processing list.
     *
     * <p>This is the second half of the reliable-queue pattern. {@code RPOPLPUSH} guaranteed the
     * job was never in zero places; this is what finally takes it out of the last one. Until it
     * runs, the system's position is "this job may still need doing" — which is the correct
     * assumption to hold while a delivery is in flight, and the reason a crashed worker loses
     * nothing.
     *
     * <p><b>Order matters absolutely: deliver first, acknowledge second.</b> Acknowledging first
     * would reintroduce the exact data loss the pattern exists to prevent — the job would be gone
     * from Redis while the delivery was still unproven, so a failure or a crash would leave no
     * trace of work that never completed.
     *
     * @param json    the EXACT string that was read off the queue — see below
     * @param eventId for logging only
     */
    private void ack(String json, String eventId) {
        // LREM key count value. The count is deliberately 1, not 0:
        //
        //   0 = remove EVERY element equal to this value
        //   1 = remove the first match, scanning from the head
        //
        // One delivery acknowledges one job. A blanket "remove everything that looks like this"
        // is a broader claim than this method is entitled to make, and if two byte-identical
        // entries ever existed it would acknowledge a delivery that never happened. Identical
        // entries are currently impossible because enqueuedAt carries microsecond precision, so
        // this is insurance rather than a fix — but it is insurance against a schema change
        // (coarsening or dropping that timestamp) silently turning this line into a data-loss
        // bug in a file nobody thought to revisit.
        //
        // 🔴 THE STRING MUST BE THE ORIGINAL ONE READ FROM REDIS.
        // LREM matches on exact byte equality, not on any notion of "the same job". Passing
        // JobCodec.toJson(job) instead of the original would look equivalent and would be a
        // silent bug: any difference in field order, timestamp formatting or escaping produces a
        // string that matches nothing, so LREM removes nothing, returns 0, and a delivered job
        // stays parked forever — until the recovery sweep re-delivers it. That surfaces days
        // later as unexplained duplicates, with nothing in any log pointing here.
        Long removed = redis.opsForList().remove(RedisKeys.PROCESSING, 1, json);

        if (removed != null && removed == 1) {
            log.info("Acked event {} — removed from '{}'", eventId, RedisKeys.PROCESSING);
            return;
        }

        // Reaching here means the delivery succeeded but the job could not be removed. It is
        // logged loudly because it is the failure mode described above, and because its only
        // other visible symptom is a duplicate delivery much later, by which point the cause is
        // untraceable. An ack that quietly does nothing is far worse than one that fails noisily.
        log.error("ACK FAILED for event {} — LREM removed {} entries from '{}'. "
                        + "The job was delivered but remains parked and will be re-delivered.",
                eventId, removed, RedisKeys.PROCESSING);
    }

    /**
     * Asks the loop to finish and waits briefly for it.
     *
     * <p>{@code @PreDestroy} runs when the application context shuts down — on Ctrl-C, or when
     * Kubernetes sends SIGTERM to a pod during a rollout.
     *
     * <p>Without this the process would be killed wherever it happened to be, potentially
     * mid-delivery. Kubernetes replaces pods constantly, so "shut down cleanly" is not an edge
     * case here — it is something that happens on every single deploy.
     *
     * <p>The flag is used rather than {@code Thread.interrupt()} because the loop may be parked
     * inside a blocking Redis command, and interrupting that path produces a spurious error on
     * what is a completely normal shutdown. Setting the flag and waiting is quieter: the block
     * expires within {@link #BLOCK_TIMEOUT}, the loop sees {@code running == false}, and it
     * exits on its own terms. The cost is that shutdown can take up to five seconds — well
     * inside Kubernetes' default 30-second termination grace period.
     */
    @PreDestroy
    void stop() throws InterruptedException {
        log.info("Shutdown requested — stopping poll loop.");
        running = false;

        if (pollThread != null) {
            // Wait a little longer than one full block, so a loop parked in Redis has time to
            // return and exit cleanly rather than being abandoned.
            pollThread.join(BLOCK_TIMEOUT.plusSeconds(2).toMillis());
        }
    }

    private void sleepQuietly(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            // Restore the flag the exception cleared, so anything further up this thread can
            // still see that an interrupt was requested. Swallowing it silently is a common
            // bug: it makes a thread unstoppable by the standard mechanism.
            Thread.currentThread().interrupt();
            running = false;
        }
    }
}
