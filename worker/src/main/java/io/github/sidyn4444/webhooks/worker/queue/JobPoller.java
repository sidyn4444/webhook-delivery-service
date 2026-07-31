package io.github.sidyn4444.webhooks.worker.queue;

import io.github.sidyn4444.webhooks.common.model.DeliveryJob;
import io.github.sidyn4444.webhooks.common.model.DlqReason;
import io.github.sidyn4444.webhooks.common.queue.JobCodec;
import io.github.sidyn4444.webhooks.common.queue.RedisKeys;
import io.github.sidyn4444.webhooks.worker.delivery.DeliveryClassifier;
import io.github.sidyn4444.webhooks.worker.delivery.DeliveryOutcome;
import io.github.sidyn4444.webhooks.worker.delivery.DeliveryResult;
import io.github.sidyn4444.webhooks.worker.delivery.WebhookSender;
import io.github.sidyn4444.webhooks.worker.dlq.DeadLetterQueue;
import io.github.sidyn4444.webhooks.worker.persistence.DeliveryAttempt;
import io.github.sidyn4444.webhooks.worker.persistence.DeliveryAttemptRepository;
import io.github.sidyn4444.webhooks.worker.retry.RetryBackoff;
import io.github.sidyn4444.webhooks.worker.retry.RetryQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

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
 * <p><b>As of 13b every job now has an ending.</b> A failure is classified (12a) and either
 * scheduled for another attempt (12e) or dead-lettered (13b), and in both cases it is removed from
 * {@code processing} once the durable record exists. Nothing is left parked by design any more —
 * so from here on, a job still sitting in {@code processing} means a worker died holding it, which
 * is precisely the signal the recovery sweep looks for (14b).
 */
@Component
public class JobPoller implements SmartLifecycle {

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
     * The delivery log (10b/10c). Injected by type like any other bean — the implementation is
     * generated by Spring Data at startup, so there is no class in this project behind it.
     */
    private final DeliveryAttemptRepository attempts;

    /**
     * Where a failed job goes to wait for its next attempt (12c).
     *
     * <p>Note what is NOT injected here: {@code RetryScheduler}. This class writes a scheduled
     * retry into Redis and the scheduler independently reads it back out; the two never reference
     * each other in either direction. That is deliberate — the pod that schedules a 16-second retry
     * may not exist 16 seconds later, so handing the job to a specific scheduler would mean the
     * retry dies with it. Redis is the only thing they share.
     */
    private final RetryQueue retryQueue;

    /**
     * Where a job goes when the system stops trying (13a/13b).
     *
     * <p>Three branches in this class reach it, and each one passes its own reason as a constant.
     * That is deliberate and is the reason the reason is a parameter rather than something the
     * dead-letter queue works out for itself: <b>the classification is context that exists only at
     * the branch.</b> A job that got a 404 on its first attempt and a job that exhausted five
     * retries are indistinguishable once they are just a variable — the difference is which line of
     * code the call came from, and passing it along is how that survives the call.
     */
    private final DeadLetterQueue deadLetters;

    /**
     * The shutdown flag.
     *
     * <p>{@code volatile} is essential and easy to omit. Each CPU core may cache a copy of a
     * field, so a write from one thread is not guaranteed to be visible to another; without
     * this keyword the polling thread could keep reading a stale {@code true} indefinitely and
     * never stop. {@code volatile} forces every read to come from main memory.
     */
    private volatile boolean running = false;

    private Thread pollThread;

    /**
     * When this bean is stopped relative to everything else in the application.
     *
     * <p>🔴 This number is the fix for a real defect, and the reasoning is worth keeping.
     *
     * <p>Spring shuts down in two distinct passes: first it calls {@code stop()} on every bean
     * implementing {@code Lifecycle}, then it runs {@code @PreDestroy} and {@code destroy()} on
     * everything. {@code LettuceConnectionFactory} — the Redis connection — implements
     * {@code SmartLifecycle} and returns phase {@code 0}.
     *
     * <p>So while this class used {@code @PreDestroy} to stop the loop, the ordering was:
     *
     * <pre>
     *   t+0.0s   Redis connection stopped        (lifecycle pass)
     *   t+0.0s   poll loop still running, its next Redis call fails
     *   t+2.0s   more failures
     *   t+2.1s   @PreDestroy finally asks the loop to stop   ← two seconds too late
     * </pre>
     *
     * <p>The visible symptom was a handful of ERROR lines on every clean shutdown. The real
     * symptom was worse: <b>a pod terminated mid-delivery would complete the delivery and then
     * fail to acknowledge it</b>, because Redis was already gone — so the job stayed parked and
     * was re-delivered. On Kubernetes, where every rolling update terminates every pod, that is a
     * duplicate on every deploy rather than an edge case.
     *
     * <p>Lifecycle beans are stopped in <b>descending</b> phase order, so a phase above 0 stops
     * before the connection it depends on is taken away. {@code Integer.MAX_VALUE - 1} leaves one
     * step above it for the retry scheduler, which should stop first — no point promoting more
     * jobs onto a queue nobody is draining any more.
     *
     * <p>The general rule this is an instance of: <b>a component that uses a resource must be
     * stopped before the resource is closed, and with Spring's two-pass shutdown that ordering is
     * expressed with lifecycle phases, not with {@code @PreDestroy}.</b>
     */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 1;
    }

    /**
     * Whether the loop is currently running.
     *
     * <p>Spring calls this to decide whether {@link #stop()} needs calling at all, so getting it
     * wrong means a shutdown hook that silently never fires.
     */
    @Override
    public boolean isRunning() {
        return running;
    }

    public JobPoller(StringRedisTemplate redis, WebhookSender sender,
                     DeliveryAttemptRepository attempts, RetryQueue retryQueue,
                     DeadLetterQueue deadLetters) {
        this.redis = redis;
        this.sender = sender;
        this.attempts = attempts;
        this.retryQueue = retryQueue;
        this.deadLetters = deadLetters;
    }

    /**
     * Verifies Redis is genuinely reachable, then starts the polling thread.
     *
     * <p>Called by Spring during startup, once every bean is constructed and wired — slightly
     * later than {@code @PostConstruct}, which is where this used to live. It moved because
     * shutdown had to move (see {@link #getPhase()}), and start and stop belong together: a
     * component started by one mechanism and stopped by another is exactly how the ordering bug
     * got in. Throwing from here still fails application startup, so the fail-fast behaviour below
     * is unchanged.
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
    @Override
    public void start() {
        String pong = redis.execute((RedisCallback<String>) connection -> connection.ping());
        log.info("Redis reachable (PING -> {}). Starting poll loop on '{}'.", pong, RedisKeys.QUEUE);

        running = true;

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
            // 🔴 THE THIRD ROAD TO THE DEAD-LETTER QUEUE, and the one that is easy to leave out.
            //
            // Until now this message was parked in 'processing' and left there. That was harmless
            // only because nothing ever looked at that list. Once the recovery sweep exists (14b)
            // it becomes an infinite loop — picked up, unparseable, parked, swept, re-queued,
            // forever — and the attempt ceiling cannot stop it, because attemptNumber and
            // maxRetries both live inside the job and reading the job is what just failed. There
            // is nothing to count. Dead-lettering on the spot is the only exit.
            //
            // Note what is NOT recorded: no Postgres row. The delivery log is keyed on an event id
            // and there is no event id here, because nothing could be read out of the message. The
            // dead letter is the only record this failure gets, which is precisely why it has to
            // carry the raw bytes.
            deadLetters.deadLetterUnparseable(json, describeParseFailure(e));
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

        // 🔴 RECORD BEFORE ACKNOWLEDGING. The ordering is the same rule as the ack itself: do the
        // durable thing first, then remove the evidence that it still needs doing.
        //
        // Acknowledging first would open a window in which the job is gone from Redis while the
        // delivery it represents has been recorded nowhere. A crash there loses an event that was
        // genuinely delivered, permanently, with nothing anywhere pointing at it. Recording first
        // inverts which failure is possible: a crash between the two leaves the job parked, it is
        // re-delivered later, and the subscriber deduplicates on the stable event id (notes 9d).
        //
        // A duplicate delivery the system is already designed to absorb, against an audit record
        // that can never be reconstructed. That is the trade, and it is not close.
        //
        // A row is written for EVERY attempt, success or failure — this is an attempt log, not an
        // outcome log. A 500 on attempt 1 is a fact worth keeping even though the job will be
        // retried, because "what happened to event X?" is answered by the sequence, not the last
        // line of it.
        if (!record(job, result)) {
            // The write failed, so this attempt is unrecorded. Deliberately NOT acked: the job
            // stays parked and will be re-delivered rather than disappearing unlogged. This is
            // where the "evidence, not telemetry" decision from 10a is actually paid for — the
            // cost of a complete log is a duplicate delivery when Postgres is unavailable.
            return;
        }

        if (result.succeeded()) {
            log.info("Delivered event {} -> {}", job.eventId(), result.describe());
            ack(json, job.eventId());
        } else {
            handleFailure(json, job, result);
        }
    }

    /**
     * Decides what happens to a job whose delivery did not succeed.
     *
     * <p>This is where Task 12 converges. Everything before it produced one input each: the
     * classification (12a), the wait (12b), somewhere to wait (12c), and something to bring the job
     * back (12d). Until now the failure branch was a single log line and every failed job simply
     * stayed parked.
     *
     * <p>Three outcomes, and the ordering of the checks is the retry policy from notes 2e:
     *
     * <ol>
     *   <li><b>Permanent</b> — the request itself is wrong, so retrying reproduces the rejection
     *       exactly. Straight to the dead-letter queue (13b).</li>
     *   <li><b>Retriable, attempts exhausted</b> — transient, but it has had every attempt it was
     *       promised. Also dead-lettered, by the other road (13b).</li>
     *   <li><b>Retriable, attempts remaining</b> — schedule the next one and release this job.</li>
     * </ol>
     *
     * <p>Two of the three roads end in a dead letter, and each passes its own {@link DlqReason} as
     * a literal constant. The branch already did the deciding; the constant only names where the
     * code is. Working the reason out again inside the dead-letter queue would put the retry policy
     * in two files, and the day they drift nothing errors — the entries are just mislabelled.
     */
    private void handleFailure(String json, DeliveryJob job, DeliveryResult result) {
        DeliveryOutcome outcome = DeliveryClassifier.classify(result);

        if (outcome == DeliveryOutcome.PERMANENT) {
            // The immediate road to the DLQ (notes 2e). Nothing about waiting would help: a 404 is
            // a statement about this URL and will be equally true in sixteen seconds.
            //
            // The reason is a hardcoded constant rather than something computed, because reaching
            // this branch IS the fact. DeliveryClassifier already did the thinking (12a); there is
            // nothing left to work out here, only somewhere to name it.
            deadLetters.deadLetter(json, job, DlqReason.NON_RETRIABLE_RESPONSE, result);
            return;
        }

        if (!job.hasRetriesLeft()) {
            // The exhausted road to the DLQ — the one people forget when describing retry logic,
            // and the one interviewers probe with "so what happens after the last retry fails?"
            //
            // Same shape as above: hasRetriesLeft() counted the attempts, so arriving here is the
            // classification. A different constant, because a human's response is different — this
            // one usually means the subscriber was down longer than the retry window, and replaying
            // it later will probably just work.
            deadLetters.deadLetter(json, job, DlqReason.RETRIES_EXHAUSTED, result);
            return;
        }

        // 🔴 THE DELAY IS COMPUTED FROM THE ATTEMPT THAT JUST FAILED, BEFORE INCREMENTING.
        //
        // job.attemptNumber() is the attempt that failed, so attempt 1 failing waits 1s. Calling
        // nextAttempt() first and then asking for the delay would pass 2 and wait 2s — every wait
        // doubled, the schedule silently becoming 2/4/8/16/32 instead of 1/2/4/8/16. Nothing would
        // error; the system would just be twice as slow forever, and no log line would say so.
        Duration wait = RetryBackoff.nextDelay(job.attemptNumber());
        DeliveryJob next = job.nextAttempt();
        Instant dueAt = Instant.now().plus(wait);

        // 🔴 SCHEDULE FIRST, RELEASE SECOND. The same rule as recording before acking (10d) and
        // delivering before acking (9d): do the durable thing first, then remove the evidence that
        // it still needs doing.
        //
        // If this returns false the job is deliberately left in 'processing' with nothing scheduled.
        // That is the recoverable failure: the recovery sweep (14b) finds it and re-queues it. The
        // alternative ordering — release then schedule — would lose the job entirely if the schedule
        // failed, and nothing anywhere would record that it had ever existed.
        if (!retryQueue.schedule(next, dueAt)) {
            log.error("Could not schedule retry for event {} — job left in '{}' for the recovery "
                            + "sweep rather than being dropped.",
                    job.eventId(), RedisKeys.PROCESSING);
            return;
        }

        // Now safe to release: the next attempt is durably recorded in Redis, so removing this copy
        // cannot lose the job. This uses the ORIGINAL string read off the queue, not a
        // re-serialisation — LREM matches exact bytes, and a re-serialised copy differing by one
        // byte would remove nothing, return 0, and leave a duplicate to be delivered twice (9d).
        Long removed = redis.opsForList().remove(RedisKeys.PROCESSING, 1, json);

        if (removed == null || removed == 0) {
            // The retry IS scheduled, so no delivery is lost — but this copy is now stranded in
            // 'processing' and the recovery sweep will eventually re-queue it as well, producing a
            // duplicate. Logged loudly because the alternative symptom is an unexplained duplicate
            // days later with nothing pointing here.
            log.error("Scheduled a retry for event {} but failed to remove it from '{}' "
                            + "(LREM removed {}). Expect one duplicate delivery.",
                    job.eventId(), RedisKeys.PROCESSING, removed);
            return;
        }

        log.warn("RETRIABLE failure for event {} -> {}. Attempt {} of {} scheduled for {} (in {}ms).",
                job.eventId(), result.describe(), next.attemptNumber(), next.maxRetries(),
                dueAt, wait.toMillis());
    }

    /**
     * Writes one row to the delivery log describing this attempt.
     *
     * <p>Called for every attempt regardless of outcome, before the ack, and its return value
     * decides whether the ack is allowed to happen at all.
     *
     * @return {@code true} if the attempt was recorded; {@code false} if it could not be
     */
    private boolean record(DeliveryJob job, DeliveryResult result) {
        try {
            // The mapping lives on the entity (10b), so the job and the outcome are the only
            // inputs and nothing here re-decides what "success" means or which fields to copy.
            // Note what cannot be passed even by mistake: there is no payload parameter, because
            // there is no payload column.
            DeliveryAttempt saved = attempts.save(DeliveryAttempt.of(job, result));

            log.info("Recorded attempt {} for event {} (id={}, {})",
                    job.attemptNumber(), job.eventId(), saved.getId(), result.describe());
            return true;

        } catch (Exception e) {
            // Deliberately broad, for the same reason the poll loop is: an exception escaping here
            // would kill the polling thread and leave a process that looks healthy while consuming
            // nothing at all (notes 9b). A logging failure must never be able to stop the work it
            // is logging.
            //
            // toString() rather than a stack trace: if Postgres is down this fires on every job,
            // and a full trace each time buries the one line that identifies the cause.
            log.error("FAILED to record attempt {} for event {} — not acking, job stays in '{}' "
                            + "and will be re-delivered: {}",
                    job.attemptNumber(), job.eventId(), RedisKeys.PROCESSING, e.toString());
            return false;
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
     * <p>Called by Spring during the lifecycle-stop pass, which happens on Ctrl-C or when
     * Kubernetes sends SIGTERM to a pod during a rollout — and, critically, <b>before</b> the Redis
     * connection this loop depends on is closed. See {@link #getPhase()} for why that ordering had
     * to be made explicit.
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
    @Override
    public void stop() {
        log.info("Shutdown requested — stopping poll loop.");
        running = false;

        if (pollThread != null) {
            try {
                // Wait a little longer than one full block, so a loop parked in Redis has time to
                // return and exit cleanly rather than being abandoned.
                pollThread.join(BLOCK_TIMEOUT.plusSeconds(2).toMillis());
            } catch (InterruptedException e) {
                // The shutdown itself was interrupted. Restore the flag so anything above this
                // frame can still see it, and stop waiting — the flag is already set, so the loop
                // will exit on its own as soon as its current block expires.
                Thread.currentThread().interrupt();
                log.warn("Interrupted while waiting for the poll loop to finish.");
            }
        }
    }

    /**
     * Turns a parse failure into a short description safe to store and to log.
     *
     * <p>🔴 <b>The exception's message is deliberately not used, and the reason is a security one.</b>
     *
     * <p>Jackson quotes the offending input back at you. A real message looks like:
     *
     * <pre>
     *   Unrecognized token 'not': was expecting (JSON String, ...)
     *    at [Source: (String)"{not json at all, and here is the payload"; line: 1, column: 5]
     * </pre>
     *
     * <p>That embedded fragment is <b>unvalidated content of unknown origin</b> — and this is the
     * one code path where that phrase is literally true, since the message failed to parse and
     * nothing about it has been checked. Copying it into {@code lastFailureReason} would put a
     * slice of a possibly-personal payload into a human-readable summary field, and copying it into
     * a log line would put it somewhere with far weaker access control than the queue it came from.
     * That is exactly the leak the no-payload-in-logs rule exists to prevent (notes 2b, 10b).
     *
     * <p>The exception <i>type</i> carries the diagnosis without the content, and it is genuinely
     * diagnostic: {@code JsonParseException} means the bytes are not JSON at all — suspect a human
     * with {@code redis-cli}, or a truncated write. {@code MismatchedInputException} or
     * {@code ValueInstantiationException} means it was valid JSON that could not become a job —
     * suspect a version skew, a renamed field, or a message written before a schema change
     * (notes 13a).
     *
     * <p>The full bytes are still preserved, in {@code originalJob} on the entry itself. Nothing is
     * lost; it is just kept in the access-controlled place rather than the readable-by-everyone one.
     */
    private String describeParseFailure(Exception e) {
        Throwable cause = e.getCause();
        return cause != null
                ? "could not be parsed as a job (" + cause.getClass().getSimpleName() + ")"
                : "could not be parsed as a job (" + e.getClass().getSimpleName() + ")";
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
