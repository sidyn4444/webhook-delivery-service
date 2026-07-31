package io.github.sidyn4444.webhooks.worker.retry;

import io.github.sidyn4444.webhooks.common.queue.JobCodec;
import io.github.sidyn4444.webhooks.common.model.DeliveryJob;
import io.github.sidyn4444.webhooks.common.queue.RedisKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Asks one question on a loop: is anything due?
 *
 * <p>Jobs waiting out a backoff sit in a Redis sorted set scored by the time they become due
 * (12c). Nothing about that data structure causes anything to happen — a job could be due for a
 * week and no worker would notice, because the poll loop reads a completely different key. This
 * class is the thing that closes that gap: it wakes on a short interval, moves whatever has come
 * due onto the main work queue, and goes back to sleep.
 *
 * <p><b>Every worker pod runs one of these.</b> That is safe only because the move is atomic —
 * see {@code claim-due-retries.lua}. Three schedulers reading the same set with separate Redis
 * commands would each push the same due job, and every retried webhook would be delivered three
 * times, on every pass, with nothing wrong anywhere. The script makes concurrent schedulers a
 * non-issue rather than a problem to be managed.
 *
 * <p><b>Why not one scheduler instead of one per worker</b> — the two obvious alternatives, and
 * why both cost more than the script:
 *
 * <ul>
 *   <li><b>A dedicated scheduler process.</b> One deployment with a single replica whose only job
 *       is this loop. It works, and it is a single point of failure: while that pod is down or
 *       restarting, nothing in the system retries anything. Kubernetes will restart it, so the
 *       outage is a gap rather than permanent — but it is still a second artefact to build,
 *       deploy, monitor and alert on, to replace a thread.</li>
 *   <li><b>Leader election among the workers.</b> All pods identical, one elected leader running
 *       the loop, a new election if it dies. This needs a distributed lock, heartbeats to prove
 *       the leader is alive, and a timeout after which the others take over — and if two pods'
 *       clocks disagree, both can believe they hold the lock at once, which is the failure the
 *       whole mechanism existed to prevent.</li>
 * </ul>
 *
 * <p>Both are ways of <i>solving</i> the coordination problem. The script makes the coordination
 * problem not exist: no locks, no leader, no heartbeats, and correctness comes from Redis being
 * single-threaded rather than from any two processes agreeing about anything. <b>Prefer a design
 * that needs no coordination over one that coordinates well.</b>
 */
@Component
public class RetryScheduler implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(RetryScheduler.class);

    private final RetryQueue retryQueue;

    /**
     * How often to ask whether anything is due.
     *
     * <p>This is the accuracy of the whole retry schedule: a job due at time {@code t} actually
     * runs somewhere in {@code [t, t + interval]}. One second against a minimum backoff of one
     * second is a worst-case 100% overshoot on the first retry and about 6% on the last, which is
     * comfortably inside the jitter already applied deliberately (12b) — so tightening it would
     * buy precision the design does not want.
     *
     * <p>It cannot be tightened much anyway. The loop runs on every worker, so the query rate is
     * {@code pods / interval} — three pods at one second is three queries a second whether or not
     * anything is due, and every one of them is a round trip plus a script execution on Redis's
     * single thread. Polling has a floor cost that a longer interval reduces linearly.
     */
    private final Duration pollInterval;

    /**
     * The most jobs to promote in one pass.
     *
     * <p>Bounds how long the script occupies Redis. A subscriber that has been down for an hour
     * has an hour of jobs come due in the same instant; moving all of them in one script would
     * block every other client of this Redis — including the producer's LPUSH inside a live HTTP
     * request — for the duration. Many short passes instead of one long stall.
     */
    private final int batchSize;

    /**
     * The shutdown flag. {@code volatile} because it is written by the thread calling
     * {@link #stop()} and read by the scheduler thread; without it the scheduler could keep
     * reading a cached {@code true} and never stop (notes 9b).
     */
    private volatile boolean running = false;

    private Thread schedulerThread;

    /**
     * Stopped one step BEFORE the poll loop, and well before the Redis connection.
     *
     * <p>Spring stops lifecycle beans in descending phase order, and
     * {@code LettuceConnectionFactory} sits at phase {@code 0} — so anything that talks to Redis
     * has to be above it or it will find the connection already gone. The full story is on
     * {@code JobPoller.getPhase()}, where the same number fixes the same defect.
     *
     * <p>Why one step above the poll loop rather than the same phase: this class <i>produces</i>
     * work and the poll loop <i>consumes</i> it. Stopping the producer first means the queue is not
     * still growing while the consumer is trying to finish. Nothing breaks if the order were
     * reversed — a promoted job simply waits on the queue for another pod — but "stop feeding
     * before you stop eating" is the shape that needs no explanation.
     */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    public RetryScheduler(RetryQueue retryQueue,
                          @Value("${webhook.retry.poll-interval:1s}") Duration pollInterval,
                          @Value("${webhook.retry.batch-size:100}") int batchSize) {
        this.retryQueue = retryQueue;
        this.pollInterval = pollInterval;
        this.batchSize = batchSize;
    }

    @Override
    public void start() {
        running = true;
        schedulerThread = new Thread(this::runForever, "retry-scheduler");

        // DAEMON, unlike the poll loop's thread. A JVM exits when its last non-daemon thread
        // finishes, and the poll loop deliberately holds a non-daemon thread so that IT is what
        // defines "this worker is alive" (9b). Making this one non-daemon too would mean a worker
        // whose poll loop had died could still hold the process open while consuming nothing —
        // the exact zombie state the poll loop's broad catch exists to avoid.
        //
        // One thing keeps the process alive; everything else is a helper attached to it.
        schedulerThread.setDaemon(true);
        schedulerThread.start();

        log.info("Retry scheduler started — checking '{}' every {}ms, up to {} jobs per pass",
                RedisKeys.RETRY, pollInterval.toMillis(), batchSize);
    }

    private void runForever() {
        while (running) {
            try {
                promoteDueJobs();
            } catch (Exception e) {
                // Deliberately broad, for the reason the poll loop is (9b): an exception escaping
                // here kills this thread, and nothing restarts it. The process would stay alive,
                // pass every health check, keep delivering new webhooks normally — and silently
                // never retry anything again. A failure that leaves the system looking healthy is
                // worse than one that takes it down.
                log.error("Retry scheduler pass failed, continuing: {}", e.toString());
            }

            sleepQuietly(pollInterval);
        }

        log.info("Retry scheduler stopped.");
    }

    /**
     * One pass: claim everything due and report it.
     *
     * <p>The move itself is already done inside Redis by the time {@code claimDue} returns. All
     * this does is describe what happened.
     */
    private void promoteDueJobs() {
        List<String> promoted = retryQueue.claimDue(Instant.now(), batchSize);

        // Silence when there is nothing to say. This runs every second on every pod, so a log
        // line per pass would be tens of thousands of "nothing was due" entries a day, and the
        // handful of lines that matter would be unfindable inside them. A quiet log is a
        // readable log.
        if (promoted.isEmpty()) {
            return;
        }

        for (String json : promoted) {
            // Parsed purely to log the event id and attempt number. The job itself is already on
            // the queue and needs nothing from this code.
            //
            // Wrapped because an unparseable entry must not stop the rest of the batch from being
            // reported — and it genuinely cannot stop the MOVE, which Redis already completed
            // without ever looking inside the string. A poison pill here is a logging problem,
            // not a delivery problem; the poll loop deals with it properly when it picks the job
            // up (9b), and Task 13 sends it to the dead-letter queue.
            try {
                DeliveryJob job = JobCodec.fromJson(json);
                log.info("Retry due — requeued event {} for attempt {} of {}",
                        job.eventId(), job.attemptNumber(), job.maxRetries());
            } catch (IllegalArgumentException e) {
                log.warn("Requeued an unparseable retry entry onto '{}': {}",
                        RedisKeys.QUEUE, e.toString());
            }
        }

        log.info("Promoted {} due job(s) from '{}' to '{}' ({} still waiting)",
                promoted.size(), RedisKeys.RETRY, RedisKeys.QUEUE, retryQueue.size());
    }

    @Override
    public void stop() {
        log.info("Shutdown requested — stopping retry scheduler.");
        running = false;

        if (schedulerThread != null) {
            try {
                // The flag rather than interrupt(), and a join with a generous margin — the same
                // reasoning as the poll loop (9b). The worst case is one full poll interval plus a
                // script execution, so a wait of interval + 2s is comfortable, and far inside
                // Kubernetes' 30-second termination grace period.
                schedulerThread.join(pollInterval.plusSeconds(2).toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while waiting for the retry scheduler to finish.");
            }
        }
    }

    private void sleepQuietly(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            // Restore the flag the exception cleared, so anything further up this thread can
            // still see that an interrupt was requested. Swallowing it silently makes a thread
            // unstoppable by the standard mechanism (notes 9b).
            Thread.currentThread().interrupt();
            running = false;
        }
    }
}
