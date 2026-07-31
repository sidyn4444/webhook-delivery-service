package io.github.sidyn4444.webhooks.worker.queue;

import io.github.sidyn4444.webhooks.common.model.DeliveryJob;
import io.github.sidyn4444.webhooks.common.queue.JobCodec;
import io.github.sidyn4444.webhooks.common.queue.RedisKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

/**
 * Asks one question on a loop: has anybody died holding a job?
 *
 * <p>Everything before this task made the evidence exist. {@code RPOPLPUSH} guarantees a job is
 * never in zero places (9b), the ack removes it only after a confirmed delivery (9d), 13b gave every
 * failure road an ending so nothing is parked on purpose any more, and 14a stamped each pickup with
 * a time. None of that causes anything to happen. <b>A job abandoned by a dead worker sits in
 * {@code webhooks:processing} forever, correctly recorded and completely ignored.</b> This class is
 * what closes that gap, and it is the last piece of the reliable-queue pattern from notes 2c.
 *
 * <p>It is deliberately the same shape as {@link io.github.sidyn4444.webhooks.worker.retry.RetryScheduler}:
 * a Redis-facing class that knows the operations ({@link InFlightIndex}) and a thread that knows
 * <i>when</i> (this one). Splitting them is what lets the operation be tested without waiting for a
 * timer, and lets the timer be reasoned about without Redis.
 *
 * <h2>Every pod runs one, and that is only safe because the pass is atomic</h2>
 *
 * <p>The sweep is a background thread, so three worker pods means three copies of it reading the
 * same two keys. Written as separate commands — read the stale entries, then move them — all three
 * would read the same answer and all three would act on it, producing a duplicate delivery per pod
 * per pass with nothing wrong anywhere.
 *
 * <p><b>The general rule, worth stating once and applying everywhere afterwards: a background thread
 * that runs in every worker is N copies of itself over shared state. Anywhere it reads something and
 * then acts on what it read has to be one script.</b> A single Redis command is already atomic and
 * needs no protection — it is the read-then-act sequence that needs it. That is why the reclaim runs
 * in Lua while the pickup stamp in 14a does not, and it is the identical argument to 12d.
 *
 * <h2>Why the timer numbers are what they are</h2>
 *
 * <p><b>Staleness threshold and poll interval are different numbers and are set independently.</b>
 * The threshold is derived from the hard 10-second delivery cap (9c): 60 seconds is six times the
 * longest a delivery could possibly take, so exceeding it is proof rather than suspicion. The
 * interval is how often anyone looks, and it adds to the detection delay — a worker that dies one
 * second after its stamp is not stale for 60 seconds, and then waits up to one more interval to be
 * noticed. Ten seconds gives a worst case around 70; sixty would give around 120 for no real saving.
 *
 * <p>There is no reason to poll as fast as the retry scheduler does. There the interval <i>is</i> the
 * schedule's accuracy, because a job due in one second noticed one second late has doubled its wait.
 * Here the threshold is already a minute, so a few seconds of discovery latency is noise — and
 * polling is not free, since the query rate is {@code pods / interval} and every pass occupies
 * Redis's single thread.
 */
@Component
public class RecoverySweep implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(RecoverySweep.class);

    private final InFlightIndex inFlight;

    /**
     * How long a job may be in flight before the worker holding it is presumed dead.
     *
     * <p>Derived, not chosen. A delivery is capped at 10 seconds by the HTTP client (9c), so a job
     * picked up 60 seconds ago cannot still be in progress — which is what makes abandonment
     * provable without any worker announcing that it is alive.
     *
     * <p>The cost of the margin is stated plainly: a crashed worker's job waits up to this long plus
     * one poll interval before anyone retries it. Shortening it eventually starts reclaiming work
     * from healthy-but-slow workers, which produces duplicate deliveries — so the threshold only
     * holds because the delivery timeout is hard-capped. <b>Raising that cap without raising this
     * would silently turn the sweep into a duplicate generator.</b>
     */
    private final Duration staleAfter;

    /** How often to look. See the class comment for why this is not the same number as above. */
    private final Duration pollInterval;

    /**
     * The most jobs to touch per phase in one pass.
     *
     * <p>Bounds how long the script occupies Redis's single thread. A node failure can strand
     * hundreds of jobs at once; handling them in bounded passes keeps every other client — including
     * the producer's LPUSH inside a live HTTP request — from waiting on this.
     */
    private final int batchSize;

    /**
     * The shutdown flag. {@code volatile} because it is written by the thread calling {@link #stop()}
     * and read by the sweep thread; without it the sweep could keep reading a cached {@code true} and
     * never stop (notes 9b).
     */
    private volatile boolean running = false;

    private Thread sweepThread;

    /**
     * Stopped in the same phase as the retry scheduler, and before the poll loop.
     *
     * <p>Spring stops lifecycle beans in descending phase order, and {@code LettuceConnectionFactory}
     * sits at phase {@code 0} — so anything that talks to Redis must be above it or it will find the
     * connection already closed. That defect and its fix are documented on {@code JobPoller.getPhase()}.
     *
     * <p>Sharing a phase with {@code RetryScheduler} is intentional: both of these <i>produce</i> work
     * onto the queue, the poll loop <i>consumes</i> it, and their order relative to each other does
     * not matter. What matters is that both stop before the consumer does, so nothing is still being
     * pushed onto a queue nobody is draining.
     */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    public RecoverySweep(InFlightIndex inFlight,
                         @Value("${webhook.sweep.stale-after:60s}") Duration staleAfter,
                         @Value("${webhook.sweep.poll-interval:10s}") Duration pollInterval,
                         @Value("${webhook.sweep.batch-size:100}") int batchSize) {
        this.inFlight = inFlight;
        this.staleAfter = staleAfter;
        this.pollInterval = pollInterval;
        this.batchSize = batchSize;
    }

    @Override
    public void start() {
        running = true;
        sweepThread = new Thread(this::runForever, "recovery-sweep");

        // DAEMON, like the retry scheduler and unlike the poll loop. The poll loop deliberately holds
        // the one non-daemon thread, so that IT is what defines "this worker is alive" (9b). If this
        // thread also held the JVM open, a worker whose poll loop had died could keep running,
        // sweeping jobs back onto a queue it no longer consumes — a zombie that looks healthy and
        // does nothing but move work in circles.
        sweepThread.setDaemon(true);
        sweepThread.start();

        log.info("Recovery sweep started — reclaiming jobs in '{}' idle for more than {}s, "
                        + "checking every {}s, up to {} per pass",
                RedisKeys.PROCESSING, staleAfter.toSeconds(), pollInterval.toSeconds(), batchSize);
    }

    private void runForever() {
        while (running) {
            try {
                sweepOnce();
            } catch (Exception e) {
                // Deliberately broad, for the reason established in 9b: an exception escaping here
                // kills this thread and nothing restarts it. The process would stay alive, pass
                // every health check, deliver new webhooks normally — and silently never recover an
                // abandoned job again. A failure that leaves the system looking healthy is worse
                // than one that takes it down.
                log.error("Recovery sweep pass failed, continuing: {}", e.toString());
            }

            sleepQuietly(pollInterval);
        }

        log.info("Recovery sweep stopped.");
    }

    /**
     * One pass. Redis has already done the work by the time this returns; everything here describes
     * what happened.
     */
    private void sweepOnce() {
        InFlightIndex.SweepResult result = inFlight.sweep(Instant.now(), staleAfter, batchSize);

        // Silence on a healthy system. This runs on every pod on a short interval, so a line per
        // pass would be thousands of "nothing to do" entries a day and the handful that matter
        // would be unfindable among them. A quiet log is a readable log.
        if (result.isEmpty()) {
            return;
        }

        if (result.adopted() > 0) {
            // Not an error, and worth saying why in the message itself: this is the expected
            // consequence of a pickup being two commands with a gap between them (14a). A steady
            // trickle is normal; a sudden flood means pods are dying mid-pickup.
            log.info("Adopted {} orphaned job(s) in '{}' — they had no pickup time, so one was "
                            + "recorded now and they become eligible for reclaim in {}s",
                    result.adopted(), RedisKeys.PROCESSING, staleAfter.toSeconds());
        }

        if (result.pruned() > 0) {
            log.info("Pruned {} stale index entr(ies) from '{}' — those jobs were no longer parked, "
                            + "so nothing was re-queued",
                    result.pruned(), RedisKeys.INFLIGHT);
        }

        for (String json : result.reclaimed()) {
            // 🔴 WARN, per reclaimed job, naming the event.
            //
            // This is the one place a worker death becomes visible, and it is deliberately one line
            // per job rather than a count. A reclaim is not routine — it means a process died
            // holding customer work — and naming the event id is what makes a job that is being
            // reclaimed over and over greppable. That matters because reclaim does NOT increment the
            // attempt number, so nothing in the system currently bounds that loop; this log line is
            // what stops it from also being invisible.
            //
            // Parsed only for the log. The move is already done, and the job on the queue needs
            // nothing from this code — so a parse failure here must not stop the rest of the batch
            // being reported. The poll loop dead-letters the unparseable message properly when it
            // picks it up (13b).
            try {
                DeliveryJob job = JobCodec.fromJson(json);
                log.warn("RECLAIMED event {} — no worker completed it within {}s, re-queued for "
                                + "attempt {} of {}",
                        job.eventId(), staleAfter.toSeconds(), job.attemptNumber(), job.maxRetries());
            } catch (IllegalArgumentException e) {
                log.warn("Reclaimed an unparseable message onto '{}': {}", RedisKeys.QUEUE, e.toString());
            }
        }

        if (!result.reclaimed().isEmpty()) {
            log.info("Recovery sweep re-queued {} abandoned job(s) from '{}' to '{}' ({} still in flight)",
                    result.reclaimed().size(), RedisKeys.PROCESSING, RedisKeys.QUEUE, inFlight.size());
        }
    }

    @Override
    public void stop() {
        log.info("Shutdown requested — stopping recovery sweep.");
        running = false;

        if (sweepThread != null) {
            try {
                // The flag rather than interrupt(), with a generous join — same reasoning as the
                // poll loop and the retry scheduler (9b, 12d). Worst case is one poll interval plus
                // a script execution, comfortably inside Kubernetes' 30-second grace period.
                sweepThread.join(pollInterval.plusSeconds(2).toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while waiting for the recovery sweep to finish.");
            }
        }
    }

    private void sleepQuietly(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            // Restore the flag the exception cleared, so anything further up this thread can still
            // see that an interrupt was requested. Swallowing it silently makes a thread unstoppable
            // by the standard mechanism (notes 9b).
            Thread.currentThread().interrupt();
            running = false;
        }
    }
}
