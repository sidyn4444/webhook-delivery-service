package io.github.sidyn4444.webhooks.worker.health;

import io.github.sidyn4444.webhooks.worker.queue.JobPoller;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Reports whether the worker's poll loop thread is still alive.
 *
 * <p><b>Why this class exists.</b> Every other health check in this application is supplied for
 * free: Spring Boot registers a {@code redis} indicator because the Redis library is on the
 * classpath and a {@code db} indicator because a datasource is configured. Nothing, however,
 * knows that this application's real work happens on a thread created by
 * {@link JobPoller#start()} — so nothing can report that the thread has died.
 *
 * <p>🔴 <b>That failure is the reason the worker has an HTTP port at all.</b> A worker whose poll
 * thread has terminated is a process that is up, holds healthy connections to Redis and Postgres,
 * answers every other health check with {@code UP}, and does no work at all, forever. There is no
 * request to fail and no exception left to log, so from the outside it is indistinguishable from a
 * worker that simply has nothing to do.
 *
 * <p><b>Why the result belongs in LIVENESS, not readiness.</b> The rule is: <i>would restarting
 * the container fix this?</i> A dead thread — yes, a restart creates a new one, so liveness is
 * correct and Kubernetes will replace the pod. An unreachable Redis — no, restarting cannot start
 * somebody else's server, so that belongs in readiness. Putting this check in readiness would be
 * useless: nothing sends the worker traffic, so withholding traffic from it changes nothing and
 * the broken pod would sit there indefinitely.
 *
 * <p>Wired into the liveness group by name in {@code application.properties}. Spring derives the
 * indicator's name from the bean name with the {@code HealthIndicator} suffix removed, so this
 * class is referred to as {@code pollLoop}.
 */
@Component
public class PollLoopHealthIndicator implements HealthIndicator {

    private final JobPoller poller;

    public PollLoopHealthIndicator(JobPoller poller) {
        this.poller = poller;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Reads {@link JobPoller#isPollThreadAlive()} rather than {@code isRunning()} on purpose.
     * {@code isRunning()} returns a flag the application sets; the thread's own liveness is what
     * the JVM knows. They differ exactly when the thread has died unexpectedly, which is the only
     * case this indicator exists to catch — so reading the flag would report {@code UP} for the
     * one failure being looked for.
     *
     * <p>The {@code withDetail} values are visible only when health details are exposed; the
     * probe itself needs nothing but the status.
     */
    @Override
    public Health health() {
        boolean alive = poller.isPollThreadAlive();

        if (alive) {
            return Health.up()
                    .withDetail("pollThread", "alive")
                    .build();
        }

        return Health.down()
                .withDetail("pollThread", "not alive")
                .withDetail("consequence", "no jobs are being consumed from the queue")
                .build();
    }
}
