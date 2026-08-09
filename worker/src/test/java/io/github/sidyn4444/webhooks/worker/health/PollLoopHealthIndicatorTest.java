package io.github.sidyn4444.webhooks.worker.health;

import io.github.sidyn4444.webhooks.worker.queue.JobPoller;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

/**
 * Tests for the one piece of health logic this project writes itself.
 *
 * <p>Two tests, and the second one is the reason this file exists. The first is close to
 * restating the method; the second would <b>fail</b> against the most likely wrong
 * implementation, which is what makes it worth having.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("PollLoopHealthIndicator")
class PollLoopHealthIndicatorTest {

    @Mock
    JobPoller poller;

    @Test
    @DisplayName("reports UP while the poll thread is alive")
    void upWhenThreadAlive() {
        when(poller.isPollThreadAlive()).thenReturn(true);

        Health health = new PollLoopHealthIndicator(poller).health();

        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails()).containsEntry("pollThread", "alive");
    }

    /**
     * 🔴 The test that carries the weight.
     *
     * <p>The mock is set up to describe the exact failure the class exists to catch: the
     * {@code running} flag still says {@code true} — because nothing set it to false — while the
     * thread itself is gone. That is what happens when the poll thread dies from an
     * {@link Error} rather than an {@link Exception}, which {@code pollForever}'s catch block
     * does not handle.
     *
     * <p><b>Why this is not a circular test.</b> The obvious wrong implementation is
     * {@code poller.isRunning() ? up() : down()} — it reads naturally, it compiles, and it is
     * wrong. Against this mock that implementation returns UP, and this test goes red. A test
     * that only stubbed {@code isPollThreadAlive()} without also making {@code isRunning()}
     * disagree could not tell the two implementations apart.
     */
    @Test
    @DisplayName("reports DOWN when the thread is gone even though the running flag still says true")
    void downWhenThreadDeadButFlagStillTrue() {
        when(poller.isPollThreadAlive()).thenReturn(false);
        // The flag deliberately disagrees. An implementation reading THIS would report UP.
        lenient().when(poller.isRunning()).thenReturn(true);

        Health health = new PollLoopHealthIndicator(poller).health();

        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        assertThat(health.getDetails())
                .containsEntry("pollThread", "not alive")
                .containsEntry("consequence", "no jobs are being consumed from the queue");
    }
}
