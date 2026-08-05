package io.github.sidyn4444.webhooks.worker.retry;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Proves the wait between retries grows the way it claims to, and that the randomness is real.
 *
 * <p>Two different properties live in this class and they need opposite kinds of test. The
 * schedule is exact arithmetic — 1s, 2s, 4s, 8s, 16s, then a ceiling — so it can be asserted on
 * the nose. The jitter is deliberately random, so there is no single correct answer to compare
 * against and the only honest assertion is a range.
 *
 * <p>A range assertion on its own has a hole worth naming: an implementation that ignored
 * randomness entirely and always returned the base delay would sit inside the range on every
 * run and pass. So the range test is paired with one that requires the values to actually
 * differ. Neither test is sufficient alone.
 */
class RetryBackoffTest {

    // ---------------------------------------------------------------------
    // The schedule
    // ---------------------------------------------------------------------

    @Test
    @DisplayName("the delay doubles: 1s, 2s, 4s, 8s, 16s")
    void delayDoublesEachAttempt() {
        assertThat(RetryBackoff.baseDelay(1)).isEqualTo(Duration.ofSeconds(1));
        assertThat(RetryBackoff.baseDelay(2)).isEqualTo(Duration.ofSeconds(2));
        assertThat(RetryBackoff.baseDelay(3)).isEqualTo(Duration.ofSeconds(4));
        assertThat(RetryBackoff.baseDelay(4)).isEqualTo(Duration.ofSeconds(8));
        assertThat(RetryBackoff.baseDelay(5)).isEqualTo(Duration.ofSeconds(16));
    }

    @Test
    @DisplayName("the delay stops growing at 16s instead of running away")
    void delayIsCappedAtSixteenSeconds() {
        assertThat(RetryBackoff.baseDelay(6)).isEqualTo(Duration.ofSeconds(16));
        assertThat(RetryBackoff.baseDelay(7)).isEqualTo(Duration.ofSeconds(16));
        assertThat(RetryBackoff.baseDelay(99)).isEqualTo(Duration.ofSeconds(16));
    }

    @Test
    @DisplayName("the cap holds at an attempt number large enough to overflow a naive shift")
    void capHoldsAtExtremeAttemptNumbers() {
        // 1L << 63 overflows to a negative number. The implementation clamps the exponent, and
        // this is the input that proves it — a delay that came back negative or enormous would
        // schedule a retry in the past or in a decade, and neither raises an error anywhere.
        assertThat(RetryBackoff.baseDelay(Integer.MAX_VALUE)).isEqualTo(Duration.ofSeconds(16));
        assertThat(RetryBackoff.baseDelay(1000)).isEqualTo(Duration.ofSeconds(16));
    }

    @Test
    @DisplayName("an attempt number below 1 is refused rather than quietly accepted")
    void rejectsAttemptNumbersBelowOne() {
        assertThatThrownBy(() -> RetryBackoff.baseDelay(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be >= 1");

        assertThatThrownBy(() -> RetryBackoff.baseDelay(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ---------------------------------------------------------------------
    // The jitter
    // ---------------------------------------------------------------------

    @Test
    @DisplayName("jitter keeps every delay within 20% of the scheduled one")
    void jitterStaysWithinTwentyPercent() {
        for (int attempt = 1; attempt <= 6; attempt++) {
            long base = RetryBackoff.baseDelay(attempt).toMillis();
            long low = (long) (base * 0.8);
            long high = (long) (base * 1.2);

            for (int run = 0; run < 1_000; run++) {
                long actual = RetryBackoff.nextDelay(attempt).toMillis();
                assertThat(actual)
                        .as("attempt %d, run %d: %dms outside [%d, %d]", attempt, run, actual, low, high)
                        .isBetween(low, high);
            }
        }
    }

    @Test
    @DisplayName("a delay is never negative, which would schedule a retry in the past")
    void delayIsNeverNegative() {
        for (int run = 0; run < 1_000; run++) {
            assertThat(RetryBackoff.nextDelay(1).toMillis()).isNotNegative();
        }
    }

    @Test
    @DisplayName("the jitter actually varies — a fixed delay would pass the range test")
    void jitterProducesDifferentValues() {
        Set<Long> seen = new HashSet<>();
        for (int run = 0; run < 200; run++) {
            seen.add(RetryBackoff.nextDelay(5).toMillis());
        }

        // The window for attempt 5 is 16,000ms +/- 3,200ms, so 6,401 possible values. Drawing
        // 200 times and seeing only one of them would mean the randomness is not happening.
        assertThat(seen)
                .as("200 draws produced %d distinct delays", seen.size())
                .hasSizeGreaterThan(1);
    }

    @Test
    @DisplayName("jitter spreads both earlier and later than the scheduled delay")
    void jitterGoesBothDirections() {
        long base = RetryBackoff.baseDelay(5).toMillis();
        boolean sawEarlier = false;
        boolean sawLater = false;

        for (int run = 0; run < 1_000 && !(sawEarlier && sawLater); run++) {
            long actual = RetryBackoff.nextDelay(5).toMillis();
            if (actual < base) {
                sawEarlier = true;
            }
            if (actual > base) {
                sawLater = true;
            }
        }

        // One-sided jitter still passes the range check but only ever delays, which quietly
        // stretches the whole retry schedule instead of spreading a thundering herd.
        assertThat(sawEarlier).as("some delay landed before the scheduled time").isTrue();
        assertThat(sawLater).as("some delay landed after the scheduled time").isTrue();
    }
}
