package io.github.sidyn4444.webhooks.worker.delivery;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the retry-versus-dead-letter decision is made where we think it is.
 *
 * <p>This class is three lines of branching, and it decides whether a failed webhook is tried
 * again or abandoned forever. Getting it wrong is not a crash — it is a system that silently
 * discards events a subscriber would have accepted on the second attempt, or one that hammers a
 * server forever over a request that can never succeed. Neither shows up as an error.
 *
 * <p>The tests that matter most here are the boundaries. Asserting that 503 is retriable would
 * also pass for an implementation that retried everything above 400, so each rule is pinned with
 * one value on each side of the line it claims to draw.
 */
class DeliveryClassifierTest {

    // ---------------------------------------------------------------------
    // Success
    // ---------------------------------------------------------------------

    @ParameterizedTest(name = "{0} is a success, so the job is done")
    @ValueSource(ints = {200, 201, 202, 204, 299})
    @DisplayName("any 2xx counts as delivered, not just 200")
    void anySuccessIsDelivered(int status) {
        assertThat(DeliveryClassifier.classify(DeliveryResult.responded(status, 12)))
                .isEqualTo(DeliveryOutcome.DELIVERED);
    }

    // ---------------------------------------------------------------------
    // Worth trying again
    // ---------------------------------------------------------------------

    @ParameterizedTest(name = "{0} means their side is broken, so retry")
    @ValueSource(ints = {500, 502, 503, 504, 599})
    @DisplayName("a 5xx is the subscriber's problem and may pass on a later attempt")
    void serverErrorsAreRetriable(int status) {
        assertThat(DeliveryClassifier.classify(DeliveryResult.responded(status, 12)))
                .isEqualTo(DeliveryOutcome.RETRIABLE);
    }

    @ParameterizedTest(name = "{0} is a temporary refusal, so retry")
    @ValueSource(ints = {408, 429})
    @DisplayName("429 and 408 are retriable even though they are 4xx")
    void rateLimitAndTimeoutAreRetriable(int status) {
        assertThat(DeliveryClassifier.classify(DeliveryResult.responded(status, 12)))
                .isEqualTo(DeliveryOutcome.RETRIABLE);
    }

    @Test
    @DisplayName("no response at all is retriable — silence is not a rejection")
    void noResponseIsRetriable() {
        assertThat(DeliveryClassifier.classify(DeliveryResult.failed(10_011, "timeout")))
                .isEqualTo(DeliveryOutcome.RETRIABLE);
    }

    @Test
    @DisplayName("a refused connection is retriable — the server may simply be restarting")
    void connectionRefusedIsRetriable() {
        assertThat(DeliveryClassifier.classify(DeliveryResult.failed(5, "connection refused")))
                .isEqualTo(DeliveryOutcome.RETRIABLE);
    }

    // ---------------------------------------------------------------------
    // Hopeless
    // ---------------------------------------------------------------------

    @ParameterizedTest(name = "{0} means the request itself is wrong, so stop")
    @ValueSource(ints = {400, 401, 403, 404, 410, 422})
    @DisplayName("most 4xx responses are permanent — retrying cannot change the answer")
    void clientErrorsArePermanent(int status) {
        assertThat(DeliveryClassifier.classify(DeliveryResult.responded(status, 12)))
                .isEqualTo(DeliveryOutcome.PERMANENT);
    }

    // ---------------------------------------------------------------------
    // The boundaries. These are the tests with real weight.
    // ---------------------------------------------------------------------

    @Test
    @DisplayName("the retry line sits exactly at 500: 499 gives up, 500 retries")
    void fiveHundredIsTheExactBoundary() {
        assertThat(DeliveryClassifier.classify(DeliveryResult.responded(499, 12)))
                .as("499 is still a client error")
                .isEqualTo(DeliveryOutcome.PERMANENT);

        assertThat(DeliveryClassifier.classify(DeliveryResult.responded(500, 12)))
                .as("500 is the first server error")
                .isEqualTo(DeliveryOutcome.RETRIABLE);
    }

    @Test
    @DisplayName("the success band is exactly 200-299 on both ends")
    void successBandIsExactlyTwoHundreds() {
        assertThat(DeliveryClassifier.classify(DeliveryResult.responded(199, 12)))
                .as("199 is below the success band")
                .isNotEqualTo(DeliveryOutcome.DELIVERED);

        assertThat(DeliveryClassifier.classify(DeliveryResult.responded(200, 12)))
                .isEqualTo(DeliveryOutcome.DELIVERED);

        assertThat(DeliveryClassifier.classify(DeliveryResult.responded(299, 12)))
                .isEqualTo(DeliveryOutcome.DELIVERED);

        assertThat(DeliveryClassifier.classify(DeliveryResult.responded(300, 12)))
                .as("a redirect is not a delivery — we never followed it")
                .isNotEqualTo(DeliveryOutcome.DELIVERED);
    }

    @Test
    @DisplayName("428 and 430 are permanent — only 429 itself is carved out")
    void rateLimitCarveOutIsExact() {
        assertThat(DeliveryClassifier.classify(DeliveryResult.responded(428, 12)))
                .isEqualTo(DeliveryOutcome.PERMANENT);

        assertThat(DeliveryClassifier.classify(DeliveryResult.responded(430, 12)))
                .isEqualTo(DeliveryOutcome.PERMANENT);
    }

    // ---------------------------------------------------------------------
    // Anti-vacuity: a classifier that returned one value for everything would
    // pass any single group above. It cannot pass this.
    // ---------------------------------------------------------------------

    @Test
    @DisplayName("all three outcomes are actually reachable")
    void everyOutcomeIsProduced() {
        assertThat(DeliveryClassifier.classify(DeliveryResult.responded(200, 1)))
                .isEqualTo(DeliveryOutcome.DELIVERED);
        assertThat(DeliveryClassifier.classify(DeliveryResult.responded(503, 1)))
                .isEqualTo(DeliveryOutcome.RETRIABLE);
        assertThat(DeliveryClassifier.classify(DeliveryResult.responded(404, 1)))
                .isEqualTo(DeliveryOutcome.PERMANENT);
    }
}
