package io.github.sidyn4444.webhooks.worker.delivery;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link WebhookSender}'s startup guard.
 *
 * <p>⚠️ <b>This file deliberately does not test {@code send()}, and the reason is worth stating
 * rather than leaving as a gap.</b> {@code send()} drives a {@code WebClient}, whose API is a
 * fluent chain — {@code post().uri(…).header(…).bodyValue(…).exchangeToMono(…)}. Mocking it means
 * mocking every link in that chain and having each one return the next mock, which produces a
 * test that asserts the shape of the chain rather than any behaviour, and that breaks whenever the
 * chain is edited even when the behaviour is unchanged. That is the circular test in its most
 * expensive form.
 *
 * <p>{@code send()} was instead proven by execution in note 9c against a purpose-built local
 * subscriber: real 200/201/204 responses, real 500 and 404, a refused connection at 13ms, a DNS
 * failure at 283ms, and a deliberately hanging endpoint cut off by the hard cap at 10,011ms. That
 * evidence is stronger than any mock could produce — it is simply not automated, which is exactly
 * the distinction 17e is about.
 *
 * <p>What IS tested here is the constructor guard, because it is a security control with a
 * one-line failure mode: a worker that starts without an HMAC secret sends unsigned webhooks, and
 * a subscriber cannot distinguish an unsigned webhook from a forged one.
 */
@DisplayName("WebhookSender")
class WebhookSenderTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private WebhookSender build(String secret) {
        // The real builder, not a mock: the constructor only calls build() on it, so there is
        // nothing to fake and nothing gained by faking it.
        return new WebhookSender(WebClient.builder(), TIMEOUT, secret);
    }

    @Nested
    @DisplayName("the HMAC secret guard — refuse to start rather than send unsigned")
    class SecretGuard {

        @Test
        @DisplayName("a null secret refuses to construct")
        void nullSecretIsRefused() {
            assertThatThrownBy(() -> build(null))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("missing or blank");
        }

        @ParameterizedTest(name = "a secret of [{0}] refuses to construct")
        @ValueSource(strings = {"", " ", "   ", "\t", "\n"})
        @DisplayName("🔴 blank and whitespace-only secrets are both refused")
        void blankSecretsAreRefused(String blank) {
            // Whitespace matters as much as empty, and is the easier one to arrive at by
            // accident: an env var set to a trailing space in a .env file, or a Kubernetes secret
            // holding a stray newline, both produce a non-empty string that is still no secret at
            // all. A guard written as `secret.isEmpty()` passes every one of these.
            assertThatThrownBy(() -> build(blank))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("missing or blank");
        }

        @Test
        @DisplayName("the refusal explains how to fix it")
        void theMessageIsActionable() {
            assertThatThrownBy(() -> build(null))
                    .hasMessageContaining("HMAC_SECRET")
                    .hasMessageContaining("openssl rand -hex 32");

            // An operator meets this message at 3am with a pod in CrashLoopBackOff. A message
            // that names the variable and the command to generate a value is the difference
            // between a two-minute fix and a bug report.
        }

        @Test
        @DisplayName("the reason is stated, not just the rule")
        void theMessageExplainsWhy() {
            assertThatThrownBy(() -> build("   "))
                    .hasMessageContaining("unsigned webhook is indistinguishable from a forged one");
        }

        @Test
        @DisplayName("a real secret constructs cleanly — without this, the file proves nothing")
        void aValidSecretIsAccepted() {
            // The anti-vacuity case. Every other test here expects a refusal, and all of them
            // would pass against a constructor that threw unconditionally.
            assertThatCode(() -> build("0123456789abcdef0123456789abcdef")).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("a short secret is accepted — length is not the guard's job")
        void lengthIsNotChecked() {
            // Stated as a test rather than left implicit, because the natural assumption on
            // reading the guard is that it validates the secret. It does not: it checks presence
            // only. A one-character secret starts the worker happily.
            assertThatCode(() -> build("x")).doesNotThrowAnyException();

            assertThat(true)
                    .as("a minimum-length check is a deliberate non-feature; the secret is "
                            + "generated by the operator and validating its strength here would "
                            + "be security theatre against a value we do not control")
                    .isTrue();
        }
    }
}
