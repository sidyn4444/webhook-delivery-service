package io.github.sidyn4444.webhooks.common.queue;

import io.github.sidyn4444.webhooks.common.model.DeliveryJob;
import io.github.sidyn4444.webhooks.common.model.Event;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Proves the queue's wire format actually works.
 *
 * <p>This is the first test in the project, and it exists because everything after it depends
 * on an assumption that is easy to state and easy to get wrong: that a job written by the
 * producer can be read back by the worker, unchanged. Nothing in the compiler checks that. The
 * only proof is running it.
 */
class JobCodecTest {

    private static final String EVENT_ID = "3f2b7c1a-9d4e-4f6b-8a2c-1e5d7f9b3c4d";

    private static DeliveryJob sampleJob() {
        Event event = new Event(EVENT_ID, "https://webhook.site/abc", "{\"order\":42}");
        return DeliveryJob.firstAttempt(event, 5);
    }

    @Test
    @DisplayName("a job survives the round trip through JSON unchanged")
    void roundTripsUnchanged() {
        DeliveryJob original = sampleJob();

        DeliveryJob restored = JobCodec.fromJson(JobCodec.toJson(original));

        // Records generate a value-based equals, so this compares all six fields at once.
        // If it passes, the object that comes out of Redis is indistinguishable from the one
        // that went in.
        assertThat(restored).isEqualTo(original);
    }

    @Test
    @DisplayName("the Instant survives with full precision, not just to the second")
    void preservesInstantPrecision() {
        // Deliberately a value with sub-second precision. Several date formats silently
        // truncate to whole seconds, and the resulting bug is invisible until someone
        // measures a duration and finds it is always a round number.
        Instant precise = Instant.parse("2026-07-22T14:03:11.482137Z");
        DeliveryJob original = new DeliveryJob(
                EVENT_ID, "https://webhook.site/abc", "{}", 1, 5, precise);

        DeliveryJob restored = JobCodec.fromJson(JobCodec.toJson(original));

        assertThat(restored.enqueuedAt()).isEqualTo(precise);
    }

    @Test
    @DisplayName("JSON keys are snake_case, and the timestamp is readable ISO-8601")
    void usesTheAgreedWireFormat() {
        String json = JobCodec.toJson(sampleJob());

        // The exact keys matter: they are the contract between two processes. A test on the
        // round trip alone would still pass if BOTH sides silently switched to camelCase,
        // so the field names are asserted directly.
        assertThat(json)
                .contains("\"event_id\"")
                .contains("\"subscriber_url\"")
                .contains("\"payload\"")
                .contains("\"attempt_number\"")
                .contains("\"max_retries\"")
                .contains("\"enqueued_at\"")
                .doesNotContain("eventId")
                .doesNotContain("subscriberUrl");

        // ISO-8601 text rather than a numeric epoch, so `redis-cli LRANGE` is readable.
        assertThat(json).containsPattern("\"enqueued_at\":\"\\d{4}-\\d{2}-\\d{2}T");
    }

    @Test
    @DisplayName("an unknown field is ignored, so a rolling deploy cannot break old workers")
    void toleratesUnknownFields() {
        // Simulates the real deployment scenario: the producer has been updated and now
        // writes a field this (older) worker's DeliveryJob does not have. Both versions run
        // at once during any rolling update. With FAIL_ON_UNKNOWN_PROPERTIES left enabled,
        // this line would throw and every old worker would fail on every message.
        String fromNewerProducer = """
                {"event_id":"%s","subscriber_url":"https://webhook.site/abc",
                 "payload":"{}","attempt_number":1,"max_retries":5,
                 "enqueued_at":"2026-07-22T14:03:11.482Z",
                 "priority":"high"}
                """.formatted(EVENT_ID);

        DeliveryJob restored = JobCodec.fromJson(fromNewerProducer);

        assertThat(restored.eventId()).isEqualTo(EVENT_ID);
        assertThat(restored.attemptNumber()).isEqualTo(1);
    }

    @Test
    @DisplayName("malformed JSON fails loudly rather than producing a broken job")
    void rejectsMalformedJson() {
        // A truncated or hand-edited entry must not deserialize into a half-built object that
        // then fails somewhere unrelated. Failing at the parse point keeps the diagnosis local.
        assertThatThrownBy(() -> JobCodec.fromJson("{\"event_id\": \"abc\", "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Failed to deserialize");
    }

    @Test
    @DisplayName("JSON that parses but violates an invariant is still rejected")
    void enforcesInvariantsOnDeserialization() {
        // The compact constructor runs on Jackson's construction path too, so a structurally
        // valid message carrying impossible data cannot become a DeliveryJob. Without this,
        // a corrupted attempt_number would flow into the retry logic as real state.
        String zeroAttempt = """
                {"event_id":"%s","subscriber_url":"https://webhook.site/abc",
                 "payload":"{}","attempt_number":0,"max_retries":5,
                 "enqueued_at":"2026-07-22T14:03:11.482Z"}
                """.formatted(EVENT_ID);

        assertThatThrownBy(() -> JobCodec.fromJson(zeroAttempt))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
