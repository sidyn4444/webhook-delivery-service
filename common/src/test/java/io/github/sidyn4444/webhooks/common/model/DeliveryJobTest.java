package io.github.sidyn4444.webhooks.common.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Proves the unit of work on the queue counts its own attempts correctly.
 *
 * <p>A stateless worker decides retry-versus-dead-letter from the message alone, so the attempt
 * counter carried inside a job is the only thing standing between "try again" and "give up".
 * An off-by-one here is not a crash: it is every event in the system silently getting one extra
 * delivery, or one fewer, forever.
 *
 * <p>These assertions started life as nine throwaway checks in a {@code jshell} session during
 * 7b. They are here now because the question changed from "did I build this right?" to "must
 * this keep working?", and only the second question deserves a permanent test.
 */
class DeliveryJobTest {

    private static final String EVENT_ID = "3f2b7c1a-9d4e-4f6b-8a2c-1e5d7f9b3c4d";
    private static final String URL = "https://webhook.site/abc";
    private static final String PAYLOAD = "{\"order\":42}";

    private static Event sampleEvent() {
        return new Event(EVENT_ID, URL, PAYLOAD);
    }

    // ---------------------------------------------------------------------
    // Where a job starts
    // ---------------------------------------------------------------------

    @Test
    @DisplayName("a new job starts at attempt 1, not 0")
    void firstAttemptStartsAtOne() {
        DeliveryJob job = DeliveryJob.firstAttempt(sampleEvent(), 5);

        assertThat(job.attemptNumber()).isEqualTo(1);
        assertThat(job.maxRetries()).isEqualTo(5);
    }

    @Test
    @DisplayName("a new job carries the event's fields across unchanged")
    void firstAttemptCopiesTheEvent() {
        DeliveryJob job = DeliveryJob.firstAttempt(sampleEvent(), 5);

        assertThat(job.eventId()).isEqualTo(EVENT_ID);
        assertThat(job.subscriberUrl()).isEqualTo(URL);
        assertThat(job.payload()).isEqualTo(PAYLOAD);
        assertThat(job.enqueuedAt()).isNotNull();
    }

    // ---------------------------------------------------------------------
    // Moving to the next attempt
    // ---------------------------------------------------------------------

    @Test
    @DisplayName("the next attempt returns a copy and leaves the original untouched")
    void nextAttemptDoesNotMutateTheOriginal() {
        DeliveryJob first = DeliveryJob.firstAttempt(sampleEvent(), 5);
        DeliveryJob second = first.nextAttempt();

        assertThat(second.attemptNumber()).isEqualTo(2);
        assertThat(first.attemptNumber())
                .as("the original job must still read 1 — a mutation here would corrupt a job "
                        + "another thread is holding")
                .isEqualTo(1);
        assertThat(second).isNotSameAs(first);
    }

    @Test
    @DisplayName("only the attempt counter moves — identity and queue time are preserved")
    void nextAttemptChangesNothingElse() {
        DeliveryJob first = DeliveryJob.firstAttempt(sampleEvent(), 5);
        DeliveryJob second = first.nextAttempt();

        assertThat(second.eventId()).isEqualTo(first.eventId());
        assertThat(second.subscriberUrl()).isEqualTo(first.subscriberUrl());
        assertThat(second.payload()).isEqualTo(first.payload());
        assertThat(second.maxRetries()).isEqualTo(first.maxRetries());
        assertThat(second.enqueuedAt())
                .as("enqueuedAt records when the event entered the queue, not when it was retried")
                .isEqualTo(first.enqueuedAt());
    }

    @Test
    @DisplayName("attempts keep incrementing by exactly one across a chain of retries")
    void attemptsIncrementOneAtATime() {
        DeliveryJob job = DeliveryJob.firstAttempt(sampleEvent(), 5);

        for (int expected = 2; expected <= 6; expected++) {
            job = job.nextAttempt();
            assertThat(job.attemptNumber()).isEqualTo(expected);
        }
    }

    // ---------------------------------------------------------------------
    // The give-up boundary. This is the assertion with real consequences.
    // ---------------------------------------------------------------------

    @Test
    @DisplayName("the retry budget runs out exactly after the last allowed attempt")
    void retryBudgetBoundaryIsExact() {
        DeliveryJob atLimit = new DeliveryJob(EVENT_ID, URL, PAYLOAD, 5, 5, Instant.now());
        DeliveryJob pastLimit = new DeliveryJob(EVENT_ID, URL, PAYLOAD, 6, 5, Instant.now());

        assertThat(atLimit.hasRetriesLeft())
                .as("attempt 5 of a 5-retry budget is still allowed")
                .isTrue();
        assertThat(pastLimit.hasRetriesLeft())
                .as("attempt 6 of a 5-retry budget is not")
                .isFalse();
    }

    @Test
    @DisplayName("a job with zero retries allowed still gets its first delivery")
    void zeroRetriesStillDeliversOnce() {
        DeliveryJob job = DeliveryJob.firstAttempt(sampleEvent(), 0);

        assertThat(job.attemptNumber()).isEqualTo(1);
        assertThat(job.hasRetriesLeft())
                .as("maxRetries=0 means no RE-tries, not no delivery at all")
                .isFalse();
    }

    // ---------------------------------------------------------------------
    // Invariants — a malformed job must never reach the queue
    // ---------------------------------------------------------------------

    @Test
    @DisplayName("an attempt number below 1 is refused")
    void rejectsAttemptNumberBelowOne() {
        assertThatThrownBy(() -> new DeliveryJob(EVENT_ID, URL, PAYLOAD, 0, 5, Instant.now()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("attemptNumber");

        assertThatThrownBy(() -> new DeliveryJob(EVENT_ID, URL, PAYLOAD, -1, 5, Instant.now()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("a negative retry budget is refused")
    void rejectsNegativeMaxRetries() {
        assertThatThrownBy(() -> new DeliveryJob(EVENT_ID, URL, PAYLOAD, 1, -1, Instant.now()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxRetries");
    }

    @Test
    @DisplayName("every required field is refused when null")
    void rejectsNullFields() {
        assertThatThrownBy(() -> new DeliveryJob(null, URL, PAYLOAD, 1, 5, Instant.now()))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("eventId");

        assertThatThrownBy(() -> new DeliveryJob(EVENT_ID, null, PAYLOAD, 1, 5, Instant.now()))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("subscriberUrl");

        assertThatThrownBy(() -> new DeliveryJob(EVENT_ID, URL, null, 1, 5, Instant.now()))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("payload");

        assertThatThrownBy(() -> new DeliveryJob(EVENT_ID, URL, PAYLOAD, 1, 5, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("enqueuedAt");
    }

    // ---------------------------------------------------------------------
    // Anti-vacuity: a constructor that threw on everything would pass every
    // rejection test above.
    // ---------------------------------------------------------------------

    @Test
    @DisplayName("a well-formed job is accepted")
    void acceptsAValidJob() {
        DeliveryJob job = new DeliveryJob(EVENT_ID, URL, PAYLOAD, 1, 5, Instant.now());

        assertThat(job.eventId()).isEqualTo(EVENT_ID);
        assertThat(job.hasRetriesLeft()).isTrue();
    }
}
