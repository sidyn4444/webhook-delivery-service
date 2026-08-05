package io.github.sidyn4444.webhooks.worker.dlq;

import io.github.sidyn4444.webhooks.common.model.DeliveryJob;
import io.github.sidyn4444.webhooks.common.model.DlqReason;
import io.github.sidyn4444.webhooks.common.model.Event;
import io.github.sidyn4444.webhooks.common.queue.RedisKeys;
import io.github.sidyn4444.webhooks.worker.delivery.DeliveryResult;
import io.github.sidyn4444.webhooks.worker.queue.InFlightIndex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link DeadLetterQueue}.
 *
 * <p><b>The same relay-baton rule as {@code JobPoller}, in a second place.</b> Dead-lettering is
 * two operations against two different Redis keys:
 *
 * <ol>
 *   <li><b>record</b> — push the dead-letter entry onto {@code webhooks:dlq};
 *   <li><b>release</b> — remove the job from {@code webhooks:processing}.
 * </ol>
 *
 * <p>They must happen in that order. Reversed, a crash in between leaves the job removed from
 * {@code processing} with no dead-letter entry ever written: it exists nowhere, nothing sweeps it
 * up, and there is no record it was ever received. In the written order the same crash leaves the
 * job parked in {@code processing}, the recovery sweep reclaims it, and the cost is one duplicate
 * attempt.
 *
 * <p>No return value expresses "before", so the ordering is asserted with {@link InOrder} — and
 * the negative case matters just as much: <b>if the record fails, the release must not happen at
 * all.</b>
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("DeadLetterQueue")
class DeadLetterQueueTest {

    @Mock private StringRedisTemplate redis;
    @Mock private ListOperations<String, String> listOps;
    @Mock private InFlightIndex inFlight;

    private DeadLetterQueue deadLetters;

    private static final String EVENT_ID = "3f2504e0-4f89-11d3-9a0c-0305e82c3301";
    private static final String ORIGINAL_JSON = "{\"event_id\":\"" + EVENT_ID + "\"}";

    @BeforeEach
    void setUp() {
        lenient().when(redis.opsForList()).thenReturn(listOps);
        deadLetters = new DeadLetterQueue(redis, inFlight);
    }

    private DeliveryJob job() {
        return DeliveryJob.firstAttempt(new Event(EVENT_ID, "https://8.8.8.8/hook", "payload"), 5);
    }

    private DeliveryResult failed() {
        return DeliveryResult.responded(404, 12);
    }

    @Nested
    @DisplayName("the ordering — record before release")
    class Ordering {

        @Test
        @DisplayName("🔴 pushes to the DLQ BEFORE removing from processing")
        void recordsBeforeReleasing() {
            when(listOps.leftPush(eq(RedisKeys.DLQ), anyString())).thenReturn(1L);
            when(listOps.remove(eq(RedisKeys.PROCESSING), eq(1L), anyString())).thenReturn(1L);

            deadLetters.deadLetter(ORIGINAL_JSON, job(), DlqReason.RETRIES_EXHAUSTED, failed());

            // Two plain verify() calls would pass against the reversed implementation, because
            // both calls still happen. Only the ordered verifier can see the difference.
            InOrder ordering = inOrder(listOps);
            ordering.verify(listOps).leftPush(eq(RedisKeys.DLQ), anyString());
            ordering.verify(listOps).remove(RedisKeys.PROCESSING, 1L, ORIGINAL_JSON);
        }

        @Test
        @DisplayName("🔴 if the DLQ push returns no result, the job is NOT released")
        void aFailedRecordMeansNoRelease() {
            when(listOps.leftPush(eq(RedisKeys.DLQ), anyString())).thenReturn(null);

            boolean result = deadLetters.deadLetter(
                    ORIGINAL_JSON, job(), DlqReason.RETRIES_EXHAUSTED, failed());

            assertThat(result).isFalse();

            // The job stays in `processing` deliberately. Releasing it here would delete the only
            // copy of a job whose dead-letter entry was never written.
            verify(listOps, never()).remove(anyString(), any(Long.class), anyString());
            verify(inFlight, never()).forget(anyString());
        }

        @Test
        @DisplayName("🔴 if the DLQ push throws, the job is NOT released")
        void aThrownRecordMeansNoRelease() {
            when(listOps.leftPush(eq(RedisKeys.DLQ), anyString()))
                    .thenThrow(new RuntimeException("redis down"));

            boolean result = deadLetters.deadLetter(
                    ORIGINAL_JSON, job(), DlqReason.RETRIES_EXHAUSTED, failed());

            assertThat(result).isFalse();
            verify(listOps, never()).remove(anyString(), any(Long.class), anyString());
        }
    }

    @Nested
    @DisplayName("what gets written")
    class TheEntry {

        @Test
        @DisplayName("the entry carries the event id and the reason it was dead-lettered")
        void theEntryCarriesTheReason() {
            when(listOps.leftPush(eq(RedisKeys.DLQ), anyString())).thenReturn(1L);
            when(listOps.remove(eq(RedisKeys.PROCESSING), eq(1L), anyString())).thenReturn(1L);

            deadLetters.deadLetter(ORIGINAL_JSON, job(), DlqReason.RETRIES_EXHAUSTED, failed());

            ArgumentCaptor<String> pushed = ArgumentCaptor.forClass(String.class);
            verify(listOps).leftPush(eq(RedisKeys.DLQ), pushed.capture());

            // The reason is the whole point of the DLQ entry: "gave up after five attempts" and
            // "the subscriber rejected this outright" need completely different responses from
            // whoever reads the queue, and the two are indistinguishable without it.
            assertThat(pushed.getValue())
                    .contains(EVENT_ID)
                    .contains("RETRIES_EXHAUSTED");
        }

        @Test
        @DisplayName("a non-retriable rejection is recorded with its own reason")
        void aPermanentRejectionCarriesItsOwnReason() {
            when(listOps.leftPush(eq(RedisKeys.DLQ), anyString())).thenReturn(1L);
            when(listOps.remove(eq(RedisKeys.PROCESSING), eq(1L), anyString())).thenReturn(1L);

            deadLetters.deadLetter(
                    ORIGINAL_JSON, job(), DlqReason.NON_RETRIABLE_RESPONSE, failed());

            ArgumentCaptor<String> pushed = ArgumentCaptor.forClass(String.class);
            verify(listOps).leftPush(eq(RedisKeys.DLQ), pushed.capture());

            assertThat(pushed.getValue()).contains("NON_RETRIABLE_RESPONSE");
        }

        @Test
        @DisplayName("the release removes the ORIGINAL string, byte for byte")
        void releaseUsesTheOriginalString() {
            when(listOps.leftPush(eq(RedisKeys.DLQ), anyString())).thenReturn(1L);
            when(listOps.remove(eq(RedisKeys.PROCESSING), eq(1L), anyString())).thenReturn(1L);

            deadLetters.deadLetter(ORIGINAL_JSON, job(), DlqReason.RETRIES_EXHAUSTED, failed());

            // LREM matches exact bytes. Re-serializing the job to build the removal would differ
            // by a byte, remove nothing, return 0, and strand a duplicate (9d).
            verify(listOps).remove(RedisKeys.PROCESSING, 1L, ORIGINAL_JSON);
        }
    }

    @Nested
    @DisplayName("the in-flight index is cleared only on a real removal")
    class InFlightBookkeeping {

        @Test
        @DisplayName("a successful release forgets the pickup time")
        void aRealRemovalForgets() {
            when(listOps.leftPush(eq(RedisKeys.DLQ), anyString())).thenReturn(1L);
            when(listOps.remove(eq(RedisKeys.PROCESSING), eq(1L), anyString())).thenReturn(1L);

            deadLetters.deadLetter(ORIGINAL_JSON, job(), DlqReason.RETRIES_EXHAUSTED, failed());

            verify(inFlight).forget(ORIGINAL_JSON);
        }

        @Test
        @DisplayName("🔴 a removal that removed NOTHING does not forget")
        void removingNothingDoesNotForget() {
            when(listOps.leftPush(eq(RedisKeys.DLQ), anyString())).thenReturn(1L);
            // LREM returning 0 is not an error — the call succeeded and did nothing.
            when(listOps.remove(eq(RedisKeys.PROCESSING), eq(1L), anyString())).thenReturn(0L);

            deadLetters.deadLetter(ORIGINAL_JSON, job(), DlqReason.RETRIES_EXHAUSTED, failed());

            // Clearing the pickup time here would erase the very evidence the recovery sweep
            // needs to find a job that is still sitting in `processing`.
            verify(inFlight, never()).forget(anyString());
        }
    }

    @Nested
    @DisplayName("an unparseable message")
    class Unparseable {

        @Test
        @DisplayName("is recorded and released, with the raw text kept")
        void garbageIsRecordedAndReleased() {
            when(listOps.leftPush(eq(RedisKeys.DLQ), anyString())).thenReturn(1L);
            when(listOps.remove(eq(RedisKeys.PROCESSING), eq(1L), anyString())).thenReturn(1L);

            assertThat(deadLetters.deadLetterUnparseable("not json", "bad shape")).isTrue();

            InOrder ordering = inOrder(listOps);
            ordering.verify(listOps).leftPush(eq(RedisKeys.DLQ), anyString());
            ordering.verify(listOps).remove(RedisKeys.PROCESSING, 1L, "not json");
        }

        @Test
        @DisplayName("a failed record also blocks the release")
        void aFailedRecordBlocksReleaseHereToo() {
            when(listOps.leftPush(eq(RedisKeys.DLQ), anyString())).thenReturn(null);

            assertThat(deadLetters.deadLetterUnparseable("not json", "bad shape")).isFalse();
            verify(listOps, never()).remove(anyString(), any(Long.class), anyString());
        }
    }
}
