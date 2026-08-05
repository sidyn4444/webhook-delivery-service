package io.github.sidyn4444.webhooks.worker.queue;

import io.github.sidyn4444.webhooks.common.model.DeliveryJob;
import io.github.sidyn4444.webhooks.common.model.DlqReason;
import io.github.sidyn4444.webhooks.common.model.Event;
import io.github.sidyn4444.webhooks.common.queue.JobCodec;
import io.github.sidyn4444.webhooks.common.queue.RedisKeys;
import io.github.sidyn4444.webhooks.worker.delivery.DeliveryResult;
import io.github.sidyn4444.webhooks.worker.delivery.WebhookSender;
import io.github.sidyn4444.webhooks.worker.dlq.DeadLetterQueue;
import io.github.sidyn4444.webhooks.worker.persistence.DeliveryAttempt;
import io.github.sidyn4444.webhooks.worker.persistence.DeliveryAttemptRepository;
import io.github.sidyn4444.webhooks.worker.retry.RetryQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@code JobPoller.handle} — one job driven through the full decision tree.
 *
 * <p><b>The properties under test here are ORDERINGS, not return values.</b> {@code handle}
 * returns {@code void}; every guarantee it makes is a statement about what happened before what:
 *
 * <ul>
 *   <li><b>Record before release</b> (10d) — the Postgres row is written <i>before</i> the job is
 *       removed from {@code processing}. Reversed, a crash in the gap loses all record of a
 *       delivery that definitely happened, and the job is gone too. In the order it is written,
 *       the same crash costs one duplicate, which the subscriber dedupes on a stable event id.
 *   <li><b>Schedule before removing</b> (12e, 13b) — the retry is placed on the retry set
 *       <i>before</i> the job leaves {@code processing}, so the job is never in zero places.
 * </ul>
 *
 * <p>No value returned by any method expresses either of those, which is why this file uses
 * {@link InOrder} rather than assertions on results.
 *
 * <p>Every collaborator is a mock: no Redis, no Postgres, no HTTP, no polling thread. That last
 * one matters — driving the real loop would mean starting a thread and waiting on it, and the
 * loop catches every exception and then sleeps, so a genuine failure would present as a slow
 * test rather than a failing one.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("JobPoller.handle")
class JobPollerTest {

    @Mock private StringRedisTemplate redis;
    @Mock private ListOperations<String, String> listOps;
    @Mock private WebhookSender sender;
    @Mock private DeliveryAttemptRepository attempts;
    @Mock private RetryQueue retryQueue;
    @Mock private DeadLetterQueue deadLetters;
    @Mock private InFlightIndex inFlight;

    private JobPoller poller;

    private static final String EVENT_ID = "3f2504e0-4f89-11d3-9a0c-0305e82c3301";
    private static final String URL = "https://8.8.8.8/webhooks";

    @BeforeEach
    void setUp() {
        // redis.opsForList() returns the object the real calls are made on, so it has to be
        // stubbed or every call below would hit a null. lenient() because a few tests never
        // reach Redis at all, and strict stubbing would fail them for not using this.
        lenient().when(redis.opsForList()).thenReturn(listOps);

        poller = new JobPoller(redis, sender, attempts, retryQueue, deadLetters, inFlight);
    }

    private DeliveryJob job(int attempt, int maxRetries) {
        DeliveryJob j = DeliveryJob.firstAttempt(new Event(EVENT_ID, URL, "payload"), maxRetries);
        for (int i = 1; i < attempt; i++) {
            j = j.nextAttempt();
        }
        return j;
    }

    /**
     * The repository must be stubbed to return a real object, not left to Mockito's default.
     * An unstubbed mock returns {@code null}, and {@code JobPoller.record} calls
     * {@code saved.getId()} on the result — so the default would throw an NPE, which the
     * production code catches and treats as "the write failed", silently sending every success
     * test down the failure branch.
     */
    private void repositorySavesSuccessfully(DeliveryJob j, DeliveryResult result) {
        when(attempts.save(any(DeliveryAttempt.class)))
                .thenReturn(DeliveryAttempt.of(j, result));
    }

    private void ackRemovesOneEntry() {
        when(listOps.remove(eq(RedisKeys.PROCESSING), eq(1L), anyString())).thenReturn(1L);
    }

    // =========================================================================================
    // 1. SUCCESS — the record-before-release ordering
    // =========================================================================================

    @Nested
    @DisplayName("a 2xx response")
    class Delivered {

        @Test
        @DisplayName("🔴 writes the Postgres row BEFORE removing the job from processing")
        void recordsBeforeAcking() {
            DeliveryJob j = job(1, 5);
            String json = JobCodec.toJson(j);
            DeliveryResult ok = DeliveryResult.responded(200, 12);

            when(sender.send(any(DeliveryJob.class))).thenReturn(ok);
            repositorySavesSuccessfully(j, ok);
            ackRemovesOneEntry();

            poller.handle(json);

            // THE assertion of this file. An InOrder verifier checks the calls happened in the
            // order they are listed here — not merely that both happened. Two separate verify()
            // calls would pass against a JobPoller that acked first and recorded second, which
            // is precisely the bug this ordering exists to prevent.
            InOrder ordering = inOrder(attempts, listOps);
            ordering.verify(attempts).save(any(DeliveryAttempt.class));
            ordering.verify(listOps).remove(RedisKeys.PROCESSING, 1L, json);
        }

        @Test
        @DisplayName("the ack removes the ORIGINAL string, byte for byte")
        void ackUsesTheExactStringItWasGiven() {
            DeliveryJob j = job(1, 5);
            String json = JobCodec.toJson(j);
            DeliveryResult ok = DeliveryResult.responded(204, 8);

            when(sender.send(any(DeliveryJob.class))).thenReturn(ok);
            repositorySavesSuccessfully(j, ok);
            ackRemovesOneEntry();

            poller.handle(json);

            ArgumentCaptor<String> removed = ArgumentCaptor.forClass(String.class);
            verify(listOps).remove(eq(RedisKeys.PROCESSING), eq(1L), removed.capture());

            // LREM matches on exact bytes. Re-serializing the job to build the ack — rather than
            // reusing the string that came off the queue — can differ by a byte, remove nothing,
            // and return 0 without any error (9d).
            assertThat(removed.getValue()).isEqualTo(json);
        }

        @Test
        @DisplayName("a delivered job is never retried and never dead-lettered")
        void successTouchesNeitherRetryNorDlq() {
            DeliveryJob j = job(1, 5);
            DeliveryResult ok = DeliveryResult.responded(201, 30);

            when(sender.send(any(DeliveryJob.class))).thenReturn(ok);
            repositorySavesSuccessfully(j, ok);
            ackRemovesOneEntry();

            poller.handle(JobCodec.toJson(j));

            verify(retryQueue, never()).schedule(any(), any());
            verify(deadLetters, never()).deadLetter(any(), any(), any(), any());
        }

        @ParameterizedTest(name = "{0} is a success and is acked")
        @ValueSource(ints = {200, 201, 202, 204, 299})
        @DisplayName("any 2xx is treated as delivered, not just 200")
        void anySuccessIsAcked(int status) {
            DeliveryJob j = job(1, 5);
            DeliveryResult ok = DeliveryResult.responded(status, 10);

            when(sender.send(any(DeliveryJob.class))).thenReturn(ok);
            repositorySavesSuccessfully(j, ok);
            ackRemovesOneEntry();

            poller.handle(JobCodec.toJson(j));

            verify(listOps).remove(eq(RedisKeys.PROCESSING), eq(1L), anyString());
        }
    }

    // =========================================================================================
    // 2. THE POSTGRES WRITE FAILS — the whole reason the ordering is what it is
    // =========================================================================================

    @Nested
    @DisplayName("the delivery succeeded but the Postgres write failed")
    class RecordFailed {

        @Test
        @DisplayName("🔴 does NOT ack — the job stays parked and will be re-delivered")
        void aFailedRecordMeansNoAck() {
            DeliveryJob j = job(1, 5);

            when(sender.send(any(DeliveryJob.class))).thenReturn(DeliveryResult.responded(200, 12));
            when(attempts.save(any(DeliveryAttempt.class)))
                    .thenThrow(new RuntimeException("connection refused"));

            poller.handle(JobCodec.toJson(j));

            // The delivery genuinely happened — the subscriber has the webhook. We simply have no
            // record of it, so the job is deliberately left in `processing` for the recovery
            // sweep to reclaim. The cost is one duplicate delivery; the alternative is losing all
            // evidence of a delivery that occurred, which is unrecoverable (10d).
            verify(listOps, never()).remove(anyString(), any(Long.class), anyString());
            verify(inFlight, never()).forget(anyString());
        }

        @Test
        @DisplayName("a repository failure does not propagate — the poll loop must survive it")
        void aRepositoryFailureIsContained() {
            DeliveryJob j = job(1, 5);

            when(sender.send(any(DeliveryJob.class))).thenReturn(DeliveryResult.responded(200, 12));
            when(attempts.save(any(DeliveryAttempt.class)))
                    .thenThrow(new RuntimeException("connection refused"));

            // No assertThatThrownBy here — the property IS that nothing escapes. A thrown
            // exception would kill the polling thread, leaving a process that looks healthy and
            // consumes nothing forever (9b).
            poller.handle(JobCodec.toJson(j));
        }
    }

    // =========================================================================================
    // 3. PERMANENT FAILURE — straight to the DLQ, no retry
    // =========================================================================================

    @Nested
    @DisplayName("a non-retriable response")
    class PermanentFailure {

        @ParameterizedTest(name = "{0} is permanent — dead-lettered, never retried")
        @ValueSource(ints = {400, 401, 403, 404, 422, 499})
        void clientErrorsGoStraightToTheDlq(int status) {
            DeliveryJob j = job(1, 5);
            String json = JobCodec.toJson(j);
            DeliveryResult rejected = DeliveryResult.responded(status, 15);

            when(sender.send(any(DeliveryJob.class))).thenReturn(rejected);
            repositorySavesSuccessfully(j, rejected);

            poller.handle(json);

            verify(deadLetters).deadLetter(eq(json), any(DeliveryJob.class),
                    eq(DlqReason.NON_RETRIABLE_RESPONSE), any(DeliveryResult.class));

            // The retry budget is irrelevant here: this job has 4 attempts left and still must
            // not be retried, because retrying a 404 just produces five identical 404s.
            verify(retryQueue, never()).schedule(any(), any());
        }

        @Test
        @DisplayName("the row is still written — a permanent failure is an attempt worth recording")
        void aPermanentFailureIsStillRecorded() {
            DeliveryJob j = job(1, 5);
            DeliveryResult rejected = DeliveryResult.responded(404, 15);

            when(sender.send(any(DeliveryJob.class))).thenReturn(rejected);
            repositorySavesSuccessfully(j, rejected);

            poller.handle(JobCodec.toJson(j));

            verify(attempts).save(any(DeliveryAttempt.class));
        }
    }

    // =========================================================================================
    // 4. RETRIABLE FAILURE — the schedule-before-removing ordering
    // =========================================================================================

    @Nested
    @DisplayName("a retriable failure with attempts remaining")
    class Retriable {

        @Test
        @DisplayName("🔴 schedules the retry BEFORE removing it from processing")
        void schedulesBeforeRemoving() {
            DeliveryJob j = job(1, 5);
            String json = JobCodec.toJson(j);
            DeliveryResult failed = DeliveryResult.responded(503, 40);

            when(sender.send(any(DeliveryJob.class))).thenReturn(failed);
            repositorySavesSuccessfully(j, failed);
            when(retryQueue.schedule(any(DeliveryJob.class), any(Instant.class))).thenReturn(true);
            ackRemovesOneEntry();

            poller.handle(json);

            // Reversed, a crash between the two leaves the job in neither place: removed from
            // processing, never added to retry. It exists nowhere and nothing sweeps it up.
            InOrder ordering = inOrder(retryQueue, listOps);
            ordering.verify(retryQueue).schedule(any(DeliveryJob.class), any(Instant.class));
            ordering.verify(listOps).remove(RedisKeys.PROCESSING, 1L, json);
        }

        @Test
        @DisplayName("the scheduled job carries an INCREMENTED attempt number")
        void theRetryCarriesTheNextAttemptNumber() {
            DeliveryJob j = job(2, 5);
            DeliveryResult failed = DeliveryResult.failed(10_011, "timeout");

            when(sender.send(any(DeliveryJob.class))).thenReturn(failed);
            repositorySavesSuccessfully(j, failed);
            when(retryQueue.schedule(any(DeliveryJob.class), any(Instant.class))).thenReturn(true);
            ackRemovesOneEntry();

            poller.handle(JobCodec.toJson(j));

            ArgumentCaptor<DeliveryJob> scheduled = ArgumentCaptor.forClass(DeliveryJob.class);
            verify(retryQueue).schedule(scheduled.capture(), any(Instant.class));

            // Without this, a JobPoller that re-scheduled the SAME job would retry forever: the
            // attempt number would never reach the ceiling and the budget would never run out.
            assertThat(scheduled.getValue().attemptNumber()).isEqualTo(3);
            assertThat(scheduled.getValue().eventId()).isEqualTo(EVENT_ID);
        }

        @Test
        @DisplayName("the due time is in the future, inside the backoff window for this attempt")
        void theDueTimeIsWithinTheBackoffWindow() {
            DeliveryJob j = job(1, 5);
            DeliveryResult failed = DeliveryResult.responded(500, 20);

            when(sender.send(any(DeliveryJob.class))).thenReturn(failed);
            repositorySavesSuccessfully(j, failed);
            when(retryQueue.schedule(any(DeliveryJob.class), any(Instant.class))).thenReturn(true);
            ackRemovesOneEntry();

            Instant before = Instant.now();
            poller.handle(JobCodec.toJson(j));

            ArgumentCaptor<Instant> dueAt = ArgumentCaptor.forClass(Instant.class);
            verify(retryQueue).schedule(any(DeliveryJob.class), dueAt.capture());

            // A RANGE, not a value. handleFailure reads Instant.now() internally and
            // RetryBackoff.nextDelay adds ±20% jitter, so the exact instant is unknowable from
            // outside. Asserting a window is the honest form; a Clock injected into JobPoller is
            // what would make this exact, and it is deliberately out of scope.
            assertThat(dueAt.getValue())
                    .as("attempt 1 backs off ~1s, so the due time must be ahead of now")
                    .isAfter(before)
                    .isBefore(before.plus(Duration.ofSeconds(5)));
        }

        @Test
        @DisplayName("🔴 if scheduling FAILS, the job is NOT removed — it stays for the sweep")
        void aFailedScheduleLeavesTheJobParked() {
            DeliveryJob j = job(1, 5);
            DeliveryResult failed = DeliveryResult.responded(500, 20);

            when(sender.send(any(DeliveryJob.class))).thenReturn(failed);
            repositorySavesSuccessfully(j, failed);
            when(retryQueue.schedule(any(DeliveryJob.class), any(Instant.class))).thenReturn(false);

            poller.handle(JobCodec.toJson(j));

            // Removing it here would delete the only copy of a job that was never scheduled.
            // Leaving it parked costs a duplicate; removing it loses the event.
            verify(listOps, never()).remove(anyString(), any(Long.class), anyString());
            verify(deadLetters, never()).deadLetter(any(), any(), any(), any());
        }
    }

    // =========================================================================================
    // 5. RETRY BUDGET EXHAUSTED
    // =========================================================================================

    @Nested
    @DisplayName("a retriable failure with no attempts left")
    class RetriesExhausted {

        @Test
        @DisplayName("dead-lettered as RETRIES_EXHAUSTED, not scheduled again")
        void theBudgetRunsOut() {
            // maxRetries=5 means the first attempt PLUS five retries, because hasRetriesLeft() is
            // `attemptNumber <= maxRetries`. So attempt 6 is the first one with no budget left.
            DeliveryJob j = job(6, 5);
            String json = JobCodec.toJson(j);
            DeliveryResult failed = DeliveryResult.responded(503, 40);

            when(sender.send(any(DeliveryJob.class))).thenReturn(failed);
            repositorySavesSuccessfully(j, failed);

            poller.handle(json);

            verify(deadLetters).deadLetter(eq(json), any(DeliveryJob.class),
                    eq(DlqReason.RETRIES_EXHAUSTED), any(DeliveryResult.class));
            verify(retryQueue, never()).schedule(any(), any());
        }

        @Test
        @DisplayName("🔴 the boundary, lower side: attempt 5 of 5 STILL retries")
        void attemptFiveOfFiveStillRetries() {
            // One value either side of the line, and this is the side that is easy to get wrong.
            // Writing only the exhausted case would leave `<` and `<=` indistinguishable — the
            // exact off-by-one hasRetriesLeft() exists to centralise.
            DeliveryJob stillHasBudget = job(5, 5);
            DeliveryResult failed = DeliveryResult.responded(503, 40);

            when(sender.send(any(DeliveryJob.class))).thenReturn(failed);
            when(attempts.save(any(DeliveryAttempt.class)))
                    .thenReturn(DeliveryAttempt.of(stillHasBudget, failed));
            when(retryQueue.schedule(any(DeliveryJob.class), any(Instant.class))).thenReturn(true);
            when(listOps.remove(eq(RedisKeys.PROCESSING), eq(1L), anyString())).thenReturn(1L);

            poller.handle(JobCodec.toJson(stillHasBudget));

            verify(retryQueue, times(1)).schedule(any(), any());
            verify(deadLetters, never()).deadLetter(any(), any(), any(), any());
        }
    }

    // =========================================================================================
    // 6. POISON PILL — a message that is not a job at all
    // =========================================================================================

    @Nested
    @DisplayName("a message that cannot be parsed")
    class PoisonPill {

        @Test
        @DisplayName("dead-lettered as unparseable, and nothing is ever sent")
        void garbageIsParkedNotDelivered() {
            poller.handle("this is not json at all");

            verify(deadLetters).deadLetterUnparseable(eq("this is not json at all"), anyString());

            // The most important assertion in this test: an unparseable message must never reach
            // the sender. There is no URL to send it to, and attempting it would be a crash
            // inside the loop rather than a parked message.
            verify(sender, never()).send(any());
            verify(attempts, never()).save(any());
            verify(retryQueue, never()).schedule(any(), any());
        }

        @Test
        @DisplayName("well-formed JSON that is not a job is also parked")
        void wrongShapeJsonIsAlsoParked() {
            poller.handle("{\"something\":\"else\"}");

            verify(deadLetters).deadLetterUnparseable(anyString(), anyString());
            verify(sender, never()).send(any());
        }
    }

    // =========================================================================================
    // 7. THE IN-FLIGHT INDEX — the bookkeeping that lets the sweep find abandoned jobs
    // =========================================================================================

    @Nested
    @DisplayName("the in-flight index")
    class InFlightBookkeeping {

        @Test
        @DisplayName("a successful ack forgets the job")
        void ackForgetsTheJob() {
            DeliveryJob j = job(1, 5);
            String json = JobCodec.toJson(j);
            DeliveryResult ok = DeliveryResult.responded(200, 12);

            when(sender.send(any(DeliveryJob.class))).thenReturn(ok);
            repositorySavesSuccessfully(j, ok);
            ackRemovesOneEntry();

            poller.handle(json);

            verify(inFlight).forget(json);
        }

        @Test
        @DisplayName("🔴 an ack that removes NOTHING does not forget the job")
        void aFailedAckKeepsTheInFlightEntry() {
            DeliveryJob j = job(1, 5);
            String json = JobCodec.toJson(j);
            DeliveryResult ok = DeliveryResult.responded(200, 12);

            when(sender.send(any(DeliveryJob.class))).thenReturn(ok);
            repositorySavesSuccessfully(j, ok);
            // LREM returning 0 is NOT an error — the call succeeds and does nothing. An
            // unchecked ack fails invisibly (9d).
            when(listOps.remove(eq(RedisKeys.PROCESSING), eq(1L), anyString())).thenReturn(0L);

            poller.handle(json);

            verify(inFlight, never()).forget(anyString());
        }
    }
}
