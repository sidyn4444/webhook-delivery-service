package io.github.sidyn4444.webhooks.worker.retry;

import io.github.sidyn4444.webhooks.common.model.DeliveryJob;
import io.github.sidyn4444.webhooks.common.model.Event;
import io.github.sidyn4444.webhooks.common.queue.RedisKeys;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.core.script.RedisScript;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * Tests for {@link RetryQueue}.
 *
 * <p><b>Why this class earns unit tests when its neighbours do not.</b> Most of the Redis-facing
 * code in this project is a single call with no decision attached, and a mocked test of that is
 * circular — the method says "call ZADD with these arguments" and the test asserts "ZADD was
 * called with these arguments", which is the source line retyped. It cannot fail.
 *
 * <p>This class is different because {@code schedule} <b>interprets</b> what Redis hands back, and
 * interprets it in a way that is easy to get backwards:
 *
 * <ul>
 *   <li>{@code ZADD} returns <b>true</b> when the member was newly added;
 *   <li>{@code ZADD} returns <b>false</b> when the member already existed and only its
 *       <b>score was updated</b>.
 * </ul>
 *
 * <p><b>Both are success.</b> The obvious implementation — {@code return newlyAdded;} — turns
 * every legitimate reschedule into a reported failure, and because {@code JobPoller} refuses to
 * release a job whose retry could not be scheduled, that wrong reading would strand jobs in
 * {@code processing} while logging errors during entirely healthy operation. Nothing else in the
 * suite guards it.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("RetryQueue")
class RetryQueueTest {

    @Mock private StringRedisTemplate redis;
    @Mock private ZSetOperations<String, String> zSetOps;

    private RetryQueue retryQueue;

    private static final String EVENT_ID = "3f2504e0-4f89-11d3-9a0c-0305e82c3301";

    void setUp() {
        // Called by each test rather than @BeforeEach, because the claimDue tests never touch
        // opsForZSet and strict stubbing would fail them for an unused stub.
        lenient().when(redis.opsForZSet()).thenReturn(zSetOps);
        retryQueue = new RetryQueue(redis, new SimpleMeterRegistry());
    }

    private DeliveryJob job() {
        return DeliveryJob.firstAttempt(new Event(EVENT_ID, "https://8.8.8.8/hook", "payload"), 5);
    }

    @Nested
    @DisplayName("schedule — how it reads what ZADD returns")
    class Schedule {

        @Test
        @DisplayName("a newly added member is a success")
        void newlyAddedIsSuccess() {
            setUp();
            when(zSetOps.add(eq(RedisKeys.RETRY), anyString(), any(Double.class))).thenReturn(true);

            assertThat(retryQueue.schedule(job(), Instant.now().plusSeconds(30))).isTrue();
        }

        @Test
        @DisplayName("🔴 an UPDATED score is ALSO a success — ZADD returning false is not a failure")
        void aScoreUpdateIsAlsoSuccess() {
            setUp();
            // Redis says "that member was already there; I moved its score." That is exactly what
            // happens when the same attempt is rescheduled, and it is a normal, healthy outcome.
            when(zSetOps.add(eq(RedisKeys.RETRY), anyString(), any(Double.class))).thenReturn(false);

            // The naive `return newlyAdded;` returns false here — and JobPoller would then refuse
            // to release the job, stranding it in `processing` and logging an error, on a
            // completely healthy path.
            assertThat(retryQueue.schedule(job(), Instant.now().plusSeconds(30)))
                    .as("a score update means the retry IS scheduled")
                    .isTrue();
        }

        @Test
        @DisplayName("no result at all IS a failure — null is not false")
        void nullIsAFailure() {
            setUp();
            when(zSetOps.add(eq(RedisKeys.RETRY), anyString(), any(Double.class))).thenReturn(null);

            // The three-way distinction is the point: true and false are both "scheduled",
            // null is "Redis did not answer". Collapsing null into false, or false into null,
            // breaks a different half of the contract each way.
            assertThat(retryQueue.schedule(job(), Instant.now().plusSeconds(30))).isFalse();
        }

        @Test
        @DisplayName("a thrown exception is a failure, not a crash")
        void anExceptionIsContained() {
            setUp();
            when(zSetOps.add(eq(RedisKeys.RETRY), anyString(), any(Double.class)))
                    .thenThrow(new RuntimeException("redis down"));

            // It must return false rather than propagate: JobPoller uses the boolean to decide
            // whether to release the job, and an exception escaping here would kill the poll loop.
            assertThat(retryQueue.schedule(job(), Instant.now().plusSeconds(30))).isFalse();
        }

        @Test
        @DisplayName("the score is the due time in epoch millis")
        void theScoreIsTheDueTime() {
            setUp();
            when(zSetOps.add(eq(RedisKeys.RETRY), anyString(), any(Double.class))).thenReturn(true);

            Instant dueAt = Instant.parse("2026-08-04T12:00:00Z");
            retryQueue.schedule(job(), dueAt);

            ArgumentCaptor<Double> score = ArgumentCaptor.forClass(Double.class);
            org.mockito.Mockito.verify(zSetOps).add(eq(RedisKeys.RETRY), anyString(), score.capture());

            // The scheduler pops strictly by score, so the score IS the instruction. A score in
            // seconds rather than millis would make every retry fire ~1000x too early.
            assertThat(score.getValue()).isEqualTo((double) dueAt.toEpochMilli());
        }

        @Test
        @DisplayName("the member is the serialized job, carrying its attempt number")
        void theMemberIsTheSerializedJob() {
            setUp();
            when(zSetOps.add(eq(RedisKeys.RETRY), anyString(), any(Double.class))).thenReturn(true);

            retryQueue.schedule(job().nextAttempt(), Instant.now().plusSeconds(30));

            ArgumentCaptor<String> member = ArgumentCaptor.forClass(String.class);
            org.mockito.Mockito.verify(zSetOps)
                    .add(eq(RedisKeys.RETRY), member.capture(), any(Double.class));

            assertThat(member.getValue())
                    .contains(EVENT_ID)
                    .contains("\"attempt_number\":2");
        }
    }

    @Nested
    @DisplayName("claimDue — what it does when the Lua script misbehaves")
    class ClaimDue {

        @Test
        @DisplayName("a null result becomes an empty list, never a null")
        void nullBecomesEmptyList() {
            retryQueue = new RetryQueue(redis, new SimpleMeterRegistry());
            when(redis.execute(any(RedisScript.class), anyList(), any(), any())).thenReturn(null);

            // The caller loops over this. Returning null would be a NullPointerException inside
            // the scheduler thread, which is the failure mode that kills a background loop
            // silently and leaves a process that looks healthy and consumes nothing.
            assertThat(retryQueue.claimDue(Instant.now(), 100)).isNotNull().isEmpty();
        }

        @Test
        @DisplayName("a thrown exception becomes an empty list — the jobs stay scheduled")
        void anExceptionBecomesEmptyList() {
            retryQueue = new RetryQueue(redis, new SimpleMeterRegistry());
            when(redis.execute(any(RedisScript.class), anyList(), any(), any()))
                    .thenThrow(new RuntimeException("script failed"));

            // Failing soft is correct here and is the opposite of the schedule() case: nothing
            // was promoted, so the jobs are still sitting in the retry set with their due times
            // intact and a later pass picks them up. Nothing is lost by doing nothing.
            assertThat(retryQueue.claimDue(Instant.now(), 100)).isEmpty();
        }

        @Test
        @DisplayName("promoted jobs are returned as they came back")
        void promotedJobsAreReturned() {
            retryQueue = new RetryQueue(redis, new SimpleMeterRegistry());
            when(redis.execute(any(RedisScript.class), anyList(), any(), any()))
                    .thenReturn(List.of("{\"event_id\":\"a\"}", "{\"event_id\":\"b\"}"));

            assertThat(retryQueue.claimDue(Instant.now(), 100)).hasSize(2);
        }
    }

    @Nested
    @DisplayName("size")
    class Size {

        @Test
        @DisplayName("a null count is reported as -1, not as 0")
        void nullCountIsMinusOne() {
            setUp();
            when(zSetOps.zCard(RedisKeys.RETRY)).thenReturn(null);

            // -1 rather than 0 because the two mean different things: 0 is "the retry set is
            // empty", -1 is "we could not find out". A monitoring dashboard showing 0 for
            // "unknown" reports a healthy empty queue during a Redis outage.
            assertThat(retryQueue.size()).isEqualTo(-1);
        }

        @Test
        @DisplayName("a real count is returned as-is")
        void realCountIsReturned() {
            setUp();
            when(zSetOps.zCard(RedisKeys.RETRY)).thenReturn(7L);

            assertThat(retryQueue.size()).isEqualTo(7);
        }
    }
}
