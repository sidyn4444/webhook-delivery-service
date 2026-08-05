package io.github.sidyn4444.webhooks.worker.queue;

import io.github.sidyn4444.webhooks.common.queue.RedisKeys;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.core.script.RedisScript;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link InFlightIndex} — the sorted set that records <b>when</b> each job was picked up.
 *
 * <p>It is the companion to the {@code webhooks:processing} list: the list holds the job, and this
 * holds the clock. A job whose pickup time is older than the staleness window cannot still be in
 * flight, because a delivery is capped at ten seconds — so that is how a crashed worker's
 * abandoned jobs are identified.
 *
 * <p><b>Why this class earns tests.</b> Every method here interprets a Redis return value where
 * the obvious reading is wrong:
 *
 * <ul>
 *   <li>{@code record} — {@code ZADD} returns <b>false</b> when the member already existed and
 *       only its score changed. That is the <i>normal</i> case here: the sweep adopts a job with a
 *       placeholder timestamp, and the real worker then overwrites it. Treating false as a
 *       failure would report errors during healthy operation.
 *   <li>{@code forget} — {@code null} and {@code 0} mean different things and neither is an error.
 *   <li>{@code sweep} — a Lua script's result has to be shape-checked before it is indexed into.
 * </ul>
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("InFlightIndex")
class InFlightIndexTest {

    @Mock private StringRedisTemplate redis;
    @Mock private ZSetOperations<String, String> zSetOps;

    private static final String JSON = "{\"event_id\":\"3f2504e0-4f89-11d3-9a0c-0305e82c3301\"}";

    private InFlightIndex withZSet() {
        lenient().when(redis.opsForZSet()).thenReturn(zSetOps);
        return new InFlightIndex(redis);
    }

    @Nested
    @DisplayName("record — writing the pickup time")
    class Record {

        @Test
        @DisplayName("a newly added entry is a success")
        void newEntryIsSuccess() {
            InFlightIndex index = withZSet();
            when(zSetOps.add(eq(RedisKeys.INFLIGHT), anyString(), any(Double.class)))
                    .thenReturn(true);

            assertThat(index.record(JSON)).isTrue();
        }

        @Test
        @DisplayName("🔴 an UPDATED score is also a success — and it is the normal case")
        void anUpdatedScoreIsAlsoSuccess() {
            InFlightIndex index = withZSet();
            // false = "that member was already in the set; I moved its score." This is exactly
            // what happens when the recovery sweep has already adopted a job with a placeholder
            // timestamp and the worker holding it now writes the real one.
            when(zSetOps.add(eq(RedisKeys.INFLIGHT), anyString(), any(Double.class)))
                    .thenReturn(false);

            // The naive `return added;` returns false here and logs an error on a healthy path.
            assertThat(index.record(JSON))
                    .as("a score update means the pickup time IS recorded")
                    .isTrue();
        }

        @Test
        @DisplayName("no result at all is a failure — null is not false")
        void nullIsAFailure() {
            InFlightIndex index = withZSet();
            when(zSetOps.add(eq(RedisKeys.INFLIGHT), anyString(), any(Double.class)))
                    .thenReturn(null);

            assertThat(index.record(JSON)).isFalse();
        }

        @Test
        @DisplayName("a thrown exception is contained and reported as a failure")
        void anExceptionIsContained() {
            InFlightIndex index = withZSet();
            when(zSetOps.add(eq(RedisKeys.INFLIGHT), anyString(), any(Double.class)))
                    .thenThrow(new RuntimeException("redis down"));

            // Failing soft is right: the job is still in `processing`, so the sweep will adopt it.
            // Losing the timestamp costs a delayed reclaim, not a lost job.
            assertThat(index.record(JSON)).isFalse();
        }
    }

    @Nested
    @DisplayName("forget — clearing the pickup time")
    class Forget {

        @Test
        @DisplayName("removing nothing is not an error and does not throw")
        void removingNothingIsFine() {
            InFlightIndex index = withZSet();
            when(zSetOps.remove(RedisKeys.INFLIGHT, JSON)).thenReturn(0L);

            // 0 means there was no entry — which happens legitimately, e.g. when the entry was
            // never written because record() failed. It is a debug line, not a failure.
            index.forget(JSON);
        }

        @Test
        @DisplayName("a null result does not throw")
        void nullResultIsHandled() {
            InFlightIndex index = withZSet();
            when(zSetOps.remove(RedisKeys.INFLIGHT, JSON)).thenReturn(null);

            index.forget(JSON);
        }

        @Test
        @DisplayName("🔴 a thrown exception does not escape — a stale entry is not worth a crash")
        void anExceptionDoesNotEscape() {
            InFlightIndex index = withZSet();
            when(zSetOps.remove(RedisKeys.INFLIGHT, JSON))
                    .thenThrow(new RuntimeException("redis down"));

            // No assertion, deliberately: the property IS that nothing propagates. forget() is
            // called from the ack path, and an exception here would abort an ack that had already
            // succeeded — turning a leftover index entry into a re-delivered webhook.
            index.forget(JSON);
        }
    }

    @Nested
    @DisplayName("sweep — reading what the Lua script returned")
    @SuppressWarnings("unchecked")
    class Sweep {

        @Test
        @DisplayName("a null result becomes an empty sweep, not a crash")
        void nullResultIsEmpty() {
            InFlightIndex index = new InFlightIndex(redis);
            when(redis.execute(any(RedisScript.class), anyList(), any(), any(), any()))
                    .thenReturn(null);

            assertThat(index.sweep(Instant.now(), Duration.ofSeconds(60), 100).isEmpty()).isTrue();
        }

        @Test
        @DisplayName("🔴 a result with too few elements becomes an empty sweep")
        void aShortResultIsEmpty() {
            InFlightIndex index = new InFlightIndex(redis);
            // The script is contracted to return three values. Two means something changed and
            // the assumption no longer holds — indexing into it would be an
            // IndexOutOfBoundsException inside the sweep thread, which kills a background loop
            // silently. The shape is checked before it is trusted.
            when(redis.execute(any(RedisScript.class), anyList(), any(), any(), any()))
                    .thenReturn(List.of(1L, 2L));

            assertThat(index.sweep(Instant.now(), Duration.ofSeconds(60), 100).isEmpty()).isTrue();
        }

        @Test
        @DisplayName("a well-formed result is parsed into adopted / pruned / reclaimed")
        void aGoodResultIsParsed() {
            InFlightIndex index = new InFlightIndex(redis);
            when(redis.execute(any(RedisScript.class), anyList(), any(), any(), any()))
                    .thenReturn(List.of(2L, 1L, List.of("job-a", "job-b", "job-c")));

            InFlightIndex.SweepResult result =
                    index.sweep(Instant.now(), Duration.ofSeconds(60), 100);

            // Three distinct counts that are easy to transpose. Adopted and pruned are
            // bookkeeping; reclaimed is the list of jobs actually put back on the queue, and
            // reporting one in place of another would misreport how much work was recovered.
            assertThat(result.adopted()).isEqualTo(2);
            assertThat(result.pruned()).isEqualTo(1);
            assertThat(result.reclaimed()).containsExactly("job-a", "job-b", "job-c");
            assertThat(result.isEmpty()).isFalse();
        }

        @Test
        @DisplayName("a thrown exception becomes an empty sweep — the jobs keep their timestamps")
        void anExceptionIsEmpty() {
            InFlightIndex index = new InFlightIndex(redis);
            when(redis.execute(any(RedisScript.class), anyList(), any(), any(), any()))
                    .thenThrow(new RuntimeException("script failed"));

            // Failing soft is correct: nothing was reclaimed, the parked jobs still carry their
            // pickup times, and a later pass finds them. Doing nothing loses nothing.
            assertThat(index.sweep(Instant.now(), Duration.ofSeconds(60), 100).isEmpty()).isTrue();
        }
    }

    @Nested
    @DisplayName("SweepResult.isEmpty")
    class EmptinessRule {

        @Test
        @DisplayName("only all-zero-and-nothing-reclaimed counts as empty")
        void emptinessNeedsAllThree() {
            assertThat(new InFlightIndex.SweepResult(0, 0, List.of()).isEmpty()).isTrue();

            // One value either side of the line, three times — a sweep that adopted a job did
            // work, and reporting it as empty would suppress the log line that says so.
            assertThat(new InFlightIndex.SweepResult(1, 0, List.of()).isEmpty()).isFalse();
            assertThat(new InFlightIndex.SweepResult(0, 1, List.of()).isEmpty()).isFalse();
            assertThat(new InFlightIndex.SweepResult(0, 0, List.of("a")).isEmpty()).isFalse();
        }
    }
}
