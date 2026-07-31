package io.github.sidyn4444.webhooks.worker.retry;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * How long to wait before the next attempt.
 *
 * <p>Answers exactly one question — <b>how long</b> — and nothing else. It does not decide
 * whether to retry (that is {@code DeliveryClassifier}, 12a), where the job waits (12c), or who
 * wakes it up (12d). Keeping it to one question makes it a pure function of an integer, which is
 * why it can be checked exhaustively in under a second.
 *
 * <h2>The schedule</h2>
 *
 * <pre>
 *   attempt 1 failed → wait  1s → attempt 2
 *   attempt 2 failed → wait  2s → attempt 3
 *   attempt 3 failed → wait  4s → attempt 4
 *   attempt 4 failed → wait  8s → attempt 5
 *   attempt 5 failed → dead-letter (the ceiling, notes 2e)
 * </pre>
 *
 * <p><b>Why doubling rather than a fixed interval.</b> The two are not equally good and the
 * difference is not subtle. A fixed interval has to be chosen for one scenario and is wrong for
 * every other: short enough to recover quickly from a two-second blip means hammering a
 * subscriber that has been down for ten minutes; long enough to be polite during a real outage
 * means a trivial hiccup delays the event far longer than it needed to be delayed.
 *
 * <p>Doubling adapts to the severity of the outage without knowing anything about it. The first
 * two retries are quick, because most failures are brief — a pod restarting, a deploy, a
 * momentary network blip — and those recover inside a couple of seconds. By the fourth failure
 * the evidence says this is not momentary, so the waits stretch out and stop adding load to
 * something that is clearly struggling.
 *
 * <h2>Why the delay is not exactly the number above</h2>
 *
 * <p>Exponential backoff alone has a failure mode that only appears at scale, and it is the
 * reason jitter is not optional.
 *
 * <p>A subscriber goes down. A thousand queued webhooks fail within the same second. With a pure
 * schedule, all thousand wait <i>exactly</i> one second and retry at <i>exactly</i> the same
 * instant. All thousand fail again, wait exactly two seconds, and retry together again. A smooth
 * stream of traffic has been converted into synchronised waves of a thousand simultaneous
 * requests, aimed at a server that is trying to come back up — and the waves keep landing at the
 * worst possible moments. That is the <b>thundering herd</b>, and it is how a blip becomes an
 * outage.
 *
 * <p><b>Jitter</b> breaks the synchronisation by giving every retry its own small random offset,
 * so the thousand retries spread across a window instead of firing as one spike.
 *
 * <p>The easy misunderstanding is to picture jitter as "add a second to the delay". A uniform
 * shift is useless here — move all thousand retries a second later and they are still perfectly
 * synchronised, just one second further along. <b>The randomness is the mechanism</b>: each
 * retry draws its own offset, and that is what de-synchronises them.
 *
 * <h2>Why this class has two methods</h2>
 *
 * <p>{@link #baseDelay(int)} is deterministic and {@link #nextDelay(int)} applies the randomness.
 * Splitting them is deliberate: a function containing a random number generator cannot be
 * checked against an expected value, so the schedule itself would become untestable if the two
 * were merged. Separated, the schedule is verified exactly and the jitter is verified as a
 * property — every sample lands inside the window, and the samples genuinely differ from each
 * other.
 */
public final class RetryBackoff {

    private RetryBackoff() {
        // Utility class — see DeliveryClassifier for the same reasoning.
    }

    /** The wait after the first failure. Every later delay is this doubled, repeatedly. */
    private static final Duration BASE_DELAY = Duration.ofSeconds(1);

    /**
     * The longest wait this will ever return, regardless of attempt number.
     *
     * <p>With the current ceiling of five attempts the schedule tops out at 16s on its own and
     * this cap is never reached. It exists because the ceiling is configuration, not a constant:
     * raising {@code webhook.retry.max-attempts} to 10 without a cap would silently produce a
     * 512-second wait — over eight minutes — from a change that looks like it only affects how
     * many times something is tried. A cap turns an exponential curve into a plateau, which is
     * the standard shape ("capped exponential backoff") for exactly this reason.
     */
    private static final Duration MAX_DELAY = Duration.ofSeconds(16);

    /**
     * How much either side of the nominal delay a retry may land.
     *
     * <p>0.2 means ±20%: a nominal 4s becomes somewhere in 3.2s–4.8s. Wide enough to break up a
     * synchronised herd, narrow enough that the schedule above is still recognisably the schedule
     * — which matters because "we retry at 1, 2, 4, 8, 16 seconds" should remain a true statement
     * about the system.
     */
    private static final double JITTER_FACTOR = 0.2;

    /**
     * The nominal delay after a given attempt fails, before any jitter.
     *
     * <p>Deterministic: the same attempt number always produces the same answer. This is the
     * schedule as documented, and the thing worth asserting exact values against.
     *
     * @param failedAttemptNumber the attempt that just failed, counting from 1
     * @return {@code 1s} for attempt 1, {@code 2s} for 2, {@code 4s} for 3, …, capped at
     *         {@link #MAX_DELAY}
     * @throws IllegalArgumentException if the attempt number is below 1
     */
    public static Duration baseDelay(int failedAttemptNumber) {
        if (failedAttemptNumber < 1) {
            // Attempt numbers start at 1 (notes 7b). A 0 or a negative here means a caller has
            // an off-by-one, and 2^(0-1) would silently produce half a second rather than an
            // error — a wrong answer that looks plausible is worse than a crash, because
            // nothing ever surfaces it.
            throw new IllegalArgumentException(
                    "failedAttemptNumber must be >= 1, was " + failedAttemptNumber);
        }

        // 1 << n is "multiply by 2, n times" — the doubling, done as a bit shift.
        //
        // The shift is capped at 32 before it is applied, not after. A Java int shift only uses
        // the bottom 5 bits of the shift amount, so 1 << 32 silently wraps around to 1 rather
        // than overflowing loudly: attempt 33 would come back with a 1-second delay and nothing
        // would look wrong. Clamping the exponent first makes that impossible.
        int exponent = Math.min(failedAttemptNumber - 1, 32);
        long multiplier = 1L << exponent;

        Duration delay = BASE_DELAY.multipliedBy(multiplier);

        return delay.compareTo(MAX_DELAY) > 0 ? MAX_DELAY : delay;
    }

    /**
     * The actual delay to use: the nominal schedule with a random ±20% applied.
     *
     * <p>This is what callers use. Two jobs failing on the same attempt at the same instant get
     * different answers from this method, which is the entire point.
     *
     * @param failedAttemptNumber the attempt that just failed, counting from 1
     * @return a delay within ±20% of {@link #baseDelay(int)}, never negative
     */
    public static Duration nextDelay(int failedAttemptNumber) {
        long baseMillis = baseDelay(failedAttemptNumber).toMillis();

        long spread = (long) (baseMillis * JITTER_FACTOR);

        // ThreadLocalRandom rather than Math.random() or a shared Random: several worker threads
        // will call this concurrently, and a shared generator makes them contend on the same
        // internal state — turning a free operation into a synchronisation point on the hot path.
        // ThreadLocalRandom gives each thread its own, with no coordination.
        //
        // nextLong(origin, bound) is exclusive at the top, so +1 keeps the range symmetric.
        long offset = ThreadLocalRandom.current().nextLong(-spread, spread + 1);

        // Math.max guards the arithmetic rather than the logic: with a ±20% factor the result
        // cannot go negative, but the factor is a constant someone may raise later, and a
        // negative delay would mean "already due" — a retry that fires instantly, which is the
        // one behaviour this whole file exists to prevent.
        return Duration.ofMillis(Math.max(0, baseMillis + offset));
    }
}
