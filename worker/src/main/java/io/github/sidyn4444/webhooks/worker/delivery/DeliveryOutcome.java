package io.github.sidyn4444.webhooks.worker.delivery;

/**
 * What the worker has DECIDED about a delivery attempt.
 *
 * <p>This is deliberately a different type from {@link DeliveryResult}, and the split is the
 * point of this file.
 *
 * <ul>
 *   <li>{@link DeliveryResult} is <b>what the world did to us</b> — a status code, a duration, a
 *       transport failure. Facts. They never change: a 503 is a 503 forever, and the sender
 *       produces them without deciding anything (notes 9c).</li>
 *   <li>{@code DeliveryOutcome} is <b>what we have decided to do about it</b> — policy. Whether
 *       429 is worth retrying, whether 408 counts as transient, whether an unknown status should
 *       be given the benefit of the doubt. Every one of those is a judgement call that can be
 *       revisited without a single fact changing.</li>
 * </ul>
 *
 * <p><b>Why not just put a {@code isRetriable()} method on DeliveryResult?</b> Because that type
 * is also what the delivery-log row is built from (notes 10b) and what Week 6's metrics read.
 * Folding policy into it means that changing the retry rules edits the type the database write
 * and the dashboards both depend on. Keeping facts and decisions in separate types means the
 * retry policy can be argued about, changed and tested on its own.
 *
 * <p><b>Why an enum rather than a boolean.</b> A boolean {@code retriable} cannot express three
 * outcomes, and the third one is not optional: a successful delivery is neither retriable nor
 * permanently failed. Encoding it as {@code retriable == false} would put a 200 and a 404 in the
 * same bucket, which is exactly the collapse this type exists to prevent.
 *
 * <p><b>What this enum deliberately does NOT know.</b> There is no {@code shouldRetry()} here,
 * because that question cannot be answered from the outcome alone — a {@link #RETRIABLE} attempt
 * still goes to the dead-letter queue once the attempt ceiling is reached (notes 2e). Mixing the
 * attempt count in would make this type depend on the job as well as the response, and it would
 * stop being a plain classification. That decision lives in the poll loop (12e) and the
 * dead-letter routing (13b).
 */
public enum DeliveryOutcome {

    /**
     * The subscriber accepted the webhook. Any 2xx.
     *
     * <p>Nothing further is owed: record the attempt, acknowledge the job, done.
     */
    DELIVERED,

    /**
     * The attempt failed, but <b>the request itself was fine and the world was wrong.</b>
     *
     * <p>The subscriber was overloaded, restarting, rate-limiting us, or never answered at all.
     * The identical request, sent later, has a genuine chance of succeeding — so waiting is
     * useful rather than wishful. Goes to the retry schedule (12c), or to the dead-letter queue
     * if the attempt ceiling has been reached (notes 2e's "exhausted" road).
     */
    RETRIABLE,

    /**
     * The attempt failed because <b>the world was fine and the request was wrong.</b>
     *
     * <p>A malformed body, a rejected credential, a URL that does not exist. Sending it again
     * produces the identical rejection, deterministically, so retrying is not merely useless —
     * it is actively harmful. It burns five attempts, delays the dead-letter entry by half a
     * minute, and delays the moment a human discovers the misconfiguration behind it.
     *
     * <p>Goes straight to the dead-letter queue on the first failure (notes 2e's "immediate"
     * road) — the road people forget when they describe retry logic in interviews.
     */
    PERMANENT
}
