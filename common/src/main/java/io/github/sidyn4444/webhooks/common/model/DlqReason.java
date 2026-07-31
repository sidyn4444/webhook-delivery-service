package io.github.sidyn4444.webhooks.common.model;

/**
 * Why a job stopped being retried.
 *
 * <p>This is the field that decides what a human does next, which is why it is an enum rather
 * than a free-text note. A dead-letter queue full of sentences cannot be counted, grouped or
 * alerted on — "how many events did we give up on because the subscriber's URL is wrong?" has
 * to be answerable without reading anything.
 *
 * <p>There are exactly three ways a job reaches the dead-letter queue, and they call for
 * different responses:
 */
public enum DlqReason {

    /**
     * The subscriber answered, and the answer was not one waiting would change — a 404, a 400,
     * a 401 (notes 2e's <i>immediate</i> road).
     *
     * <p><b>What it means for whoever finds it:</b> something is misconfigured, and it is
     * probably on their side. A wrong URL, a revoked credential, a body shape they no longer
     * accept. Replaying without fixing anything will fail identically.
     *
     * <p>Dead-lettered on the FIRST failure, deliberately: retrying a deterministic rejection
     * five times wastes attempts and delays this signal by half a minute.
     */
    NON_RETRIABLE_RESPONSE,

    /**
     * The failure was transient every time, and the job ran out of attempts (notes 2e's
     * <i>exhausted</i> road — the one people forget when describing retry logic).
     *
     * <p><b>What it means for whoever finds it:</b> the subscriber was genuinely down for
     * longer than the retry window, which is roughly half a minute. Nothing is misconfigured.
     * Replaying it later is likely to work, and if a lot of these appear together they are a
     * single outage rather than a lot of separate problems.
     */
    RETRIES_EXHAUSTED,

    /**
     * The message on the queue could not be turned back into a job at all.
     *
     * <p><b>What it means for whoever finds it:</b> a bug, not a delivery problem. Something
     * wrote a malformed message, or a schema changed incompatibly, or a person put something
     * there by hand while debugging. The entry is evidence and should be read, not replayed.
     *
     * <p>This one is only mandatory because the recovery sweep exists (14b). Before it, an
     * unparseable message could sit harmlessly in the processing list forever. Once something
     * scans that list and re-queues what it finds, a message nobody can parse becomes an
     * infinite loop — picked up, unparseable, parked, re-queued, forever. The dead-letter queue
     * is what breaks it.
     */
    UNPARSEABLE
}
