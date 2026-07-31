package io.github.sidyn4444.webhooks.common.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Objects;

/**
 * One entry in the dead-letter queue: a delivery the system has stopped trying to make.
 *
 * <p>The design question here is not what a dead letter <i>is</i> — it is what a dead letter has
 * to <b>carry</b>. A queue of jobs with no context is a list of failures nobody can act on, and
 * a dead-letter queue nobody acts on is just a slower way of losing data.
 *
 * <p>So each field is here because it answers a question the person who opens this at 9am is
 * going to ask:
 *
 * <ul>
 *   <li><b>What was this?</b> — {@code eventId}, {@code subscriberUrl}</li>
 *   <li><b>Why did we stop?</b> — {@code reason}</li>
 *   <li><b>What happened last?</b> — {@code lastStatusCode}, {@code lastFailureReason}</li>
 *   <li><b>How hard did we try?</b> — {@code attemptsMade}</li>
 *   <li><b>When?</b> — {@code deadLetteredAt}</li>
 *   <li><b>Can I put it back?</b> — {@code originalJob}</li>
 * </ul>
 *
 * <h2>🔴 Why this holds the payload when the delivery log deliberately does not</h2>
 *
 * <p>{@code DeliveryAttempt} has no payload column, on the grounds that it does not need one:
 * "event X returned 500 in 42ms" is a complete audit record, and a permanent queryable table is
 * a high-exposure place to keep customer data (notes 10b).
 *
 * <p>That reasoning gives the opposite answer here, from the same rule. The rule is <b>need
 * versus exposure, per store</b> — and this store has a genuine need the log does not: <b>replay
 * is impossible without the original message.</b> Postgres has no payload by design, so if the
 * dead-letter queue does not carry one either, a dead-lettered event cannot be recovered by
 * anybody, ever. Keeping the evidence is the entire point of having a DLQ rather than dropping
 * the job.
 *
 * <p>It is stored as {@code originalJob} — the exact string that was on the queue — rather than
 * as parsed fields, for two reasons. Replaying means pushing those exact bytes back, so no
 * re-serialisation can introduce a difference. And for an {@link DlqReason#UNPARSEABLE} entry
 * there is nothing to parse, so the raw string is the only thing that exists.
 *
 * <p><b>The cost has to be named rather than waved at:</b> this is now the second store holding
 * customer payloads, and unlike the queue — which holds them for milliseconds — a dead letter
 * sits until somebody drains it. The controls that make that acceptable are the ones the
 * data-minimization rule prescribes for a store that genuinely needs the content: keep it
 * access-controlled, keep it small by actually draining it, and alert on its size so it never
 * becomes a quiet archive. <b>A dead-letter queue nobody looks at is a data-retention problem
 * wearing a reliability costume.</b>
 */
public record DlqEntry(

        /*
         * The event this was. Null only for an UNPARSEABLE entry, where by definition nothing
         * could be read out of the message.
         */
        @JsonProperty("event_id")
        String eventId,

        /*
         * Where it was going. Also null for an UNPARSEABLE entry.
         *
         * Present because it is the fastest way to spot the common pattern: twenty dead letters
         * that turn out to be one broken subscriber rather than twenty separate problems.
         */
        @JsonProperty("subscriber_url")
        String subscriberUrl,

        /*
         * How many deliveries were attempted. 0 for an UNPARSEABLE entry, since none were.
         *
         * Distinguishes "we gave up immediately because the request was wrong" from "we tried
         * six times over half a minute" without needing to read the reason.
         */
        @JsonProperty("attempts_made")
        int attemptsMade,

        /* Which of the three roads to the dead-letter queue this took. */
        @JsonProperty("reason")
        DlqReason reason,

        /*
         * The status code of the final attempt, or null if no response was ever received.
         *
         * Nullable for the same reason the delivery log's column is (notes 10b): a timeout has
         * no status code, and inventing one would blame the subscriber's server for what may
         * have been our network.
         */
        @JsonProperty("last_status_code")
        Integer lastStatusCode,

        /*
         * Short description of the final failure — "timeout after 10000ms",
         * "ConnectException: Connection refused" — or null if a response arrived.
         *
         * Duplicated from the delivery log on purpose. The point of a dead letter is that it is
         * self-contained: whoever opens it should not have to join it against Postgres to find
         * out what went wrong.
         */
        @JsonProperty("last_failure_reason")
        String lastFailureReason,

        /* When we gave up. Not when the event was created, and not when it was last attempted. */
        @JsonProperty("dead_lettered_at")
        Instant deadLetteredAt,

        /*
         * The exact message that was on the queue.
         *
         * This is what makes replay possible, and it is the only field that is never null —
         * including for an UNPARSEABLE entry, where it is the whole point.
         */
        @JsonProperty("original_job")
        String originalJob
) {

    /**
     * Rejects the states that must never exist.
     *
     * <p>Invariants belong on the type rather than at each call site, so there is no path — now
     * or in a year — that can write a dead letter nobody can act on. Same reasoning as
     * {@code DeliveryJob}'s compact constructor (notes 7b).
     */
    public DlqEntry {
        Objects.requireNonNull(reason, "reason is required");
        Objects.requireNonNull(deadLetteredAt, "deadLetteredAt is required");

        // The one field with no acceptable absence. Without it the entry records that something
        // failed while making it impossible to do anything about it — which is the difference
        // between a dead-letter queue and a log line.
        if (originalJob == null || originalJob.isBlank()) {
            throw new IllegalArgumentException("originalJob is required — a dead letter that "
                    + "cannot be replayed is not a dead letter");
        }

        if (attemptsMade < 0) {
            throw new IllegalArgumentException("attemptsMade must be >= 0, was " + attemptsMade);
        }
    }

    /**
     * Builds an entry for a job that was understood but could not be delivered.
     *
     * <p>Covers both the immediate road ({@link DlqReason#NON_RETRIABLE_RESPONSE}) and the
     * exhausted one ({@link DlqReason#RETRIES_EXHAUSTED}) — the fields are identical, only the
     * reason differs, so one factory serves both and the caller states which road it took.
     *
     * @param job               the job as it was when we gave up
     * @param originalJob       the exact string read off the queue — NOT a re-serialisation, so
     *                          that a replay pushes back the identical bytes
     * @param reason            which road this took
     * @param lastStatusCode    the final attempt's status, or null if nothing answered
     * @param lastFailureReason the final attempt's failure description, or null if it answered
     * @param deadLetteredAt    the clock, passed in rather than read here, so this stays a pure
     *                          function that can be asserted against an expected value
     */
    public static DlqEntry forFailedJob(DeliveryJob job,
                                        String originalJob,
                                        DlqReason reason,
                                        Integer lastStatusCode,
                                        String lastFailureReason,
                                        Instant deadLetteredAt) {
        return new DlqEntry(
                job.eventId(),
                job.subscriberUrl(),
                job.attemptNumber(),
                reason,
                lastStatusCode,
                lastFailureReason,
                deadLetteredAt,
                originalJob
        );
    }

    /**
     * Builds an entry for a message that could not be turned into a job at all.
     *
     * <p>Everything derived from the job is absent, because there is no job — which is exactly
     * the information this entry conveys. What survives is the raw message, and that is what
     * someone needs in order to work out what wrote it.
     *
     * @param rawMessage     the unreadable string, exactly as it came off the queue
     * @param failureReason  what the parser said, so the entry explains itself
     */
    public static DlqEntry forUnparseableMessage(String rawMessage,
                                                 String failureReason,
                                                 Instant deadLetteredAt) {
        return new DlqEntry(
                null,
                null,
                0,
                DlqReason.UNPARSEABLE,
                null,
                failureReason,
                deadLetteredAt,
                rawMessage
        );
    }

    /**
     * Whether this entry could be put back on the queue.
     *
     * <p>Everything except an {@link DlqReason#UNPARSEABLE} entry can: the message was a valid
     * job, it just could not be delivered. An unparseable one could not be read when it arrived
     * and cannot be read now — it is evidence to be looked at, not work to be redone.
     *
     * <p>Note what this deliberately does NOT do: rebuild the job. Turning the raw string back
     * into a {@code DeliveryJob} is the codec's work, and a model class that reaches into the
     * serialization layer to do it would invert the dependency — the thing being converted would
     * depend on the converter. So the entry answers the question it owns ("is this replayable?")
     * and a caller that wants the job passes {@link #originalJob()} to the codec itself.
     *
     * <p>When a replay tool exists it must use {@code originalJob} rather than rebuilding from
     * the summary fields on this record, which deliberately exclude the payload — a replay
     * assembled from them would deliver an empty webhook.
     *
     * <p>🔴 <b>{@code @JsonIgnore} is load-bearing, not tidiness.</b> Jackson treats any method
     * shaped like {@code isX()} or {@code getX()} as a property, so without it this derived
     * method silently added a {@code "replayable": true} field to the stored JSON — a value
     * computed from {@code reason} being frozen into every entry at the moment it was written.
     * Change the rule about what is replayable and every old entry keeps yesterday's answer,
     * with nothing to indicate it is stale.
     *
     * <p>The general trap, worth carrying beyond this project: <b>adding a convenience method to
     * a serialized type changes its wire format.</b> For a format two processes must agree on,
     * that is a change nobody reviewed, in a file that looks like it only gained a helper.
     */
    @JsonIgnore
    public boolean isReplayable() {
        return reason != DlqReason.UNPARSEABLE;
    }
}
