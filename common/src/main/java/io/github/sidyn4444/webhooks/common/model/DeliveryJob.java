package io.github.sidyn4444.webhooks.common.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Objects;

/**
 * The unit of work on the queue: an {@link Event} plus the state a worker needs to decide
 * what to do next.
 *
 * <p>An {@code Event} describes <i>what happened</i>. A {@code DeliveryJob} describes
 * <i>the work of delivering it</i>. They are deliberately separate types even though the
 * first three fields are identical, because they answer to different owners: {@code Event}
 * is the public contract a caller writes, while everything below the payload is this
 * service's private bookkeeping. Merging them would put {@code attempt_number} into the
 * API, which invites a caller to set it — and a caller who can set the attempt counter can
 * bypass the retry ceiling.
 *
 * <p>The producer creates one of these, serializes it to JSON, and pushes it onto the Redis
 * queue. A worker pops it, deserializes it back, and reads {@code attemptNumber} and
 * {@code maxRetries} to decide between "deliver", "retry later", and "dead-letter this".
 * Those two numbers are not logging fields — they are the inputs to a live decision. The
 * record of what happened on each try is a different object entirely
 * ({@code DeliveryAttempt}, Task 10).
 */
public record DeliveryJob(

        /* Carried unchanged from the Event. Stable across every retry — that stability is
         * the entire basis of duplicate detection on the subscriber's side (notes 1e). */
        @JsonProperty("event_id")
        String eventId,

        @JsonProperty("subscriber_url")
        String subscriberUrl,

        @JsonProperty("payload")
        String payload,

        /*
         * Which try this is. Starts at 1 for the first delivery, not 0 — the job that has
         * been tried once is on attempt 1, and log rows reading "attempt 1 of 5" need no
         * mental arithmetic. Off-by-one errors in retry counters are common enough that
         * the constructor below refuses to build a job with a number below 1.
         */
        @JsonProperty("attempt_number")
        int attemptNumber,

        /*
         * The ceiling. Exceeding it sends the job to the dead-letter queue (Session 3).
         *
         * Carrying the limit ON the job, rather than reading it from worker configuration
         * at decision time, means the policy a job was enqueued under travels with it. A
         * worker restarted with a different setting mid-flight cannot change the rules for
         * jobs already in the queue. That is the behaviour you want from an audit
         * standpoint, and it is what makes "why was this dead-lettered after 3 tries when
         * the config says 5?" an answerable question.
         */
        @JsonProperty("max_retries")
        int maxRetries,

        /*
         * When the producer put this job on the queue.
         *
         * This is the one field not in the original design sketch, and it earns its place:
         * subtracting it from the delivery timestamp gives QUEUE WAIT TIME — how long a job
         * sat before a worker picked it up. That is the number that tells you whether the
         * system is short of workers, and it is a required panel on the Week 6 Grafana
         * dashboard. It is added now rather than later on purpose: a field introduced after
         * jobs are already in flight arrives as null on every job enqueued before the
         * change, and the worker then needs a null branch that exists forever to handle a
         * migration that lasted an hour.
         *
         * Instant is a precise moment on the UTC timeline, with no time zone attached.
         * Timestamps that cross machines should always be instants — a local time is
         * ambiguous the moment two servers disagree about their zone.
         */
        @JsonProperty("enqueued_at")
        Instant enqueuedAt

) {

    /**
     * Compact canonical constructor — runs before the fields are assigned, on every
     * construction path including Jackson's deserialization.
     *
     * <p>These checks are not the same thing as the Bean Validation annotations on
     * {@link Event}. Those describe what valid <i>input from a caller</i> looks like and are
     * enforced by a validator at the edge, producing a 400. These are <b>invariants</b>: things
     * that must be true of every {@code DeliveryJob} that has ever existed, enforced by the
     * type itself. A job with a null event id or a zero attempt number is not a bad request,
     * it is a bug in this service — so it fails loudly at the point of construction rather
     * than travelling through the queue and surfacing somewhere unrelated.
     */
    public DeliveryJob {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(subscriberUrl, "subscriberUrl");
        Objects.requireNonNull(payload, "payload");
        Objects.requireNonNull(enqueuedAt, "enqueuedAt");
        if (attemptNumber < 1) {
            throw new IllegalArgumentException("attemptNumber must be >= 1, was " + attemptNumber);
        }
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must be >= 0, was " + maxRetries);
        }
    }

    /**
     * Builds the first delivery job for an accepted event.
     *
     * <p>A named factory rather than a raw constructor call at the call site: the producer
     * says {@code DeliveryJob.firstAttempt(event, 5)} and cannot accidentally start the
     * counter at 0 or 2. The rule "a new job starts at attempt 1" is stated once, here,
     * instead of being re-typed everywhere a job is created.
     *
     * <p>Reading the clock inside a factory does make this method untestable at a fixed
     * time. The alternative is passing a {@code java.time.Clock} in, which is the standard
     * fix and what a service under time-sensitive test would use. It is not worth the extra
     * parameter on every call site here, because nothing asserts on this exact value —
     * timing is measured as a duration between two readings, never against a fixed instant.
     */
    public static DeliveryJob firstAttempt(Event event, int maxRetries) {
        return new DeliveryJob(
                event.eventId(),
                event.subscriberUrl(),
                event.payload(),
                1,
                maxRetries,
                Instant.now()
        );
    }

    /**
     * Returns a copy of this job with the attempt counter advanced by one.
     *
     * <p>A copy, not a mutation — the record's fields are final, so "changing" a job means
     * producing a new one. That is the safe behaviour when a job may be referenced from more
     * than one place: the version sitting in Redis cannot silently change underneath a
     * worker that is mid-delivery.
     *
     * <p>{@code enqueuedAt} is deliberately carried over unchanged rather than reset. It
     * records when the event first entered the system, so the queue-wait measurement covers
     * the whole life of the job rather than restarting the stopwatch on every retry — which
     * would hide exactly the slow, repeatedly-failing jobs the metric exists to expose.
     *
     * <p>Used by the retry logic in Session 3; nothing in Session 2 calls it yet.
     */
    public DeliveryJob nextAttempt() {
        return new DeliveryJob(
                eventId, subscriberUrl, payload,
                attemptNumber + 1, maxRetries, enqueuedAt
        );
    }

    /**
     * Whether another delivery is permitted after this one fails.
     *
     * <p>The comparison lives here, on the data, rather than being written inline in the
     * worker. One definition of "out of retries" means the retry loop and the dead-letter
     * check can never disagree about the boundary — the classic {@code <} versus
     * {@code <=} bug that silently gives every job one extra attempt, or one fewer.
     */
    public boolean hasRetriesLeft() {
        return attemptNumber <= maxRetries;
    }
}
