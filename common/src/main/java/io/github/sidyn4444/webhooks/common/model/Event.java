package io.github.sidyn4444.webhooks.common.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

/**
 * What arrives at {@code POST /events} — the public contract of this service.
 *
 * <p>This is the smallest thing a caller must hand over for a webhook to be delivered:
 * an id, a destination, and the data to send. Everything else the system needs
 * (attempt counters, retry ceilings, timestamps) is the service's own bookkeeping and
 * is deliberately kept out of the contract, because a caller should not have to know
 * how delivery works in order to ask for one.
 *
 * <p>A {@code record} is a class whose only job is to hold data. Declaring the three
 * components below generates the constructor, the accessors ({@code eventId()},
 * {@code subscriberUrl()}, {@code payload()}), plus {@code equals}, {@code hashCode}
 * and {@code toString}. The fields are final — an Event cannot be modified after it is
 * created, which is exactly what you want for something that is about to be copied into
 * a queue and read by another process.
 *
 * <p><b>Why this lives in {@code common} rather than in the producer.</b> The producer
 * parses it, but the worker later reads the same three values back out of Redis. If each
 * module owned its own copy of this shape, one could be changed without the other, and
 * the mismatch would not appear at compile time — it would appear at runtime, in
 * production, as a job the worker cannot deserialize. One definition, depended on by
 * both, makes that class of bug impossible.
 *
 * <h2>The annotations</h2>
 * <p>An annotation is an inert label attached to code. It does nothing by itself; it sits
 * there until some framework goes looking for it. Two different frameworks read the labels
 * below, for two different purposes:
 *
 * <ul>
 *   <li>{@code @JsonProperty} is read by <b>Jackson</b>, the JSON library, when converting
 *       between this object and JSON text.
 *   <li>{@code @NotBlank}, {@code @Pattern} and {@code @Size} are <b>Bean Validation</b>
 *       constraints. They describe what a valid Event is; the producer runs the validator
 *       that enforces them and rejects anything failing with a 400.
 * </ul>
 *
 * <p>The rules travel with the model on purpose. A reader opening this one file learns both
 * what the shape is and what counts as valid, with no second file to hunt down.
 */
public record Event(

        /*
         * The idempotency key (see notes 1e). The caller chooses it, and it must stay the
         * SAME across retries of the same logical event — that is the whole basis of
         * duplicate detection on the subscriber's side. A retry that invented a fresh id
         * would look like a brand-new event and no deduplication anywhere would help.
         *
         * Constrained to a UUID because an idempotency key has to be unique across callers
         * and across time; "order-1" from two different clients would collide, and a value
         * short enough to guess is a value someone will eventually reuse by accident.
         */
        @JsonProperty("event_id")
        @NotBlank(message = "event_id is required")
        @Pattern(
                regexp = "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
                message = "event_id must be a UUID"
        )
        String eventId,

        /*
         * Where the webhook gets POSTed.
         *
         * The check here is deliberately shallow — it only asserts the value looks like an
         * http(s) URL, which is enough to reject obvious junk early. It is NOT a security
         * control, and it must not be mistaken for one: a string can be a perfectly
         * well-formed URL and still point at 127.0.0.1, at a private 10.x address, or at
         * the cloud metadata endpoint 169.254.169.254 that hands out credentials. Blocking
         * those is SSRF prevention (notes 3b), it requires resolving the hostname to an IP
         * before deciding, and no regex can do it. That arrives in Session 3 as a real
         * validator; this line exists so that malformed input dies here instead of two
         * services later.
         */
        @JsonProperty("subscriber_url")
        @NotBlank(message = "subscriber_url is required")
        @Pattern(
                regexp = "^https?://.+",
                message = "subscriber_url must start with http:// or https://"
        )
        String subscriberUrl,

        /*
         * The body to deliver, as an opaque JSON string.
         *
         * "Opaque" is a design decision, not a limitation. This service never parses,
         * inspects, or logs the payload — it signs it and forwards it. Payloads may carry
         * personal data, and the safest way to avoid leaking data is to never look at it.
         * That is also why the Postgres delivery log has no payload column (notes 2b).
         *
         * The size cap is the boring kind of important. Without it, one caller can post a
         * 500 MB body, and that body then sits in Redis — an in-memory store — where a
         * handful of such requests exhaust the memory the entire queue depends on. 64 KB is
         * comfortably above any real webhook payload and far below anything dangerous.
         */
        @JsonProperty("payload")
        @NotBlank(message = "payload is required")
        @Size(max = 65536, message = "payload must not exceed 64KB")
        String payload

) {
}
