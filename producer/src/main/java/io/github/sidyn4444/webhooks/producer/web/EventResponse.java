package io.github.sidyn4444.webhooks.producer.web;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The body returned from {@code POST /events}.
 *
 * <p>Deliberately tiny: it echoes the {@code event_id} the caller sent and a fixed
 * {@code status} of {@code "accepted"}. The echo lets a caller correlate the response with the
 * request it made — useful when requests are fired asynchronously — and confirms which id the
 * service actually parsed, which is the value that matters for idempotency and for chasing the
 * event through logs later.
 *
 * <p>This type lives in the producer, not in {@code common}, because it is not part of any
 * contract the worker shares. It is purely the producer's HTTP response shape, so nothing else
 * needs to know about it.
 */
public record EventResponse(

        @JsonProperty("event_id")
        String eventId,

        @JsonProperty("status")
        String status
) {
}
