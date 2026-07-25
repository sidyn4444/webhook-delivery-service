package io.github.sidyn4444.webhooks.producer.web;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.springframework.http.HttpStatus;

import java.time.Instant;
import java.util.Map;

/**
 * The single JSON shape every error response from the producer takes.
 *
 * <p>One consistent error shape means a caller writes their error-handling code once and it
 * works for every failure this service can return — a 400 from bad input and a 500 from an
 * unexpected fault have the same fields, so the caller never has to branch on which kind of
 * error arrived to find the message.
 *
 * <p>{@code @JsonInclude(NON_EMPTY)} omits {@code field_errors} entirely when the map is empty,
 * so a non-validation error (a 500, say) simply has no {@code field_errors} key rather than an
 * awkward empty object. The field appears only when it has something to say.
 *
 * <p>Deliberately absent: any stack trace, exception class name, or framework detail. Those go
 * to the server log, never to the caller — see {@link GlobalExceptionHandler} for why.
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public record ApiError(

        Instant timestamp,
        int status,
        String error,
        String message,

        @JsonProperty("field_errors")
        Map<String, String> fieldErrors
) {

    /** For errors with no per-field detail (malformed JSON, an unexpected 500). */
    public static ApiError of(HttpStatus status, String message) {
        return new ApiError(Instant.now(), status.value(), status.getReasonPhrase(), message, Map.of());
    }

    /** For validation failures: a 400 carrying a field-name → message map. */
    public static ApiError validation(String message, Map<String, String> fieldErrors) {
        return new ApiError(Instant.now(),
                HttpStatus.BAD_REQUEST.value(),
                HttpStatus.BAD_REQUEST.getReasonPhrase(),
                message,
                fieldErrors);
    }
}
