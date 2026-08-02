package io.github.sidyn4444.webhooks.producer.web;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One place that turns exceptions from any controller into a clean {@link ApiError} response.
 *
 * <p>{@code @RestControllerAdvice} makes this class a cross-cutting interceptor: an exception
 * thrown while handling a request in <em>any</em> controller is offered here first. Error
 * formatting is therefore written once, not repeated as a try/catch in every endpoint, and a
 * second endpoint added later inherits identical error handling for free.
 *
 * <p>It extends {@link ResponseEntityExceptionHandler} rather than starting from nothing, and
 * that choice is load-bearing. That base class already carries dedicated handlers for the
 * standard Spring MVC exceptions — wrong HTTP method (405), unsupported media type (415), and
 * so on. A naive {@code @ExceptionHandler(Exception.class)} added on its own would be broader
 * than those and would swallow them, turning a correct 405 into a 500. By extending the base
 * class, those specific handlers stay in place and win by specificity; the catch-all below only
 * ever fires for something genuinely unanticipated.
 */
@RestControllerAdvice
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    /**
     * A {@code @Valid} failure. Spring has already collected every rule that failed into the
     * exception's binding result; this turns that into a field-name → message map.
     *
     * <p>The messages are exactly the {@code message} attributes written on the annotations of
     * {@code Event} — "event_id must be a UUID", "payload is required". The rule and the text a
     * caller sees to fix it live together on the model, and surface here unchanged.
     *
     * <p>{@code putIfAbsent} keeps the first message per field, so a field failing two rules at
     * once reports one clear reason rather than a confusing pile.
     */
    @Override
    protected ResponseEntity<Object> handleMethodArgumentNotValid(
            MethodArgumentNotValidException ex, HttpHeaders headers,
            HttpStatusCode status, WebRequest request) {

        Map<String, String> fieldErrors = new LinkedHashMap<>();
        ex.getBindingResult().getFieldErrors().forEach(fe ->
                fieldErrors.putIfAbsent(toSnakeCase(fe.getField()), fe.getDefaultMessage()));

        return new ResponseEntity<>(
                ApiError.validation("Validation failed", fieldErrors),
                HttpStatus.BAD_REQUEST);
    }

    /**
     * The binding result reports Java field names ({@code eventId}), but this API speaks
     * snake_case to callers ({@code event_id}) — so the key a caller sees must match the key
     * they sent, not the internal field name. This translates {@code camelCase -> snake_case}
     * so the error key and the JSON field the caller submitted line up.
     */
    private static String toSnakeCase(String camelCase) {
        return camelCase.replaceAll("([a-z])([A-Z])", "$1_$2").toLowerCase();
    }

    /**
     * The request body was not valid JSON at all (truncated, empty, wrong syntax), so it never
     * became an {@code Event} to validate.
     *
     * <p>The raw exception carries the parser's position and internals. Those are deliberately
     * dropped: a caller is told the body was unreadable and nothing about the parser. Leaking
     * parse internals is small, but it is free reconnaissance and there is no reason to give it.
     */
    @Override
    protected ResponseEntity<Object> handleHttpMessageNotReadable(
            HttpMessageNotReadableException ex, HttpHeaders headers,
            HttpStatusCode status, WebRequest request) {

        return new ResponseEntity<>(
                ApiError.of(HttpStatus.BAD_REQUEST, "Malformed or unreadable JSON request body"),
                HttpStatus.BAD_REQUEST);
    }

    /**
     * A subscriber URL that failed the SSRF check (notes 3b, 16a). The event is refused with a
     * 400 and never reaches the queue.
     *
     * <p><b>This method is the whole two-audience split, in four lines.</b> The rejection carries
     * two explanations at two levels of detail, and exactly one of them is allowed to leave the
     * building:
     *
     * <ul>
     *   <li>{@code detail} — "host localtest.me resolves to blocked address 127.0.0.1" — goes to
     *       <b>our log only</b>. It names the address that was resolved, which is the one fact
     *       that cannot be recovered afterwards: re-resolving the name tomorrow asks the
     *       attacker's own DNS server what it feels like saying then, and a one-second TTL is a
     *       configuration setting. For a security control the log is evidence.
     *   <li>{@code verdict.publicMessage()} — "subscriber_url is not an allowed destination" —
     *       goes to <b>the caller</b>, and says nothing else. A specific message would turn this
     *       400 into a network scanner: submit addresses one at a time, read the replies, and map
     *       an internal network you cannot reach by making <i>us</i> describe it.
     * </ul>
     *
     * <p>It is the same split as {@link #handleUnexpected}, applied to a different secret. There
     * the caller is denied a stack trace; here they are denied an address.
     *
     * <p>{@code WARN} rather than {@code ERROR}: nothing is broken, and a blocked URL is a
     * <i>refusal working correctly</i>. But it is not {@code INFO} either — someone aiming an
     * event at an internal address is worth noticing, and a burst of these is a probe rather than
     * a typo. Logged with the event id so it is traceable; never with the payload.
     *
     * <p>The response deliberately reuses {@link ApiError#validation} with a
     * {@code subscriber_url} key, so this is indistinguishable in shape from any other rule that
     * field can fail. A caller writes one piece of error-handling code, not two.
     */
    @ExceptionHandler(UnsafeUrlException.class)
    public ResponseEntity<ApiError> handleUnsafeUrl(UnsafeUrlException ex) {
        log.warn("Rejected event {} — unsafe subscriber_url: {}", ex.eventId(), ex.detail());

        return new ResponseEntity<>(
                ApiError.validation("Validation failed",
                        Map.of("subscriber_url", ex.verdict().publicMessage())),
                HttpStatus.BAD_REQUEST);
    }

    /**
     * The catch-all: anything not handled above is an unanticipated fault — a bug, a downstream
     * outage, something we did not foresee.
     *
     * <p>The split here is the whole security point. The <b>full</b> exception, stack trace and
     * all, goes to the server log via {@code log.error}, where operators need it to diagnose.
     * The <b>caller</b> gets a fixed, generic message and nothing else. A stack trace returned
     * to a caller would reveal the framework, library versions, and internal class names — a map
     * an attacker uses to look up known weaknesses. We decide exactly what leaves the building.
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiError> handleUnexpected(Exception ex) {
        log.error("Unhandled exception while processing request", ex);
        return new ResponseEntity<>(
                ApiError.of(HttpStatus.INTERNAL_SERVER_ERROR, "An unexpected error occurred"),
                HttpStatus.INTERNAL_SERVER_ERROR);
    }
}
