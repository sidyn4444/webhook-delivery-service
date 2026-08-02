package io.github.sidyn4444.webhooks.producer.web;

import io.github.sidyn4444.webhooks.producer.security.UrlVerdict;

/**
 * Raised when a subscriber URL fails the SSRF check, so the request never reaches the queue.
 *
 * <p><b>This class does not do anything.</b> It carries three values from the point where the
 * decision was made to the point where the HTTP response is written, and that is its whole job.
 *
 * <h2>Why an exception rather than returning a response from the controller</h2>
 *
 * <p>The controller could build a 400 itself. It deliberately does not, because the shape of an
 * error response is not the controller's business — {@link GlobalExceptionHandler} owns that for
 * the entire service, so every failure looks the same to a caller and the formatting is written
 * once. A controller that hand-builds one error body is the first step towards two endpoints
 * disagreeing about what an error looks like.
 *
 * <p>It is also what keeps the happy path readable: the controller says "this is not acceptable"
 * and stops, rather than threading a rejection through the rest of the method.
 *
 * <p>⚠️ Note this is the opposite call from the one in {@code WebhookSender}, where an HTTP status
 * from a subscriber is treated as <b>data</b> rather than an exception. The distinction is who the
 * outcome belongs to. A 503 from a subscriber is an <i>expected answer</i> that our retry logic
 * branches on — normal control flow, so it must not be an exception. A URL aimed at our own
 * network is the end of this request: there is no branch, nothing downstream runs, and the only
 * remaining question is what to tell the caller. <b>Exceptions are for "stop here", not for
 * "here is an answer you might not like."</b>
 *
 * <h2>Why it carries the detail rather than logging it here</h2>
 *
 * <p>Two audiences have to be served from one rejection — a private explanation for our log and a
 * deliberately vague sentence for the caller — and both decisions are made in the handler, in one
 * place. Logging at the throw site and formatting at the catch site would split that across two
 * files, which is how the two drift apart.
 *
 * @param eventId the id of the rejected event, so the log line is traceable. Never the payload
 * @param verdict which check failed — carries the caller-facing sentence
 * @param detail  the full internal explanation, naming the resolved address. <b>Log only</b>
 */
public class UnsafeUrlException extends RuntimeException {

    private final String eventId;
    private final UrlVerdict verdict;
    private final String detail;

    public UnsafeUrlException(String eventId, UrlVerdict verdict, String detail) {
        // The message is the internal detail. It reaches the log, never a caller: the handler
        // below chooses what is sent back, and it never sends this.
        super(detail);
        this.eventId = eventId;
        this.verdict = verdict;
        this.detail = detail;
    }

    public String eventId() {
        return eventId;
    }

    public UrlVerdict verdict() {
        return verdict;
    }

    public String detail() {
        return detail;
    }
}
