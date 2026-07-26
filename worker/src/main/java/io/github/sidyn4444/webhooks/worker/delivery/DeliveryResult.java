package io.github.sidyn4444.webhooks.worker.delivery;

/**
 * The outcome of one delivery attempt: what came back, and how long it took.
 *
 * <p>Three things can happen when the worker POSTs to a subscriber, and this record has to
 * represent all three without distorting any of them:
 *
 * <ol>
 *   <li>the subscriber answered with a success status — {@code statusCode = 200}</li>
 *   <li>the subscriber answered with an error status — {@code statusCode = 503}, {@code 400}, …</li>
 *   <li><b>nothing came back at all</b> — a timeout, a DNS failure, a refused connection.
 *       No response was received, so <b>no status code exists</b>.</li>
 * </ol>
 *
 * <h2>Why {@code Integer} and not {@code int}</h2>
 *
 * <p>This is the design decision in the file. A primitive {@code int} cannot be null, so case (3)
 * would have to be encoded as some invented number — {@code 0}, {@code -1}, or worst of all
 * {@code 500}.
 *
 * <p>That last one is an actively harmful lie. {@code 500} means <i>the subscriber's server told
 * us it had failed</i>. A timeout means <i>we never heard from them at all</i>. Those have
 * different causes and different owners: the first is a bug on their side, the second may be DNS,
 * a firewall, or our own network. Someone debugging from a delivery log full of fabricated
 * {@code 500}s would investigate entirely the wrong system.
 *
 * <p>{@code Integer} is nullable, so "there is no status code" is expressible directly. The type
 * carries the fact rather than relying on a sentinel value every future reader must know about.
 * This is also why the Postgres column in Task 10 is nullable: "we tried and heard nothing" is a
 * real, recordable outcome.
 *
 * <h2>Why duration is recorded even on failure</h2>
 *
 * <p>It is often the most diagnostic number available. A failure at 10,003ms is a timeout; a
 * failure at 12ms is a connection refused instantly. Identical outcome, completely different
 * investigation. In Week 6 this becomes the p50/p95/p99 latency panels, and timeout rate and
 * error rate are deliberately separate charts.
 *
 * <p>Nothing acts on any of this yet. Session 3's retry logic branches on the status code
 * (retry 5xx/timeout/429, dead-letter non-retriable 4xx — notes 2e) and Task 10 writes these
 * fields to the delivery log. 9c's job is to produce them accurately.
 */
public record DeliveryResult(

        /** HTTP status the subscriber returned, or {@code null} if no response was received. */
        Integer statusCode,

        /** Wall-clock time spent on the attempt, including a timeout that ran to its limit. */
        long durationMs,

        /** Short description of a transport failure, or {@code null} when a response arrived. */
        String failureReason
) {

    /**
     * Whether this attempt counts as delivered.
     *
     * <p>Success is defined as any 2xx. Subscribers legitimately answer 200, 201, 202 or 204
     * depending on how they handle the event, and treating only 200 as success would dead-letter
     * perfectly good deliveries. The rule lives here, once, so the retry logic and the metrics
     * can never disagree about what "delivered" means.
     */
    public boolean succeeded() {
        return statusCode != null && statusCode >= 200 && statusCode < 300;
    }

    /**
     * Whether any response was received at all.
     *
     * <p>Distinguishes "they said no" from "nobody answered" — the distinction the nullable
     * status code exists to preserve.
     */
    public boolean hasResponse() {
        return statusCode != null;
    }

    /** Builds the no-response outcome. */
    public static DeliveryResult failed(long durationMs, String failureReason) {
        return new DeliveryResult(null, durationMs, failureReason);
    }

    /** Builds the outcome for a response that actually arrived, whatever its status. */
    public static DeliveryResult responded(int statusCode, long durationMs) {
        return new DeliveryResult(statusCode, durationMs, null);
    }

    /** Compact human-readable form for logs: {@code "204 in 87ms"} or {@code "no response after 10004ms (timeout)"}. */
    public String describe() {
        return hasResponse()
                ? statusCode + " in " + durationMs + "ms"
                : "no response after " + durationMs + "ms (" + failureReason + ")";
    }
}
