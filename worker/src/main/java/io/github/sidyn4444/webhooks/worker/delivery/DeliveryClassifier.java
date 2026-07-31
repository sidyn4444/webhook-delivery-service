package io.github.sidyn4444.webhooks.worker.delivery;

/**
 * Turns the facts of a delivery attempt into a decision: delivered, worth retrying, or hopeless.
 *
 * <p>This is the branch the whole of Session 3 hangs off. Everything downstream — the retry
 * schedule, the backoff, the dead-letter queue — is machinery hanging off the answer this class
 * gives. It is deliberately the smallest, dumbest, most testable piece in the system: one pure
 * function, no state, no I/O, no clock, no Redis.
 *
 * <p><b>The principle it encodes</b> (notes 2e): a retriable failure means the request was fine
 * and the world was wrong; a permanent failure means the world was fine and the request was
 * wrong. Time fixes the first and cannot fix the second.
 *
 * <h2>Why a static utility rather than a Spring bean</h2>
 *
 * <p>It has no dependencies, no configuration and no state, so there is nothing for a container
 * to inject or manage — the same reasoning that makes {@code JobCodec} and {@code RedisKeys}
 * static (notes 7c, 7d). A pure static function is also the easiest thing in the codebase to
 * test: no mocks, no context, no setup.
 *
 * <p>The honest boundary: this stops being right the moment the policy needs to vary. A
 * per-subscriber retry policy — "this partner's flaky gateway gets ten attempts, everyone else
 * gets five" — needs configuration and therefore a bean. That is a real requirement in a mature
 * webhook product and it is deliberately out of scope here, because it needs a subscriber
 * registry the brief explicitly excludes (notes 3a).
 */
public final class DeliveryClassifier {

    private DeliveryClassifier() {
        // Utility class. A private constructor makes "new DeliveryClassifier()" a compile error
        // rather than a meaningless object, and documents the intent to anyone reading it.
    }

    /**
     * Classifies one attempt.
     *
     * @param result what actually happened, from {@link WebhookSender}
     * @return whether it was delivered, is worth retrying, or should be given up on
     */
    public static DeliveryOutcome classify(DeliveryResult result) {

        // ── 1. Success: any 2xx. ────────────────────────────────────────────────────────────
        // The definition lives on DeliveryResult, not here, deliberately: "2xx means success" is
        // HTTP's rule rather than our policy, and it is also read by the delivery-log row and the
        // metrics. One definition means the retry logic and the dashboards can never disagree
        // about what "delivered" means (notes 9c).
        if (result.succeeded()) {
            return DeliveryOutcome.DELIVERED;
        }

        // ── 2. No response at all → RETRIABLE. ──────────────────────────────────────────────
        // A timeout, a refused connection, a DNS failure, a TLS handshake that fell over. We
        // never heard from them, so we know nothing about whether the request was acceptable —
        // and "we couldn't reach you" is the most transient failure there is. Networks blip,
        // pods restart, load balancers rotate.
        //
        // ⚠️ The uncomfortable case this deliberately swallows: an unresolvable hostname. A
        // domain that does not exist will never resolve, so retrying it five times is guaranteed
        // waste. It is still classified retriable, because a DNS lookup failure genuinely cannot
        // be told apart from a DNS outage, a resolver hiccup, or a domain mid-propagation — and
        // guessing wrong in the other direction dead-letters real events during a resolver blip.
        //
        // The cost is bounded and small: five attempts over ~31 seconds, then the dead-letter
        // queue surfaces it to a human anyway. Paying half a minute to avoid discarding
        // deliverable events during a DNS wobble is the right side of that trade.
        if (!result.hasResponse()) {
            return DeliveryOutcome.RETRIABLE;
        }

        int status = result.statusCode();

        // ── 3. 5xx → RETRIABLE. ─────────────────────────────────────────────────────────────
        // The server admitted it failed. 500 internal error, 502 bad gateway, 503 unavailable,
        // 504 gateway timeout — every one of them describes a broken server rather than a broken
        // request, and every one of them routinely fixes itself. This is the single most common
        // reason a webhook needs retrying at all.
        //
        // Written as >= 500 rather than a list so that non-standard codes some servers emit
        // (509, 520-527 from certain CDNs) land on the retriable side, which is where a
        // server-side error belongs regardless of who invented the number.
        if (status >= 500) {
            return DeliveryOutcome.RETRIABLE;
        }

        // ── 4. The two 4xx exceptions. ──────────────────────────────────────────────────────
        // 4xx normally means "your request is wrong", which is why the default below is
        // permanent. These two are the standard exceptions, and both are explicitly about
        // TIMING rather than about the request being unacceptable:
        //
        //   429 Too Many Requests — "your request is fine, you are sending too many". This is a
        //       server asking to be retried later. It is the case exponential backoff was
        //       invented for: retrying immediately makes it strictly worse, waiting fixes it.
        //
        //   408 Request Timeout — "you were too slow sending it". Nothing about the content was
        //       rejected; the connection was. The same request over a healthier network works.
        //
        // A production system would go one step further and honour the Retry-After header when
        // a 429 carries one (notes 2e) — the server has told us exactly how long to wait, and
        // overriding that with our own backoff is both rude and usually wrong. That is a
        // deliberate omission here: it needs the response headers, which the sender discards
        // for the connection-leak reasons in 9c, so adding it means changing what the sender
        // captures rather than changing this rule.
        if (status == 429 || status == 408) {
            return DeliveryOutcome.RETRIABLE;
        }

        // ── 5. Everything else → PERMANENT. ─────────────────────────────────────────────────
        // 400 malformed, 401 unauthenticated, 403 forbidden, 404 wrong URL, 410 gone, 422
        // unprocessable. Every one is a statement about THIS request that will be equally true
        // for an identical request sent later.
        //
        // 3xx lands here too, and that is intentional rather than an oversight. The sender does
        // not follow redirects (notes 3b — following one is an SSRF bypass, since a public URL
        // can 302 to 169.254.169.254). So a 3xx means the subscriber's URL is stale and needs
        // updating by a human. Retrying returns the identical 3xx forever.
        //
        // 🔴 The default direction is a decision, not a fallthrough. An unrecognised status —
        // 418, or something bespoke — is treated as permanent, so the safe assumption is "do not
        // retry" rather than "retry". The reasoning: unknown is not the same as known-transient.
        // Guessing retriable costs five pointless attempts AND delays the dead-letter entry a
        // human needs in order to notice; guessing permanent surfaces it immediately and wastes
        // nothing. When you cannot tell, prefer the outcome that reaches a person faster.
        return DeliveryOutcome.PERMANENT;
    }
}
