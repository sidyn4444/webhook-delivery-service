package io.github.sidyn4444.webhooks.worker.delivery;

import io.github.sidyn4444.webhooks.common.model.DeliveryJob;
import io.github.sidyn4444.webhooks.common.security.HmacSigner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Duration;
import java.time.Instant;

/**
 * Performs one HTTP POST to a subscriber and reports what happened.
 *
 * <p>This is the class the entire system exists to reach. Everything before it — validation, the
 * queue, the poll loop — is machinery for getting a payload to this call reliably.
 *
 * <p>Its contract is narrow on purpose: <b>attempt the delivery and describe the outcome. Decide
 * nothing.</b> It does not retry, does not dead-letter, and does not treat an error status as a
 * problem. Those are policy, and policy belongs to the caller (Session 3). A sender that also
 * decided when to retry would be impossible to test against a subscriber that returns 503 without
 * also triggering a retry storm.
 *
 * <p>What it does own is the one guarantee the caller cannot supply for itself: <b>this call will
 * return within the timeout, always.</b>
 */
@Component
public class WebhookSender {

    private static final Logger log = LoggerFactory.getLogger(WebhookSender.class);

    /**
     * Carries the event id to the subscriber so they can discard a duplicate delivery.
     *
     * <p>This is not decoration. This system delivers <b>at least once</b> — an acknowledged
     * duplicate is chosen over a lost event at four separate points (notes 9d, 10d, 12e, 13b), and
     * every one of those decisions is justified by "the subscriber deduplicates on a stable event
     * id." That justification only holds if the id actually reaches them, and until now the outbound
     * request contained the payload and nothing else.
     *
     * <p>Deliberately outside the signature, matching Stripe and GitHub, whose event and delivery
     * ids also ride in unsigned headers. The duplicates this system genuinely produces come from our
     * own retries and reclaims rather than from an attacker, and those are all fixed by this. The
     * residual: someone who can modify a request in flight could alter the id to force reprocessing
     * — they cannot touch the body, and the five-minute freshness window bounds them. Folding the id
     * into the signed string would close that too, at the cost of changing a scheme that is already
     * published and verified.
     */
    public static final String EVENT_ID_HEADER = "X-Webhook-Event-Id";

    private final WebClient webClient;
    private final Duration hardTimeout;
    private final String hmacSecret;

    /**
     * @param builder     Spring Boot auto-configures a {@code WebClient.Builder} bean, pre-wired
     *                    with sensible codecs and observability hooks. Building from it, rather
     *                    than calling {@code WebClient.create()}, means that shared configuration
     *                    is inherited — including the Micrometer instrumentation that Week 6's
     *                    Prometheus metrics depend on.
     * @param hardTimeout the ceiling on a single delivery attempt, from configuration.
     * @param hmacSecret  the shared signing secret, from the environment with no default.
     */
    public WebhookSender(WebClient.Builder builder,
                         @Value("${webhook.http.timeout:10s}") Duration hardTimeout,
                         @Value("${webhook.hmac.secret}") String hmacSecret) {

        // FAIL AT STARTUP, NOT ON THE FIRST CUSTOMER EVENT.
        //
        // The property has no default, so a MISSING variable already fails context startup before
        // this line runs. This catches the other half: a variable that is SET AND EMPTY resolves to
        // "" and sails through. Both cases have to die here, because the alternative is a worker
        // that boots clean, passes every health check, and delivers webhooks that cannot be
        // verified by anybody — with nothing in any log saying so.
        //
        // Throwing from a constructor fails the Spring context, which exits the process with a
        // non-zero code. Same posture as the Redis password (8a) and the datasource credentials
        // (10a): a missing secret is a refusal to run, not a degraded mode.
        if (hmacSecret == null || hmacSecret.isBlank()) {
            throw new IllegalStateException(
                    "webhook.hmac.secret is missing or blank — refusing to start, because an "
                            + "unsigned webhook is indistinguishable from a forged one. "
                            + "Set HMAC_SECRET (generate with: openssl rand -hex 32).");
        }
        this.hmacSecret = hmacSecret;
        // ONE WebClient, built once and reused for every delivery. This is not a micro-optimisation:
        // a WebClient owns a connection pool, so reusing it lets repeated deliveries to the same
        // subscriber reuse an established TCP connection and skip the TLS handshake — the single
        // largest cost in an HTTPS request. Constructing one per call would silently discard the
        // pool and make every delivery pay full handshake price.
        this.webClient = builder.build();
        this.hardTimeout = hardTimeout;
    }

    /**
     * Delivers one job. Never throws; every outcome comes back as a {@link DeliveryResult}.
     */
    public DeliveryResult send(DeliveryJob job) {
        long startNanos = System.nanoTime();

        // 🔴 THE CLOCK IS READ HERE, PER ATTEMPT — NOT WHEN THE JOB WAS ENQUEUED.
        //
        // Signing once at enqueue time and storing the result would compute one HMAC instead of one
        // per attempt, and it would break retries silently. The subscriber rejects anything older
        // than five minutes, and a job can easily be delivered long after it was created: the
        // backoff schedule alone spans 31 seconds (12b), a recovery sweep adds at least 60 more
        // (14b), and a subscriber outage can stack both. A delivery carrying a six-minute-old
        // timestamp is refused for being LATE rather than wrong — and since 12a classifies a 401 as
        // permanent, the event would go straight to the dead-letter queue with a log line blaming
        // the subscriber.
        //
        // So: fresh timestamp, fresh signature, every attempt. The cost is an HMAC per delivery,
        // which is microseconds against a network call that takes milliseconds at best.
        long timestampSeconds = Instant.now().getEpochSecond();
        String signature = HmacSigner.headerValue(timestampSeconds, job.payload(), hmacSecret);

        try {
            Integer status = webClient.post()
                    .uri(job.subscriberUrl())
                    .contentType(MediaType.APPLICATION_JSON)

                    // Proves this delivery came from us, unaltered, and recently (15a).
                    .header(HmacSigner.SIGNATURE_HEADER, signature)

                    // Lets the subscriber discard a duplicate. See EVENT_ID_HEADER above.
                    .header(EVENT_ID_HEADER, job.eventId())

                    // THE BODY IS UNCHANGED — the same String that came off the queue, which is the
                    // same String that was just signed. Not a copy, not a re-serialization. If this
                    // ever became `objectMapper.writeValueAsString(...)` of a parsed payload, the
                    // bytes could differ from the signed ones by a space or a key order and every
                    // subscriber would reject every delivery (15a).
                    .bodyValue(job.payload())

                    // exchangeToMono gives raw access to the response and, critically, applies NO
                    // default error handling. The obvious alternative, retrieve(), throws a
                    // WebClientResponseException on any 4xx or 5xx — which is wrong here. A
                    // subscriber answering 503 is an ordinary, expected outcome that the retry
                    // logic must branch on; the status code is the ANSWER, not evidence of a
                    // fault. Recovering it by catching an exception and reading a field off it
                    // would be using exceptions for control flow.
                    //
                    // releaseBody() discards the response body while freeing the underlying
                    // network buffers. Skipping it leaks connections: an unreleased body holds
                    // its pooled connection open, and after enough deliveries the pool is
                    // exhausted and every subsequent request hangs waiting for a free slot.
                    // We genuinely do not want the body — a webhook subscriber's reply carries
                    // no information we act on, and reading arbitrary bodies from arbitrary
                    // hosts into memory is a needless risk.
                    .exchangeToMono(response ->
                            response.releaseBody().thenReturn(response.statusCode().value()))

                    // THE HARD CEILING. This wraps the whole operation — DNS resolution, TCP
                    // connect, TLS handshake, sending, and waiting for a reply — so no
                    // combination of slow stages can exceed it.
                    //
                    // Without it a subscriber that accepts a connection and then never responds
                    // holds this worker hostage indefinitely. That is not a hypothetical: it is
                    // the cheapest way for one bad subscriber to take out a delivery pipeline,
                    // because every worker that touches that URL stops doing anything else. A
                    // timeout converts an unbounded hang into a bounded, recorded failure.
                    .timeout(hardTimeout)

                    // The poll loop is a single blocking thread handling one job at a time
                    // (notes 9b), so the async result is awaited here. Concurrency in this system
                    // comes from running MORE WORKER PODS, not from more in-flight requests per
                    // worker — which is the whole reason the project is orchestrated by
                    // Kubernetes. The cost is honest: this worker delivers nothing else while
                    // waiting, up to the timeout.
                    .block();

            long durationMs = elapsedMs(startNanos);

            // status cannot be null here: exchangeToMono always produces a value, and any failure
            // path throws instead. The check documents that reasoning rather than trusting it.
            if (status == null) {
                return DeliveryResult.failed(durationMs, "no status returned");
            }
            return DeliveryResult.responded(status, durationMs);

        } catch (Exception e) {
            // Reached only when NO response was received: a timeout, an unresolvable host, a
            // refused connection, a TLS failure. Deliberately broad — this method promises never
            // to throw, and an unexpected exception escaping into the poll loop would be handled
            // far less precisely there.
            long durationMs = elapsedMs(startNanos);
            String reason = describeFailure(e);

            // The URL is logged but the payload never is (notes 2b). A subscriber URL is
            // operational data needed to diagnose a failure; the payload may contain personal
            // data and has no place in a log line.
            log.warn("Delivery failed for event {} to {} after {}ms: {}",
                    job.eventId(), job.subscriberUrl(), durationMs, reason);

            return DeliveryResult.failed(durationMs, reason);
        }
    }

    /**
     * Turns an exception into a short, stable, human-readable reason.
     *
     * <p>The raw message from a nested reactive failure is long, deeply wrapped, and unstable
     * across library versions — poor material for something that will be written to a database
     * column and grouped in a dashboard. A timeout in particular deserves naming explicitly,
     * because "did it time out or was it refused?" is the first question anyone asks.
     */
    private String describeFailure(Exception e) {
        Throwable root = rootCause(e);

        if (e instanceof java.util.concurrent.TimeoutException
                || root instanceof java.util.concurrent.TimeoutException) {
            return "timeout after " + hardTimeout.toMillis() + "ms";
        }
        return root.getClass().getSimpleName() + ": " + root.getMessage();
    }

    private Throwable rootCause(Throwable t) {
        Throwable current = t;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current;
    }

    /**
     * Elapsed milliseconds since a {@code System.nanoTime()} reading.
     *
     * <p>{@code nanoTime} rather than {@code currentTimeMillis} because only the former is
     * monotonic — guaranteed to move forward at a steady rate. Wall-clock time can jump backwards
     * when NTP corrects the system clock, which would produce negative or wildly inflated
     * durations at random. For measuring an interval, always use a monotonic clock; save
     * wall-clock for recording when something happened.
     */
    private static long elapsedMs(long startNanos) {
        return Duration.ofNanos(System.nanoTime() - startNanos).toMillis();
    }
}
