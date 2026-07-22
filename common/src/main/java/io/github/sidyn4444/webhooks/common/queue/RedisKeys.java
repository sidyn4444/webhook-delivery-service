package io.github.sidyn4444.webhooks.common.queue;

/**
 * Every Redis key this system touches, named in one place.
 *
 * <p>Redis has no schema, no tables and no declaration step. A key exists because someone
 * wrote to it, and a key that nobody wrote to is indistinguishable from a key that was
 * misspelled — both are simply absent. A producer pushing to {@code "queue"} while a worker
 * pops from {@code "webhooks:queue"} produces no error anywhere: the push succeeds, the pop
 * blocks forever, and the queue silently fills with jobs nobody will ever read.
 *
 * <p>Constants make that mismatch impossible to write, because both services compile against
 * this one file. It is the same argument as putting the model classes in {@code common} — the
 * two processes must agree, so the agreement lives where the compiler can enforce it.
 *
 * <h2>Why every key starts with {@code webhooks:}</h2>
 *
 * <p>Redis stores every key in a single flat namespace per database. There are no folders. A
 * shared Redis — which is what ElastiCache will be in Week 5 — holds keys from everything
 * pointed at it, so unprefixed names like {@code queue} or {@code dlq} are a collision waiting
 * for a second service to exist.
 *
 * <p>The colon is not syntax; Redis attaches no meaning to it. It is a near-universal
 * convention that tooling has grown to understand — Redis Insight and most dashboards render
 * {@code webhooks:queue} and {@code webhooks:dlq} as a browsable tree, and
 * {@code SCAN MATCH webhooks:*} becomes a way to find everything this service owns, which is
 * what you want when debugging at 3am or writing a cleanup script.
 */
public final class RedisKeys {

    /**
     * Private constructor: this class is a namespace, not a thing you instantiate.
     *
     * <p>Without it, Java supplies a public no-argument constructor automatically, and
     * {@code new RedisKeys()} compiles — creating an empty object with no purpose and
     * implying to the next reader that instances mean something. Making it private states
     * the intent in the only way the compiler will enforce. {@code final} on the class does
     * the matching job for inheritance.
     */
    private RedisKeys() {
    }

    /** Prefix owned by this service. Everything below is built from it. */
    public static final String NAMESPACE = "webhooks:";

    /**
     * The main work queue. The producer LPUSHes jobs onto the left; workers take from the
     * right, which makes it FIFO — oldest job served next (notes 2c).
     */
    public static final String QUEUE = NAMESPACE + "queue";

    /**
     * The in-flight holding list. A worker atomically moves a job here as it picks it up
     * (RPOPLPUSH), and removes it only after a successful delivery — that removal is the ack.
     *
     * <p>This list is what makes at-least-once delivery mechanically true. If a worker dies
     * mid-delivery the job is still sitting here rather than having evaporated with the
     * process, and a recovery sweep can push it back onto {@link #QUEUE} (notes 2c).
     *
     * <p>It is a holding area, not a second queue: nothing pops from it in order. It exists
     * so that there is always a trace of every job currently being worked on.
     */
    public static final String PROCESSING = NAMESPACE + "processing";

    /**
     * The dead-letter queue: jobs that failed permanently — either a non-retriable response
     * or the retry ceiling reached (notes 2e).
     *
     * <p>Without one, a permanently-failing webhook either retries forever (burning workers
     * on a subscriber that will never accept it) or is dropped silently (losing data with no
     * record). The DLQ is the third option: stop trying, keep the evidence, and let its size
     * be a metric someone can alert on.
     */
    public static final String DLQ = NAMESPACE + "dlq";

    /**
     * Prefix for the producer's duplicate-suppression keys. One key per event id, written
     * with a TTL so the set cannot grow without bound.
     *
     * <p>Use {@link #seenEvent(String)} rather than concatenating this by hand.
     *
     * <p>This is best-effort deduplication at the front door — it catches a caller that
     * submits the same event twice. It does <em>not</em> catch the duplicate that matters
     * most, which is created later, inside the worker, when a delivery succeeds but the
     * process dies before acknowledging it. Nothing the producer does can see that one, which
     * is why the subscriber remains the last line of defence and why every webhook provider
     * tells integrators to make their handlers idempotent on the event id (notes 1e, 2c).
     */
    public static final String SEEN_EVENT_PREFIX = NAMESPACE + "seen:";

    /**
     * Builds the duplicate-suppression key for one event, e.g.
     * {@code webhooks:seen:3f2b7c1a-9d4e-4f6b-8a2c-1e5d7f9b3c4d}.
     *
     * <p>A method rather than a constant because this key is per-event. Writing the
     * concatenation once here means no call site can invent a slightly different shape — the
     * kind of drift that produces two parallel sets of keys, each half-populated, with no
     * error to point at.
     */
    public static String seenEvent(String eventId) {
        return SEEN_EVENT_PREFIX + eventId;
    }
}
