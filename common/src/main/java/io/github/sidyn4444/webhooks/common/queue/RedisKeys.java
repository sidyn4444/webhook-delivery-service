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
     * Jobs waiting out a backoff before their next attempt.
     *
     * <p><b>This is the one key here that is not a list.</b> It is a Redis <i>sorted set</i>:
     * every entry carries a number alongside it, and Redis keeps the whole thing permanently
     * ordered by that number. Here the number is the <b>absolute clock time, in epoch
     * milliseconds, at which the job becomes due</b>.
     *
     * <p>The reason it cannot be a list is the reason for the whole structure. A list answers
     * one question — <i>what is next in line?</i> — and jobs here do not leave in the order they
     * arrived. A job scheduled second with a 1-second backoff is due long before a job scheduled
     * first with a 16-second one. What is needed is a different question entirely: <i>which of
     * these are due yet?</i> That is a range query, and a sorted set is the only Redis structure
     * that answers one without reading everything.
     *
     * <p><b>Where {@link #PROCESSING} keeps time as a failsafe, this keeps time as the
     * instruction.</b> A job in {@code PROCESSING} is being worked on right now and its pickup
     * time only matters if something goes wrong; a job here is doing nothing at all until its
     * time arrives, so the time is the only thing that makes it actionable. That difference is
     * why {@code PROCESSING} can be a list with a separate index beside it while this cannot
     * (notes 12c, 14a).
     *
     * <p>Nothing polls this key for work. A scheduler asks it one question on a loop — "anything
     * due by now?" — and moves whatever comes back onto {@link #QUEUE}, where an ordinary worker
     * picks it up with no idea it was ever a retry.
     */
    public static final String RETRY = NAMESPACE + "retry";

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
