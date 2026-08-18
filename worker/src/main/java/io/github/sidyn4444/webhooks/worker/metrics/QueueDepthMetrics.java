package io.github.sidyn4444.webhooks.worker.metrics;

import io.github.sidyn4444.webhooks.common.queue.RedisKeys;
import io.github.sidyn4444.webhooks.worker.dlq.DeadLetterQueue;
import io.github.sidyn4444.webhooks.worker.retry.RetryQueue;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.function.ToDoubleFunction;

/**
 * Publishes the depth of all four queue structures as Prometheus gauges.
 *
 * <p><b>These four numbers cannot arrive for free, and that is the whole reason this class exists.</b>
 * Micrometer supplies JVM, HTTP and connection-pool metrics automatically because those are
 * properties of the <i>process</i>, and a library can see the process it lives in. Queue depth is a
 * property of the <i>problem domain</i>. Nothing else in the JVM knows that a Redis list named
 * {@code webhooks:dlq} exists, or that its length is the single most important number in the system.
 * Domain metrics are always hand-written; that is not a gap in Micrometer.
 *
 * <h2>Why a gauge and not a counter — the distinction that decides whether the graph means anything</h2>
 *
 * A <b>counter</b> only ever increases. It answers <i>"how many have happened, ever?"</i> and its raw
 * value is close to meaningless on its own — you read it through {@code rate()} to get "per second".
 * Deliveries attempted, retries scheduled, jobs dead-lettered: all counters.
 *
 * <p>A <b>gauge</b> is a level that goes up and down. It answers <i>"what is it right now?"</i> and is
 * read directly, with no {@code rate()} anywhere near it. Queue depth, memory in use, pods ready.
 *
 * <p>🔴 <b>Getting this backwards produces a chart that looks fine and says nothing.</b> A queue depth
 * modelled as a counter would only ever climb, so it could never show a backlog draining — the
 * single most useful thing a queue graph does. And {@code rate()} over a gauge is worse than
 * useless: it reports how fast the level is <i>changing</i>, so a queue sitting at a steady,
 * catastrophic 50,000 reports a rate of <b>zero</b>, identical to an empty queue.
 * <b>The instrument type is part of the measurement, not a detail of how it is stored.</b>
 *
 * <h2>🔴 Every worker pod reports the SAME value, and the dashboard has to know that</h2>
 *
 * These gauges do not describe this pod. They describe <b>shared state in Redis</b> that every pod
 * reads. Three workers therefore publish three identical series for one real number.
 *
 * <p>⚠️ <b>So a dashboard must aggregate these with {@code max()} — never {@code sum()}.</b> Summing
 * three pods' view of a 100-job backlog reports 300. That is not a rounding error, it is a number
 * that is wrong by exactly the replica count, and it gets <i>worse</i> as the deployment scales,
 * which is precisely when someone is looking at the graph. Nothing in Prometheus can detect this;
 * summing is the natural default and it produces a plausible, confident, wrong answer.
 *
 * <p>This is the exact opposite of the rule for {@code http_client_requests_seconds}, where each pod
 * measures its <i>own</i> traffic and {@code sum} is the only correct aggregation. <b>The question to
 * ask of any metric before graphing it: does each replica measure itself, or does each replica
 * measure the same shared thing?</b>
 *
 * <p>An alternative was available and rejected: elect one pod to publish these, so the value appears
 * once. That means a leader election, which this system has deliberately avoided everywhere else —
 * no pod knows any other pod exists, and correctness lives in the datastore. <b>Adding coordination
 * to tidy up a dashboard would trade a real architectural property for a cosmetic one.</b>
 * Documenting {@code max()} is the cheaper and more honest fix.
 */
@Component
public class QueueDepthMetrics {

    private static final Logger log = LoggerFactory.getLogger(QueueDepthMetrics.class);

    /**
     * What a depth reading of {@code -1} means, and why it is better than {@code 0}.
     *
     * <p>{@link DeadLetterQueue#size()} and {@link RetryQueue#size()} were written in Session 3 to
     * catch their own Redis failures and return {@code -1} rather than throw. That decision predates
     * this class and turns out to matter a great deal here.
     *
     * <p>🔴 <b>If an unreachable Redis reported {@code 0}, the dashboard would display an empty
     * dead-letter queue — a positive all-clear — at the exact moment the system had lost the ability
     * to tell.</b> "Everything is fine" and "I cannot see" would render identically, and the more
     * reassuring of the two would be the one on screen. {@code -1} is impossible for a real length,
     * so it is unmistakably a read failure. This class preserves that convention for the two depths
     * it reads itself rather than inventing a second one.
     */
    private static final double UNREADABLE = -1;

    private final StringRedisTemplate redis;

    public QueueDepthMetrics(StringRedisTemplate redis,
                             RetryQueue retryQueue,
                             DeadLetterQueue deadLetters,
                             MeterRegistry registry) {

        this.redis = redis;

        // ------------------------------------------------------------------
        // THE BACKLOG. The number that says whether the workers are keeping up.
        //
        // 🔴 This gauge is what makes Task 35's headline claim measurable at all. "N deliveries per
        // second" is only true if the system HELD that rate -- and a system accepting 500/sec while
        // delivering 200/sec looks identical to a healthy one from the producer's side, because
        // POST /events returns 202 either way. The difference is visible here and nowhere else.
        // Sustained throughput is the rate at which THIS number stays flat.
        // ------------------------------------------------------------------
        register(registry, "webhook.queue.depth",
                "Jobs waiting to be picked up by a worker",
                r -> listLength(RedisKeys.QUEUE));

        // ------------------------------------------------------------------
        // IN FLIGHT. Jobs taken off the queue and not yet finished.
        //
        // Normally near zero and briefly non-zero -- a job is only here while a worker is mid
        // delivery. ⚠️ A value that stays high is the signature of the failure this system was
        // built to survive: a worker died holding jobs, and they are waiting for the recovery
        // sweep rather than for a subscriber. That is a different problem from a deep queue and
        // needs its own line on the graph.
        // ------------------------------------------------------------------
        register(registry, "webhook.processing.depth",
                "Jobs picked up by a worker and not yet completed",
                r -> listLength(RedisKeys.PROCESSING));

        // ------------------------------------------------------------------
        // WAITING ON BACKOFF. Failed at least once, scheduled to be tried again later.
        //
        // Deliberately separate from queue.depth even though both mean "not delivered yet", because
        // the two have opposite implications: a deep QUEUE means we are too slow, while a deep RETRY
        // set means SUBSCRIBERS are failing and the system is behaving exactly as designed. Merging
        // them would hide which of those is happening.
        // ------------------------------------------------------------------
        register(registry, "webhook.retry.depth",
                "Jobs waiting on exponential backoff before another attempt",
                r -> safe(retryQueue.size()));

        // ------------------------------------------------------------------
        // 🔴 THE DEAD-LETTER QUEUE. The number the whole retry design exists to keep small, and the
        // only one here that represents permanent loss.
        //
        // Every other depth above is transient by nature -- it drains on its own. Nothing drains
        // this one. A job here has exhausted its retries or was rejected outright, and it will sit
        // here until a human looks. DeadLetterQueue.size()'s own comment says it: the only way
        // anyone notices is a metric. Until this line existed, nobody was ever going to.
        // ------------------------------------------------------------------
        register(registry, "webhook.dlq.depth",
                "Jobs permanently failed and awaiting manual attention",
                r -> safe(deadLetters.size()));
    }

    /**
     * Registers one gauge.
     *
     * <p>⚠️ <b>The {@code this} in the second argument is load-bearing and its absence is a silent
     * failure.</b> Micrometer holds a <b>weak reference</b> to the object a gauge is built from — by
     * design, so that a gauge cannot keep a dead object alive and leak it. The consequence is a
     * classic trap: build a gauge over a temporary or a lambda-captured local and it works, then
     * starts reporting {@code NaN} at some unpredictable point after garbage collection, with
     * nothing logged and nothing thrown. Passing {@code this} — a Spring singleton, strongly held
     * for the application's lifetime — is what makes these gauges permanent.
     *
     * <p>The base unit is deliberately unset: these are counts of jobs, not seconds or bytes, and
     * inventing a unit suffix would make the metric name lie.
     */
    private void register(MeterRegistry registry, String name, String description,
                          ToDoubleFunction<QueueDepthMetrics> reader) {
        Gauge.builder(name, this, reader)
                .description(description)
                .register(registry);
    }

    /**
     * Reads a Redis list length, converting every failure into {@link #UNREADABLE}.
     *
     * <p>🔴 <b>A gauge function runs during a Prometheus scrape, and an exception thrown here would
     * propagate into the scrape response and fail it.</b> That would take out the <i>entire</i>
     * metrics endpoint — JVM, HTTP, delivery latency, everything — because Redis was briefly
     * unavailable. The target would flip to {@code up=0} and every unrelated graph would go blank
     * at once, which reads exactly like a dead pod.
     *
     * <p>⚠️ <b>That is the worst possible coupling: a monitoring blackout caused by the monitoring
     * itself, during an incident.</b> So this method cannot throw. It logs at debug rather than warn
     * because it runs on every scrape and a warn here would produce a log line every 30 seconds per
     * pod for as long as Redis stayed down — turning one incident into a second one.
     */
    private double listLength(String key) {
        try {
            Long length = redis.opsForList().size(key);
            return length == null ? UNREADABLE : length;
        } catch (Exception e) {
            log.debug("Could not read the length of '{}' for a gauge: {}", key, e.toString());
            return UNREADABLE;
        }
    }

    /**
     * Passes through the {@code -1}-on-failure convention the Session 3 classes already use, so a
     * failed read looks the same whichever class performed it.
     */
    private static double safe(long size) {
        return size;
    }
}
