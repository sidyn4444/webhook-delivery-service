package io.github.sidyn4444.webhooks.worker.metrics;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.config.MeterFilter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * Caps the label cardinality of the outbound delivery timer.
 *
 * <p><b>This class exists because of a measurement that contradicted a prediction (32a).</b>
 * {@code WebhookSender} calls {@code .uri(job.subscriberUrl())} — a fully-formed URL string, not a
 * URI template. The expectation was that Spring's client observation convention, having no template
 * to work from, would tag {@code uri="none"} and the question would be closed. It does not. It
 * extracts the <b>path of the actual URL</b>, so a delivery to
 * {@code https://acme.example.com/hook} was measured emitting {@code uri="/hook"}, and
 * {@code client.name="acme.example.com"} beside it — rendered by the Prometheus exporter
 * as {@code client_name}, which is a distinction that matters and is dealt with at
 * {@link #UNBOUNDED_TAGS}.
 *
 * <p><b>Both of those label values are supplied by whoever registered the subscriber, and neither is
 * bounded by anything.</b> That is the problem, and it is not a small one:
 *
 * <ul>
 *   <li>Prometheus stores <b>one independent time series per unique combination of label
 *       values</b>. Two subscribers on different paths are not two rows in one series; they are two
 *       series.</li>
 *   <li>Enabling {@code percentiles-histogram} in {@code application.properties} took this timer
 *       from 3 series to <b>54</b> — 51 cumulative buckets plus count, sum and max. Measured, not
 *       estimated.</li>
 *   <li>So the cost of one new subscriber URL is 54 series <b>per worker pod</b>. At 3 pods and 100
 *       subscribers that is ~16,000 series from this single metric family, against ~150 for the
 *       entire rest of the worker.</li>
 * </ul>
 *
 * <p>🔴 <b>The failure mode is what makes this worth a class rather than a comment.</b> Unbounded
 * label cardinality does not degrade the application — the worker keeps delivering webhooks
 * perfectly. It degrades <i>Prometheus</i>, which holds every series in memory, and it does so
 * gradually and then all at once. The monitoring dies while the thing being monitored is healthy,
 * which is precisely the moment you need it. And the trigger is not load: it is <b>one new customer
 * with a URL nobody has used before</b>, so it cannot be reproduced by a load test that hammers a
 * single endpoint. This project's own load test (35) points every request at one receiver and would
 * therefore report this as fine.
 *
 * <p><b>What is dropped and what is deliberately kept.</b> The two unbounded tags collapse to a
 * single constant. Everything else stays, because everything else is bounded by something real and
 * is what the dashboards are built from:
 *
 * <ul>
 *   <li>{@code status} — HTTP status codes, a fixed set. This is how success/failure is split.</li>
 *   <li>{@code outcome} — {@code SUCCESS}/{@code CLIENT_ERROR}/{@code SERVER_ERROR}, five values.</li>
 *   <li>{@code method} — always POST here.</li>
 *   <li>{@code exception}, {@code error} — bounded by the number of exception classes that can be
 *       thrown, which is a property of our code rather than of our customers.</li>
 * </ul>
 *
 * <p>⚠️ <b>The cost, stated rather than buried: per-subscriber latency becomes unanswerable.</b>
 * "Is Acme's endpoint slow?" cannot be asked of this metric any more. That is a real loss and it is
 * accepted for two reasons. First, the claim this timer exists to support is a <b>fleet-wide</b>
 * p95 — the résumé number is about the system, not about one subscriber. Second, the question is
 * better answered by the delivery-attempt log in Postgres, which already stores
 * {@code duration_ms} per attempt and is queryable per subscriber without any cardinality cost at
 * all, because a database row is cheap in exactly the way a time series is not.
 *
 * <p><b>Why a {@code MeterFilter} and not a properties line.</b> There is no property that removes
 * or rewrites a tag; {@code management.metrics.tags.*} only <i>adds</i> common tags. Rewriting an
 * identity is a programmatic operation, and {@link Meter.Id#replaceTags} is the supported hook.
 * The filter runs when a meter is first registered, so it rewrites the identity before any series
 * is ever published — the high-cardinality series are never created, rather than created and then
 * discarded.
 */
@Configuration
public class OutboundMetricsConfig {

    /**
     * The meter-name prefix this filter applies to, and <b>only</b> this family.
     *
     * <p>Micrometer's convenience helpers ({@code MeterFilter.replaceTagValues}) apply across every
     * meter in the registry. That would be wrong here: {@code uri} is also a tag on the producer's
     * {@code http.server.requests}, where it is bounded by the number of routes <i>we</i> wrote and
     * is genuinely useful. A filter is scoped for the same reason a security rule is scoped to a
     * path — the blast radius of "all meters" is the problem, not the typing.
     *
     * <p>🔴 <b>A PREFIX AND NOT AN EXACT NAME, AND THIS WAS THE SECOND HALF-FAILURE CAUGHT BY
     * MEASUREMENT AT 32a.</b> Spring does not register one meter for an outbound call, it registers
     * two: {@code http.client.requests} (a Timer, the completed call) and
     * {@code http.client.requests.active} (a LongTaskTimer, calls in flight right now). <b>Both
     * carry the same {@code uri} and {@code client.name} tags</b>, so an exact-name filter cleaned
     * the first and left the second — measured at <b>62 series for two subscriber hostnames</b>,
     * the identical unbounded growth simply wearing a different meter name.
     *
     * <p>⚠️ <b>The general lesson, which is the part worth keeping: a cardinality fix has to be
     * verified against the SCRAPE, not against the meter you were thinking about.</b> Nothing in
     * the code hinted a second meter existed; only grepping the exported output for the raw
     * hostname found it, and the first two attempts at this filter both looked correct by every
     * check that started from the code.
     *
     * <p>The prefix cannot over-reach: {@code http.server.requests} does not start with it.
     */
    private static final String OUTBOUND_TIMER_PREFIX = "http.client.requests";

    /**
     * The replacement value. A constant rather than an empty string or {@code "none"}: a reader
     * seeing {@code uri="subscriber"} on a dashboard learns that the dimension was deliberately
     * collapsed, whereas {@code "none"} reads like instrumentation that failed.
     */
    private static final String COLLAPSED = "subscriber";

    /**
     * The two tag keys whose values are supplied by the subscriber and bounded by nothing.
     *
     * <p>⚠️ <b>Written in Micrometer's spelling, not Prometheus's, and getting that backwards is a
     * silent half-failure that was actually made and caught here (32a).</b> Micrometer tag keys use
     * dots — Spring's client observation names this one {@code client.name}. The Prometheus exporter
     * rewrites dots to underscores <b>at render time</b>, which is why the scrape output reads
     * {@code client_name}. A filter matching {@code "client_name"} therefore matches nothing, because
     * {@link MeterFilter#map} runs at <b>registration</b>, long before any rendering happens.
     *
     * <p>🔴 <b>The reason this was worth catching rather than shipping: the failure is partial and
     * therefore convincing.</b> {@code uri} has no dot in either spelling, so it collapsed correctly
     * and the fix looked like it worked. Only reading the full scrape line — rather than checking
     * that {@code uri} had changed — showed the subscriber's hostname still sitting there as an
     * unbounded label. <b>A verification that confirms the case you thought of is not a
     * verification.</b>
     *
     * <p>Matched through {@link #normalise} rather than by listing both spellings, so a future tag
     * cannot reintroduce the same mistake.
     */
    private static final List<String> UNBOUNDED_TAGS = List.of("uri", "client.name");

    /**
     * Collapses the two spellings a tag key can arrive in so a match cannot depend on which one the
     * author happened to think of.
     */
    private static String normalise(String tagKey) {
        return tagKey.replace('.', '_');
    }

    @Bean
    MeterFilter outboundDeliveryCardinalityFilter() {
        return new MeterFilter() {
            @Override
            public Meter.Id map(Meter.Id id) {
                if (!id.getName().startsWith(OUTBOUND_TIMER_PREFIX)) {
                    return id;
                }
                List<Tag> rewritten = new ArrayList<>();
                for (Tag tag : id.getTags()) {
                    boolean unbounded = UNBOUNDED_TAGS.stream()
                            .map(OutboundMetricsConfig::normalise)
                            .anyMatch(normalise(tag.getKey())::equals);
                    rewritten.add(unbounded ? Tag.of(tag.getKey(), COLLAPSED) : tag);
                }
                return id.replaceTags(Tags.of(rewritten));
            }
        };
    }
}
