package io.github.sidyn4444.webhooks.common.queue;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.sidyn4444.webhooks.common.model.DeliveryJob;
import io.github.sidyn4444.webhooks.common.model.DlqEntry;

/**
 * Converts a {@link DeliveryJob} to the JSON string stored in Redis, and back.
 *
 * <p>Redis lists hold strings, not objects. So a job has to become text on the way in and an
 * object again on the way out — and crucially, those two steps happen in <b>different
 * processes</b>: the producer writes, a worker reads. The two must produce and expect exactly
 * the same text.
 *
 * <p>Both apps already have an {@code ObjectMapper} supplied by Spring Boot, and using those
 * would work today. It would also mean the wire format is determined by each application's
 * configuration — two files, in two modules, that nothing requires to match. A single
 * {@code spring.jackson.*} property added to one service to fix an unrelated HTTP concern
 * would silently change how queue messages are written, and the failure would appear in the
 * other service as fields that deserialize to null.
 *
 * <p>So the queue's wire format is defined here instead: one mapper, configured in code, in
 * the module both services compile against. The producer and the worker cannot disagree
 * because there is only one configuration and neither of them owns it. This is the same
 * argument that put the models and the key names in {@code common} — an agreement between two
 * processes belongs in the artifact they share.
 */
public final class JobCodec {

    /**
     * One mapper, shared. {@code ObjectMapper} is thread-safe once configured, which is why
     * the usual advice is to build one and reuse it: construction is comparatively expensive
     * (it builds and caches a serializer per type), while use is cheap. Creating one per
     * message is a classic and easily-missed performance mistake.
     */
    private static final ObjectMapper MAPPER = new ObjectMapper()

            // Teaches Jackson about java.time. Without this line, writing a DeliveryJob
            // throws: "Java 8 date/time type java.time.Instant not supported by default".
            .registerModule(new JavaTimeModule())

            // Write instants as ISO-8601 text ("2026-07-22T14:03:11.482Z") rather than the
            // default numeric epoch ("1784...E9").
            //
            // Both round-trip correctly, so this is chosen purely for the humans: reading the
            // queue with `redis-cli LRANGE webhooks:queue 0 -1` while debugging shows a
            // readable timestamp instead of a float nobody can interpret at a glance. The
            // cost is a slightly longer string, which is irrelevant next to the payload.
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

            // Ignore JSON fields the target type does not have, instead of throwing.
            //
            // This is the important one, and it is about deployments rather than parsing.
            // Producer and worker are separate deployments, so during any rolling update
            // BOTH VERSIONS RUN AT ONCE. If a field is added to DeliveryJob and the producer
            // updates first, the old workers still running will see a key they do not know.
            // With the default setting they would throw on every message until the last one
            // finished updating, turning a routine deploy into an outage.
            //
            // Ignoring unknown fields makes the format FORWARD-COMPATIBLE: adding a field is
            // safe in any deployment order. Removing or renaming one is still a breaking
            // change, which is why 7b added `enqueuedAt` up front rather than later.
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private JobCodec() {
    }

    /**
     * Serializes a job to the JSON string that gets pushed onto Redis.
     *
     * <p>Jackson signals failure with a checked {@code JsonProcessingException}, which would
     * force every call site to write a try/catch around a call that cannot realistically fail
     * — a {@code DeliveryJob} is six non-null fields of types Jackson handles natively, and
     * its constructor already rejects nulls. Wrapping it in an unchecked exception keeps the
     * call sites readable while still failing loudly if the impossible happens.
     *
     * <p>The wrapping keeps the original exception as the cause, so nothing is lost from the
     * stack trace. Swallowing it, or replacing it with a generic message, is what turns a
     * five-minute diagnosis into an hour.
     */
    public static String toJson(DeliveryJob job) {
        try {
            return MAPPER.writeValueAsString(job);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize DeliveryJob " + job.eventId(), e);
        }
    }

    /**
     * Rebuilds a job from the JSON string read off Redis.
     *
     * <p>Unlike serialization, this one <b>can</b> genuinely fail — the input came from an
     * external store and may be truncated, may have been written by an incompatible version,
     * or may be something a human put there by hand while debugging. The worker is expected
     * to catch this, and Session 3 routes such a message to the DLQ rather than letting one
     * unparseable entry stop the poll loop forever.
     */
    public static DeliveryJob fromJson(String json) {
        try {
            return MAPPER.readValue(json, DeliveryJob.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to deserialize DeliveryJob from queue", e);
        }
    }

    /**
     * Serializes a dead-letter entry to the JSON string pushed onto the dead-letter queue.
     *
     * <p>Handled by the same mapper as jobs, deliberately. The dead-letter queue is a second wire
     * format living in the same Redis, read by the same kinds of tools and by people running
     * {@code redis-cli}, and there is no reason for it to render timestamps differently or to
     * behave differently when a field is added. <b>One configuration means one set of surprises,
     * not two.</b>
     *
     * <p>In particular the forward-compatibility setting matters here for the same reason it does
     * for jobs: a dead letter can sit in Redis for days, so a future version of this record must
     * be able to read entries written by today's.
     */
    public static String toJson(DlqEntry entry) {
        try {
            return MAPPER.writeValueAsString(entry);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to serialize DlqEntry for event " + entry.eventId(), e);
        }
    }

    /**
     * Rebuilds a dead-letter entry from the JSON string read off the dead-letter queue.
     *
     * <p>Nothing in the delivery path calls this — the worker only ever writes to the DLQ. It
     * exists because a queue you can only write to is a bucket, and the entries are meant to be
     * read: by a replay tool, an admin endpoint, or a support script. Providing the read side now
     * keeps the format symmetric and makes it testable by round trip, which is the only way to
     * prove a serializer and a deserializer actually agree.
     */
    public static DlqEntry dlqEntryFromJson(String json) {
        try {
            return MAPPER.readValue(json, DlqEntry.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to deserialize DlqEntry from the DLQ", e);
        }
    }
}
