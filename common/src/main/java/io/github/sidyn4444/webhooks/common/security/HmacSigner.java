package io.github.sidyn4444.webhooks.common.security;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.HexFormat;

/**
 * Signs outbound webhooks.
 *
 * <p>The problem this solves belongs entirely to the subscriber. A POST arrives at their server
 * claiming to be a payment event from us — but anyone on the internet can POST to a URL, and
 * anything on the path between us could have altered the body. They cannot answer either question
 * from the request alone: <b>did this come from us</b>, and <b>is it unchanged</b>.
 *
 * <p>A signature answers both. We compute a fingerprint of the message using a secret only we and
 * that subscriber hold, and send it alongside. They recompute it and compare. An attacker can copy
 * the message and read the fingerprint, but cannot produce a valid fingerprint for a message they
 * altered, because they do not have the secret.
 *
 * <h2>What is signed, and why it is not just the payload</h2>
 *
 * <p>The signed string is {@code <unix seconds> + "." + <payload>}. Signing the payload alone
 * would leave a captured request replayable forever: the body never changes, so its signature
 * never expires, and an attacker who records one valid delivery can resend it a thousand times a
 * year later and every one of them verifies. Folding a timestamp in gives the subscriber something
 * to reject on — anything older than a few minutes is refused regardless of how valid the
 * signature is.
 *
 * <p>It has to be <b>inside</b> the signed string rather than beside it. A timestamp sent as its
 * own unprotected header buys nothing at all, because the attacker simply edits it to the current
 * time before replaying. Inside, editing it invalidates the signature.
 *
 * <h2>Why this class is stateless and holds no secret</h2>
 *
 * <p>Every method takes the secret as a parameter. Nothing is cached, nothing is configured, and
 * there is no Spring annotation anywhere in the file — it is arithmetic, not a service. That
 * matters for three reasons: the same code can be called by a signer and by a verifier holding
 * different secrets; a test can exercise it with no application context, no Redis and no network;
 * and there is no instance whose secret could be left stale after a rotation.
 *
 * <h2>Why it lives in {@code common}</h2>
 *
 * <p>The producer never signs anything — signing happens at the moment of the outbound POST, which
 * only the worker makes. So this is not here because two modules use it. It is here because
 * {@code common} holds the things that are <b>agreements with something outside the module</b>:
 * {@code Event} is the agreement with the caller, {@code JobCodec} and {@code RedisKeys} the
 * agreement between producer and worker, and this is the agreement with the <b>subscriber</b> —
 * the most externally visible contract in the system, since a stranger's server reimplements it
 * from documentation. It also costs nothing to put here: {@code Mac}, {@code SecretKeySpec} and
 * {@code HexFormat} all ship inside the JDK, so the producer inherits no new dependency.
 *
 * <p>The boundary is worth stating, because it is where the reasoning would flip: had signing
 * needed a third-party crypto library, this would live in {@code worker} and the duplication would
 * be accepted, rather than making the producer inherit that library's vulnerabilities, version
 * conflicts and image size for code it never calls. <b>Shared modules carry contracts, not
 * conveniences.</b>
 */
public final class HmacSigner {

    /**
     * The JCA name for HMAC over SHA-256. Every Java SE implementation is required to provide it,
     * which is what makes the "cannot happen" catch below genuinely unreachable.
     */
    private static final String ALGORITHM = "HmacSHA256";

    /** The header every signed delivery carries. Named here so signer and verifier cannot drift. */
    public static final String SIGNATURE_HEADER = "X-Webhook-Signature";

    /**
     * Identifies the signing <b>scheme</b> — not the key.
     *
     * <p>{@code v1} means: sign {@code timestamp.payload}, with HMAC-SHA256, hex-encoded lowercase.
     * Changing any one of those would be {@code v2}, and the two could be sent side by side while
     * subscribers migrated: {@code t=…,v1=<old scheme>,v2=<new scheme>}.
     *
     * <p><b>What makes rotation survivable is the shape of the header rather than this constant:</b>
     * it can carry more than one signature, each labelled. Rotating the <i>secret</i> keeps the same
     * scheme and sends two {@code v1} values, one under the old key and one under the new
     * ({@code t=…,v1=<old key>,v1=<new key>}); the subscriber accepts either, and the old key is
     * withdrawn once everyone has moved. The labels are what make any of this parseable — without
     * them a receiver could not tell which signature to compute which way.
     *
     * <p>Both ends can never flip at the same instant, because messages are already in flight. A
     * format with no room for a second signature leaves only a flag day, or breaking every
     * subscriber at once.
     */
    public static final String VERSION_TAG = "v1";

    /** Never instantiated: every method is static and the class holds no state. */
    private HmacSigner() {
        throw new AssertionError("HmacSigner is a utility class and is never instantiated");
    }

    /**
     * Builds the exact string that gets signed: {@code <timestamp>.<payload>}.
     *
     * <p>This is deliberately its own method even though it is one line of string concatenation,
     * because <b>both sides have to produce it byte for byte identically</b> and one of those sides
     * is someone else's server in someone else's language. Every character here is part of the
     * contract: a dot rather than a colon, the timestamp first, no whitespace, no wrapper.
     *
     * <p>The timestamp is a {@code long} of whole seconds rather than an {@code Instant} for the
     * same reason. A whole number has exactly one textual form — no timezone, no locale, no leading
     * zeros, no question about whether milliseconds are included. Each of those questions is a way
     * for two independent implementations to disagree, and a disagreement here rejects every
     * legitimate webhook. Whole seconds also match the industry convention, and the freshness window
     * is measured in minutes, so sub-second precision would carry no information.
     *
     * <p>Note this differs from {@code RetryQueue} and {@code InFlightIndex}, which both use epoch
     * <i>milliseconds</i>. Those are internal scores that only our own code reads. This is an
     * external contract, so it follows the outside convention rather than ours.
     *
     * @param timestampSeconds seconds since the Unix epoch, as the header will carry it
     * @param payload          the body exactly as it will be sent — never a reformatted copy
     */
    public static String signedString(long timestampSeconds, String payload) {
        // Not a redundant check. String concatenation does NOT throw on null — it silently produces
        // the four characters "null", so a null payload here would sign the literal text
        // "1753996800.null", return a perfectly valid-looking signature, and send a body that does
        // not match it. The subscriber would reject every such delivery with no clue why.
        if (payload == null) {
            throw new IllegalArgumentException("payload must not be null");
        }
        return timestampSeconds + "." + payload;
    }

    /**
     * Computes the signature as 64 lowercase hex characters.
     *
     * <p>Two steps happen here and they are often spoken of as one. <b>HMAC-SHA256 does the
     * arithmetic</b>, taking the signed string and the secret and producing 32 raw bytes.
     * <b>Hex encoding only writes those bytes down</b>, turning each byte into two characters from
     * {@code 0-9a-f}. The encoding step exists because an HTTP header is text, and raw bytes would
     * not survive the trip intact.
     *
     * <p><b>The key is the UTF-8 bytes of the secret string</b>, not a hex-decoding of it. That is a
     * real fork — some systems treat a hex secret as 32 bytes rather than 64 characters — and it has
     * to be stated in the published documentation, because a subscriber who guesses the other way
     * gets a mismatch on every delivery with nothing to indicate which side is wrong.
     */
    public static String hexSignature(long timestampSeconds, String payload, String secret) {
        // The guard that actually matters. Signing with an empty key is perfectly legal HMAC and
        // returns a normal-looking 64-character signature — so without this, a missing HMAC_SECRET
        // would produce webhooks that look signed, are worthless, and fail only at the subscriber.
        // Failing loudly at the call site turns a silent security hole into an obvious crash.
        if (secret == null || secret.isBlank()) {
            throw new IllegalArgumentException(
                    "HMAC secret must not be null or blank — refusing to sign with an empty key");
        }

        String toSign = signedString(timestampSeconds, payload);

        try {
            Mac mac = Mac.getInstance(ALGORITHM);

            // The charset is pinned explicitly rather than relying on the platform default. Left to
            // the default, the same secret and the same payload would produce different signatures
            // on machines configured with different locales — and the failure would appear only
            // after deploying to a container whose locale differs from the laptop it was built on.
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), ALGORITHM));

            byte[] raw = mac.doFinal(toSign.getBytes(StandardCharsets.UTF_8));

            // HexFormat is JDK 17+, so no hand-rolled byte-to-hex loop. Lowercase by default, which
            // is what the convention expects; the verifier below compares bytes, so a subscriber
            // sending uppercase would be rejected — worth documenting, not worth silently accepting.
            return HexFormat.of().formatHex(raw);

        } catch (GeneralSecurityException e) {
            // Unreachable in practice, and handled only because both exceptions are checked.
            // NoSuchAlgorithmException cannot occur because HmacSHA256 is mandatory in every Java SE
            // implementation; InvalidKeyException cannot occur because HMAC accepts a key of any
            // length and the blank case is already rejected above. If either ever did fire, the JVM
            // itself is broken, so failing fast and loudly is the only sane response.
            throw new IllegalStateException("HMAC-SHA256 is unavailable in this JVM", e);
        }
    }

    /**
     * Builds the full header value: {@code t=<timestamp>,v1=<signature>}.
     *
     * <p>This is the only thing the signing side hands out, and it is worth being precise about what
     * it is not: it does not contain the payload. The payload travels in the HTTP body, untouched.
     * Three pieces of information reach the subscriber — timestamp, signature, body — across two
     * places in the request.
     *
     * <p>The timestamp appears twice, and both are needed. Inside the signed string it is what makes
     * replay detectable; here in the header it is what lets the subscriber rebuild that string,
     * since they have no other way to know which second we used.
     */
    public static String headerValue(long timestampSeconds, String payload, String secret) {
        return "t=" + timestampSeconds + "," + VERSION_TAG + "="
                + hexSignature(timestampSeconds, payload, secret);
    }

    /*
     * DELIBERATELY ABSENT: a signature-comparison method.
     *
     * There is no verify() or matches() here because nothing in this system verifies a signature.
     * Comparison happens on the SUBSCRIBER's side, in their codebase, in whatever language they
     * wrote it in — so it is their standard library that has to supply it, not ours.
     *
     * It still has to be stated in the documentation published alongside this scheme, because the
     * obvious implementation is wrong in a way no test can detect. The receiver must compare the two
     * signatures with a CONSTANT-TIME comparison, never `==` or an ordinary string equality:
     *
     *     Python  hmac.compare_digest(mine, theirs)
     *     Node    crypto.timingSafeEqual(Buffer.from(mine), Buffer.from(theirs))
     *     Go      hmac.Equal([]byte(mine), []byte(theirs))
     *     Java    MessageDigest.isEqual(mine.getBytes(UTF_8), theirs.getBytes(UTF_8))
     *
     * Ordinary string comparison returns at the first differing character, so a guess that is wrong
     * in position 1 is answered measurably sooner than one wrong in position 40. An attacker who can
     * time responses reads that difference and recovers the signature one character at a time —
     * roughly a thousand attempts instead of sixteen to the sixty-fourth. Every language ships a
     * constant-time comparison, which is a fair indication of how well known the problem is.
     */
}
