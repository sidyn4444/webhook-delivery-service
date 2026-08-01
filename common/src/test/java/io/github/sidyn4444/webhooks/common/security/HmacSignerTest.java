package io.github.sidyn4444.webhooks.common.security;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Proves the signing scheme is correct — and, more importantly, that it is correct <b>the same way
 * everyone else does it</b>.
 *
 * <p>That distinction drives the whole file. A test suite that only checked our own code against
 * itself would pass completely for an implementation that is internally consistent and useless to
 * the outside world: sign twice, get the same answer, assert they match, green. Every subscriber
 * would still reject every delivery, because the only thing proven is that the code agrees with
 * itself.
 *
 * <p>So the anchor test compares against values produced by {@code openssl}, a C implementation
 * written by people who have never seen this class. The four vectors below were generated with:
 *
 * <pre>
 * printf '%s' '1753996800.{"order":42,"amount":50}' \
 *     | openssl dgst -sha256 -hmac 'test-secret-do-not-use-in-production' -hex
 * </pre>
 *
 * <p>{@code printf} rather than {@code echo} on purpose: {@code echo} appends a newline, that
 * newline is part of the message being hashed, and the digest comes out completely different. It is
 * the smallest possible demonstration of why "the exact bytes" is not a figure of speech.
 */
class HmacSignerTest {

    private static final String SECRET = "test-secret-do-not-use-in-production";
    private static final String OTHER_SECRET = "a-different-secret";

    private static final long TIMESTAMP = 1753996800L;
    private static final String PAYLOAD = "{\"order\":42,\"amount\":50}";

    /** openssl, same timestamp, same payload, same secret. */
    private static final String EXPECTED =
            "32a2ded01d1b9a54133cbc62e39756e7fede2a8957b29143983531e5bf50968a";

    /** openssl, a payload containing non-ASCII characters. */
    private static final String UNICODE_PAYLOAD = "{\"name\":\"José\",\"rocket\":\"🚀\"}";
    private static final String EXPECTED_UNICODE =
            "9b4075569757b8860a774946614c0cafe23b9d4ffdb746bba3b8628690173f7a";

    /** openssl, identical payload and secret, timestamp one second later. */
    private static final String EXPECTED_ONE_SECOND_LATER =
            "4e92fd0f5ac98912f2c8486c095db02c08f597e95f7561af23b6464ac68135cb";

    /** openssl, identical timestamp and payload, different secret. */
    private static final String EXPECTED_OTHER_SECRET =
            "263a1a3f4da911133058fe0060e7a0bf3b29ad52e3972de2a280df23b4204b74";

    // ---------------------------------------------------------------------------------------
    // The anchor: do we agree with an implementation that knows nothing about us?
    // ---------------------------------------------------------------------------------------

    @Test
    @DisplayName("the signature matches one computed independently by openssl")
    void agreesWithAnIndependentImplementation() {
        // This single assertion covers more than it looks. To produce this exact string, the code
        // must have used HMAC (not a plain hash), SHA-256 (not SHA-1 or SHA-512), the UTF-8 bytes of
        // the secret as the key (not a hex decoding of it), a dot as the separator, the timestamp
        // before the payload, and lowercase hex output. Any one of those wrong gives 64 completely
        // different characters — there is no partial credit in a hash.
        assertThat(HmacSigner.hexSignature(TIMESTAMP, PAYLOAD, SECRET)).isEqualTo(EXPECTED);
    }

    @Test
    @DisplayName("a non-ASCII payload still matches openssl, so the charset is genuinely pinned")
    void agreesWithAnIndependentImplementationOnNonAsciiPayloads() {
        // With plain ASCII, every charset agrees, so a bug in the encoding is invisible. These
        // characters encode to different bytes under UTF-8 than under ISO-8859-1 or Windows-1252, so
        // this is the only test here that can catch the platform-default-charset mistake — the kind
        // that works on a laptop and breaks inside a container with a different locale.
        assertThat(HmacSigner.hexSignature(TIMESTAMP, UNICODE_PAYLOAD, SECRET))
                .isEqualTo(EXPECTED_UNICODE);
    }

    // ---------------------------------------------------------------------------------------
    // The security properties: what actually has to be true for this to be worth having
    // ---------------------------------------------------------------------------------------

    @Test
    @DisplayName("the timestamp is inside the signed data, so editing it invalidates the signature")
    void theTimestampChangesTheSignature() {
        // The whole replay defence rests on this. If the timestamp were merely sent alongside the
        // signature rather than folded into it, an attacker replaying a captured request would edit
        // it to the current second and the signature would still verify. Asserting against an
        // openssl vector for the later second proves it is genuinely part of the input rather than
        // proving only that two of our own outputs differ.
        String oneSecondLater = HmacSigner.hexSignature(TIMESTAMP + 1, PAYLOAD, SECRET);

        assertThat(oneSecondLater).isEqualTo(EXPECTED_ONE_SECOND_LATER);
        assertThat(oneSecondLater).isNotEqualTo(EXPECTED);
    }

    @Test
    @DisplayName("a different secret produces a different signature")
    void theSecretChangesTheSignature() {
        // This is what separates a signature from a checksum. A plain hash of the payload can be
        // recomputed by anyone, so a tampering attacker just recomputes it; the secret is the only
        // reason a forgery is impossible. If this passed with the secret ignored, the class would be
        // an elaborate way of publishing a checksum.
        assertThat(HmacSigner.hexSignature(TIMESTAMP, PAYLOAD, OTHER_SECRET))
                .isEqualTo(EXPECTED_OTHER_SECRET)
                .isNotEqualTo(EXPECTED);
    }

    @Test
    @DisplayName("one flipped character in the payload changes the whole signature")
    void tamperingWithThePayloadIsDetected() {
        // The scenario, concretely: a payload leaves us saying 50 and arrives saying 5000. The
        // amount differs by one character, and the fingerprints must share nothing recognisable —
        // otherwise a near-miss would tell an attacker they are close, which is exactly the feedback
        // a guessing attack needs.
        String tampered = "{\"order\":42,\"amount\":5000}";

        String signature = HmacSigner.hexSignature(TIMESTAMP, tampered, SECRET);

        assertThat(signature).isNotEqualTo(EXPECTED);
        // Not merely different — different everywhere. Comparing character by character, two
        // unrelated 64-character hex strings agree in about 4 positions by chance; anything above
        // roughly 20 would mean the output was tracking the input rather than diffusing it.
        assertThat(agreeingCharacters(signature, EXPECTED)).isLessThan(20);
    }

    // ---------------------------------------------------------------------------------------
    // The wire format: the part a stranger's server has to reproduce
    // ---------------------------------------------------------------------------------------

    @Test
    @DisplayName("the signed string is timestamp, a dot, then the payload — exactly")
    void signedStringHasTheAgreedShape() {
        // Asserted on the literal string rather than by round-tripping, because this IS the contract.
        // A round-trip test would pass just as happily if both sides silently switched to a colon.
        assertThat(HmacSigner.signedString(TIMESTAMP, PAYLOAD))
                .isEqualTo("1753996800.{\"order\":42,\"amount\":50}");
    }

    @Test
    @DisplayName("the signature is 64 lowercase hex characters")
    void signatureIsLowercaseHex() {
        assertThat(HmacSigner.hexSignature(TIMESTAMP, PAYLOAD, SECRET))
                .hasSize(64)
                .matches("[0-9a-f]{64}");
    }

    @Test
    @DisplayName("the header value carries the timestamp and the signature, and nothing else")
    void headerValueHasTheAgreedShape() {
        String header = HmacSigner.headerValue(TIMESTAMP, PAYLOAD, SECRET);

        assertThat(header).isEqualTo("t=1753996800,v1=" + EXPECTED);

        // The two absences matter more than the presence, and neither is hypothetical.
        // The payload belongs in the HTTP body; duplicating it into a header would mean two copies
        // that can disagree, and would put customer data into somewhere headers routinely get logged.
        assertThat(header).doesNotContain(PAYLOAD);
        // And a signing routine that leaked the key into the message it signs would hand every
        // subscriber — and every proxy in between — the ability to forge deliveries to everyone else.
        assertThat(header).doesNotContain(SECRET);
    }

    @Test
    @DisplayName("signing the same thing twice gives the same answer")
    void isDeterministic() {
        // Weak on its own — an implementation returning a constant would pass — but it rules out one
        // specific real failure: a signer that mixed in anything varying (a random nonce, the current
        // time read internally rather than passed in) would be unverifiable by anyone, and would look
        // fine in every other test here.
        assertThat(HmacSigner.hexSignature(TIMESTAMP, PAYLOAD, SECRET))
                .isEqualTo(HmacSigner.hexSignature(TIMESTAMP, PAYLOAD, SECRET));
    }

    // ---------------------------------------------------------------------------------------
    // The guards: the two ways a caller can silently produce a worthless signature
    // ---------------------------------------------------------------------------------------

    @Test
    @DisplayName("a blank secret is refused rather than signed with")
    void refusesToSignWithABlankSecret() {
        // Signing with an empty key is legal HMAC and returns a perfectly normal-looking 64-character
        // string. So a missing HMAC_SECRET would not fail — it would ship webhooks that look signed
        // and cannot be verified by anyone, and the first symptom would be 401s from every subscriber
        // hours later. All three shapes are tested separately because a null check alone would let
        // an empty environment variable through, which is the likeliest of the three.
        assertThatThrownBy(() -> HmacSigner.hexSignature(TIMESTAMP, PAYLOAD, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("secret");

        assertThatThrownBy(() -> HmacSigner.hexSignature(TIMESTAMP, PAYLOAD, ""))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> HmacSigner.hexSignature(TIMESTAMP, PAYLOAD, "   "))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("a null payload is refused rather than signed as the text \"null\"")
    void refusesANullPayload() {
        // Java's string concatenation does not throw on null — it produces the four characters
        // "null". Without the explicit guard this would sign "1753996800.null", return a valid
        // signature for a message we never sent, and the subscriber would reject a body that does not
        // match it. A crash here beats a signature that is correct for the wrong thing.
        assertThatThrownBy(() -> HmacSigner.signedString(TIMESTAMP, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("payload");
    }

    /** How many positions two equal-length strings agree in. Used to assert diffusion, not equality. */
    private static int agreeingCharacters(String a, String b) {
        int same = 0;
        for (int i = 0; i < Math.min(a.length(), b.length()); i++) {
            if (a.charAt(i) == b.charAt(i)) {
                same++;
            }
        }
        return same;
    }
}
