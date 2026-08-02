package io.github.sidyn4444.webhooks.producer.security;

/**
 * Why a subscriber URL was refused.
 *
 * <p>This enum does not <i>do</i> anything — it is the fixed list of allowed answers to the
 * question "why did we say no". It exists as an enum rather than a string for the same reason
 * {@code DlqReason} does: a rejection reason is a thing you eventually want to <b>count</b>.
 * "How many events did we refuse because someone aimed one at an internal address?" has to be
 * answerable without reading anything, and free text drifts — {@code "bad scheme"} and
 * {@code "scheme not allowed"} stop grouping together the moment two people write the message.
 *
 * <h2>Why each constant carries a deliberately vague public message</h2>
 *
 * <p>Every rejection has two audiences and they must not be told the same thing.
 *
 * <p><b>Our log gets the detail</b> — the scheme that was refused, the hostname, the address it
 * resolved to. That is what makes a support question answerable.
 *
 * <p><b>The caller gets a generic sentence.</b> This is not politeness, it is the same control
 * the rest of the check is: an error message saying "rejected — resolves to 10.0.0.4" turns our
 * 400 response into a free internal-network scanner. An attacker submits addresses one at a time
 * and reads our replies to learn which private ranges are live, which hosts exist, and where the
 * boundaries are. They cannot reach those addresses, but they can make <i>us</i> tell them what
 * is there — which is the reconnaissance half of SSRF, working perfectly, against a validator
 * that is otherwise doing its job.
 *
 * <p>So {@link #BLOCKED_ADDRESS} says only that the destination is not allowed. It never says
 * which range, never repeats the address, and reads identically whether the URL pointed at
 * {@code 127.0.0.1}, at {@code 10.0.0.4}, or at the AWS metadata endpoint. <b>The caller learns
 * that they were refused and learns nothing else.</b>
 *
 * <p>The other three are safe to be specific about, and specificity there is genuinely useful:
 * they describe the caller's <i>own</i> input, and tell them nothing about our network. Someone
 * who typed {@code ftp://} should be told the schemes we accept, because that is a mistake they
 * can fix in one edit.
 */
public enum UrlVerdict {

    /**
     * The URL passed every check and may be accepted.
     *
     * <p>This member exists so that "no problem" is a <b>named value</b> rather than the absence of
     * one. The first version of this type held only the four rejections and signalled success with a
     * {@code null} reason — which works, and quietly makes {@code null} carry meaning. Every caller
     * then has to know that a missing reason means success, and nothing stops someone constructing a
     * result that is both "safe" and carrying a reason.
     *
     * <p>It also makes outcomes uniformly countable: with a named success, "how many URLs did we
     * judge, and how did each one end?" is one grouping over one field.
     *
     * <p>⚠️ The one wart, stated rather than hidden: {@link #publicMessage()} is meaningless here and
     * returns an empty string. There is nothing to tell a caller when nothing went wrong. Reaching
     * for it means the {@code != VALID} check was skipped, which is a bug at the call site.
     */
    VALID(""),

    /**
     * The value could not be parsed as a URL with a host in it at all — empty, malformed, or
     * something like {@code http://} with nothing after it.
     *
     * <p>Safe to describe precisely: it is a statement about what they sent us.
     */
    NOT_A_URL("subscriber_url must be a valid absolute URL"),

    /**
     * The URL parsed, but its scheme is not {@code http} or {@code https}.
     *
     * <p>This is an <b>allowlist</b>, not a blocklist, and the difference is the whole point.
     * Blocking {@code file://}, {@code gopher://} and {@code ftp://} by name means being one
     * entry short the first time a client library supports something nobody thought of —
     * {@code jar:}, {@code data:}, {@code netdoc:}. Permitting exactly two schemes is safe by
     * construction, and the cost is that adding a legitimate third one is a code change.
     *
     * <p>Worth being concrete about why this matters at all, because a scheme check looks like
     * tidiness rather than security: {@code file:///etc/passwd} is a perfectly valid URL, and a
     * client that honours it reads a local file with <b>no network request involved</b>. Every
     * address-based defence in this class is bypassed, because there is no address.
     */
    SCHEME_NOT_ALLOWED("subscriber_url must use http or https"),

    /**
     * The hostname could not be resolved to any IP address.
     *
     * <p>Rejecting here is a deliberate choice and the reasoning is simple: <b>a URL nobody can
     * look up is a URL nobody can deliver to.</b> If a subscriber endpoint is real, it is
     * registered in DNS — so refusing an unresolvable host costs a legitimate caller nothing
     * they were not already going to lose, and it is the only answer available to a security
     * check that works by judging resolved addresses. A host we cannot resolve is a host we
     * cannot judge, and accepting something unjudged onto the queue is storing an attack for
     * later.
     *
     * <p>The honest cost: a transient DNS outage turns legitimate submissions into 400s, and a
     * 400 reads as "your input is wrong" rather than "try again shortly". The caller has to
     * retry. That is the price of failing closed, and it is the right side to fail on.
     */
    UNRESOLVABLE("subscriber_url host could not be resolved"),

    /**
     * The host resolved, and at least one of the addresses it resolved to is one our worker must
     * never be pointed at — loopback, a private range, link-local, or the cloud metadata address.
     *
     * <p><b>Deliberately the vaguest message of the four.</b> See the class javadoc: naming the
     * address here would let an attacker use our own validator to map a network they cannot
     * reach.
     */
    BLOCKED_ADDRESS("subscriber_url is not an allowed destination");

    private final String publicMessage;

    UrlVerdict(String publicMessage) {
        this.publicMessage = publicMessage;
    }

    /**
     * The sentence that may be sent back to the caller.
     *
     * <p>Named {@code publicMessage} rather than {@code message} on purpose — the name is the
     * warning. Anything returned from here crosses the trust boundary and is readable by whoever
     * submitted the event, so it must never be widened to include the detail that goes in our log.
     */
    public String publicMessage() {
        return publicMessage;
    }
}
