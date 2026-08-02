package io.github.sidyn4444.webhooks.producer.security;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Locale;

/**
 * Decides whether a subscriber URL is safe for the worker to POST to.
 *
 * <p>This is the SSRF control (notes 3b). The threat is not abstract: delivering to a URL somebody
 * else chose is <b>the entire product</b>, so the vulnerability class is not an incidental risk
 * here, it is the core function. Our worker runs inside the private network with reach to Redis,
 * to Postgres, and — in production — to the AWS metadata endpoint at {@code 169.254.169.254} that
 * hands out the machine's credentials to anything on the box that asks. A caller who supplies
 * {@code subscriber_url} chooses where that trusted worker points.
 *
 * <p>Worth separating two things that are easy to merge. The worker can <b>reach</b> internal
 * addresses because of where it sits, not because of what it is permitted to do — no permission
 * system is consulted when a program opens a connection on its own network. IAM governs calls to
 * AWS's own APIs, which is a different layer entirely. <b>Network position decides what an SSRF
 * attack can reach; the IAM role decides how much a stolen credential is worth.</b>
 *
 * <h2>The one idea the whole class rests on</h2>
 *
 * <p><b>Judge the resolved IP address, never the hostname.</b> A hostname is a name the attacker
 * chose. They register {@code totally-normal-webhooks.com} for the price of a coffee, point its
 * DNS record at {@code 169.254.169.254}, and submit it. Nothing about the name is suspicious,
 * because nobody picks a suspicious name. DNS does not check that an address you publish is
 * yours, is reachable, or is sane — you may point a domain you own at any number you like.
 *
 * <p>So a check that inspects the string — {@code host.contains("localhost")},
 * {@code host.startsWith("169.254")} — passes every real attack and blocks only honest typos.
 * The address is the truth; the name is a label.
 *
 * <h2>Why this is not a Spring bean</h2>
 *
 * <p>Static methods, no state, no annotations — same reasoning as {@code HmacSigner}. It is a
 * decision function, not a service: nothing to configure, nothing to cache, and a test can
 * exercise it with no application context. {@link #isBlockedAddress(InetAddress)} is public
 * specifically so the part that involves no DNS can be asserted exactly, which mirrors the split
 * in {@code BackoffCalculator} — the deterministic half is testable in a way the impure half is
 * not.
 *
 * <h2>What this deliberately does not defend against</h2>
 *
 * <p><b>DNS rebinding</b>, and it is a real exposure rather than a theoretical one. This runs at
 * the front door; the worker connects seconds or minutes later and resolves the name again. An
 * attacker controlling their own DNS can answer with a harmless public address the first time and
 * a private one the second — check and use are two different moments, and only the first is ours.
 * The proper fix is to resolve once here and connect to that pinned address at delivery time,
 * which needs the worker to carry the address rather than the name. Deliberately out of scope.
 *
 * <p><b>Redirects.</b> The same hole reached differently: a subscriber answers our POST with a
 * {@code 302} pointing at an internal address. Worth knowing that following a redirect commonly
 * converts POST into GET, which is exactly the verb the metadata endpoint serves credentials on —
 * so "we only POST" is not a defence, because the verb is not ours to keep.
 *
 * <p><b>An allowlist would be stronger than this blocklist.</b> Permitting only pre-registered
 * subscriber domains cannot be one range short; a list of blocked ranges always can. It needs a
 * subscriber registry, which this project does not have.
 */
public final class UrlSafety {

    /** Never instantiated: every method is static and the class holds no state. */
    private UrlSafety() {
        throw new AssertionError("UrlSafety is a utility class and is never instantiated");
    }

    /**
     * The answer: what the verdict was, and why.
     *
     * <p><b>A method can only return one value</b>, and this check produces two facts that are only
     * meaningful together — so they travel together in one object rather than across two calls. A
     * second method that re-derived the explanation would perform the DNS lookup twice, and the two
     * answers could disagree if a record changed in between.
     *
     * <p>The two fields exist because <b>the reason has to reach two audiences at two levels of
     * detail</b>. {@code detail} is ours: the scheme, the host, the address it resolved to. The
     * caller gets only {@link UrlVerdict#publicMessage()}, which never names an address. One string
     * for both would force a choice between a useless log and a 400 response that maps our private
     * network for whoever is probing it.
     *
     * <p>🔴 <b>And the resolved address is the field that cannot be recovered later.</b> For three of
     * the four rejections the caller's own URL already says everything; for
     * {@link UrlVerdict#BLOCKED_ADDRESS} it does not. Re-resolving the name next week asks the
     * attacker's own DNS server what it feels like saying today, and a one-second TTL is a
     * configuration setting rather than an exotic attack. For a security control the log is
     * evidence, and evidence has to be captured at the moment it happens.
     *
     * @param verdict what was decided — {@link UrlVerdict#VALID} when the URL may be accepted
     * @param detail  the full internal explanation — <b>never send this to a caller</b>
     */
    public record Result(UrlVerdict verdict, String detail) {

        /*
         * The three members below split cleanly into two kinds, and it is worth naming which is
         * which before reading them:
         *
         *   safe() and rejected()  --  the two ways a Result is CREATED. Called by check() only.
         *   isSafe()               --  a question you ASK an existing Result. Called by the caller.
         *
         * So the first two are used on the way out of this class, and the third is used on the way
         * in to whoever received one. Nothing calls all three.
         */

        /**
         * Asks an existing result whether the URL may be accepted.
         *
         * <p>This creates nothing. It is the question {@code EventController} puts to the result it
         * was handed — {@code if (!urlCheck.isSafe()) throw …} — and it is the only member of this
         * record that callers outside this class ever need.
         *
         * <p>It exists rather than making every caller write {@code verdict() == UrlVerdict.VALID}
         * because that comparison is the kind of thing that gets written backwards once. One named
         * method means the definition of "safe" lives in exactly one place, and if it ever grows
         * more nuance than a single equality, no call site changes.
         */
        public boolean isSafe() {
            return verdict == UrlVerdict.VALID;
        }

        /**
         * Builds the "nothing wrong" result.
         *
         * <p>A <b>static factory method</b>, not a constructor — the constructor is the one the
         * {@code record} declaration generates, and this calls it. The distinction is worth keeping
         * straight: {@code new Result(...)} is the constructor; {@code Result.safe()} is a named
         * shortcut that fills in the arguments for you.
         *
         * <p>Which is exactly why it is named: the success case cannot be built wrong. There is no
         * route by which a caller constructs a "safe" result that also carries a rejection reason,
         * because they never supply the verdict — this does.
         */
        static Result safe() {
            return new Result(UrlVerdict.VALID, "ok");
        }

        /**
         * Builds a refusal, carrying why.
         *
         * <p>Also a static factory method rather than a constructor. It takes the same two arguments
         * the constructor does and adds nothing — its whole value is that the call site reads as a
         * sentence: {@code Result.rejected(BLOCKED_ADDRESS, "…")} says what is happening, where
         * {@code new Result(BLOCKED_ADDRESS, "…")} leaves a reader to infer it from the arguments.
         *
         * <p>Both factories are package-private on purpose. Only {@link UrlSafety#check} builds
         * results; everyone else receives one and asks it questions.
         */
        static Result rejected(UrlVerdict verdict, String detail) {
            return new Result(verdict, detail);
        }
    }

    /**
     * Runs the full check: parse, allow only http/https, resolve the host to <b>every</b> address
     * it maps to, and refuse if any of them is one we must never be pointed at.
     *
     * <p>⚠️ <b>This method performs a DNS lookup and therefore blocks.</b> It is called on the
     * producer's request thread, so a slow or unresponsive resolver delays an HTTP request that a
     * caller is waiting on. There is no timeout parameter on {@code InetAddress}; bounding it
     * properly means resolving on a separate executor with a deadline. Accepted here because the
     * JVM caches successful lookups, so the cost falls on the first event for a given host rather
     * than on every event — but it is a real cost and it belongs in the honest list.
     */
    public static Result check(String url) {
        if (url == null || url.isBlank()) {
            return Result.rejected(UrlVerdict.NOT_A_URL, "url was null or blank");
        }

        // java.net.URI, not java.net.URL. URL.equals() and URL.hashCode() perform DNS lookups as a
        // side effect of comparing two objects, which is astonishing behaviour to inherit inside a
        // security control. URI is a pure parser: it resolves nothing and calls nothing.
        URI uri;
        try {
            uri = new URI(url.trim());
        } catch (URISyntaxException e) {
            return Result.rejected(UrlVerdict.NOT_A_URL, "unparseable url: " + e.getMessage());
        }

        // Absolute means it carries its own scheme. "/webhooks" parses perfectly well as a URI and
        // has no scheme and no host, so without this it would fall through to a confusing later
        // failure rather than an accurate one.
        if (!uri.isAbsolute()) {
            return Result.rejected(UrlVerdict.NOT_A_URL, "url is not absolute (no scheme)");
        }

        // An ALLOWLIST of two, deliberately. See UrlVerdict.SCHEME_NOT_ALLOWED: a blocklist of
        // dangerous schemes is always one entry short of the next one somebody's client supports.
        // Lowercased with Locale.ROOT because Turkish locales lowercase 'I' to a dotless 'ı', so
        // the default-locale version of this line rejects "HTTP://..." on a Turkish machine only.
        String scheme = uri.getScheme().toLowerCase(Locale.ROOT);
        if (!scheme.equals("http") && !scheme.equals("https")) {
            return Result.rejected(UrlVerdict.SCHEME_NOT_ALLOWED, "scheme was " + scheme);
        }

        // getHost() rather than getAuthority(). The authority is everything between the // and the
        // path — userinfo, host AND port — and only the host decides where the packet goes.
        //
        // The URL worth knowing about here is http://real-subscriber.com@127.0.0.1/hook. It is a
        // valid URL whose host is 127.0.0.1: everything before the @ is userinfo, so a human
        // skimming the string reads the wrong half. getHost() returns 127.0.0.1, which is the half
        // that matters.
        //
        // Measured rather than assumed, because the obvious claim here is too strong: swapping this
        // for getAuthority() still REJECTS that URL — the resolver cannot look up
        // "real-subscriber.com@127.0.0.1", so it comes back UNRESOLVABLE. It fails closed either
        // way. What getAuthority() actually costs is every legitimate URL carrying a port:
        // "localhost:8080" is not a hostname either, so https://subscriber.example.com:8443/hook
        // would be refused as unresolvable. So this line is about being right for the right reason,
        // and about not rejecting honest callers — not about closing a hole.
        //
        // It returns null for a host URI considers malformed (underscores, for instance). Treating
        // that as NOT_A_URL fails closed: we would rather refuse an unusual name we cannot parse
        // than hand an unparsed one to the resolver.
        String host = uri.getHost();
        if (host == null || host.isBlank()) {
            return Result.rejected(UrlVerdict.NOT_A_URL, "url has no parseable host");
        }

        // getAllByName, not getByName. A hostname may legitimately publish several addresses, and
        // getByName hands back one of them. An attacker publishing a harmless public address next
        // to 10.0.0.5 turns a single-address check into a coin flip that they get to re-toss on
        // every submission. Checking all of them is the difference between a control and a
        // probability.
        InetAddress[] addresses;
        try {
            addresses = InetAddress.getAllByName(host);
        } catch (UnknownHostException e) {
            return Result.rejected(UrlVerdict.UNRESOLVABLE, "could not resolve host " + host);
        }

        if (addresses.length == 0) {
            // Not reachable in practice — getAllByName throws rather than returning empty — but an
            // empty array would silently skip the loop below and return SAFE, which is the worst
            // possible failure mode for a check like this. Guarded because the cost of being wrong
            // is asymmetric.
            return Result.rejected(UrlVerdict.UNRESOLVABLE, "host " + host + " resolved to nothing");
        }

        for (InetAddress address : addresses) {
            if (isBlockedAddress(address)) {
                // The address goes in the DETAIL, which is ours. The caller receives only
                // UrlVerdict.BLOCKED_ADDRESS.publicMessage(), which names nothing.
                return Result.rejected(
                        UrlVerdict.BLOCKED_ADDRESS,
                        "host " + host + " resolves to blocked address " + address.getHostAddress());
            }
        }

        return Result.safe();
    }

    /**
     * Is this address one the worker must never be pointed at?
     *
     * <p>Public because it is the pure half of the check — no DNS, no network, one address in and
     * a boolean out — so it can be asserted against literal addresses in a way {@link #check} can
     * never be. Splitting the deterministic part out from the part that depends on the outside
     * world is the same move {@code BackoffCalculator} makes for the same reason.
     *
     * <p>Most of the work is done by methods already on {@link InetAddress}, which is worth
     * preferring over hand-rolled byte arithmetic: these are the JDK's own definitions of the
     * ranges, they handle IPv4 and IPv6 without a branch, and an IPv4-mapped IPv6 address such as
     * {@code ::ffff:127.0.0.1} has already been reduced to an {@code Inet4Address} by the time it
     * arrives here, so the loopback check catches it rather than being sidestepped by the notation.
     *
     * <p>Two ranges the JDK does <b>not</b> cover are checked by hand below, and they are not
     * padding — {@code fc00::/7} is the ordinary way private IPv6 networks are addressed today.
     */
    public static boolean isBlockedAddress(InetAddress address) {
        if (address == null) {
            return true;
        }

        // 0.0.0.0 and :: — "this host, any interface". Connecting to it reaches the local machine,
        // so it is loopback wearing a different notation.
        if (address.isAnyLocalAddress()) {
            return true;
        }

        // 127.0.0.0/8 and ::1 — our own admin ports, and anything else listening locally.
        if (address.isLoopbackAddress()) {
            return true;
        }

        // 169.254.0.0/16 and fe80::/10. THIS IS THE ONE THAT MATTERS MOST: the AWS metadata
        // endpoint 169.254.169.254 lives in this range, and it hands out the machine's credentials
        // to anything on the box that asks it, with no authentication of any kind.
        if (address.isLinkLocalAddress()) {
            return true;
        }

        // 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16 — the private ranges, which is where Redis,
        // Postgres and every internal service in the VPC actually live.
        if (address.isSiteLocalAddress()) {
            return true;
        }

        // 224.0.0.0/4 and ff00::/8. Not on the original blocklist and added deliberately: a POST to
        // a multicast group is one request delivered to every listener on the segment, which is a
        // free amplifier for the denial-of-service pivot rather than a delivery.
        if (address.isMulticastAddress()) {
            return true;
        }

        byte[] bytes = address.getAddress();

        // fc00::/7 — IPv6 unique-local, the modern equivalent of 10.0.0.0/8.
        //
        // isSiteLocalAddress() does NOT cover it. For IPv6 that method tests fec0::/10, a scheme
        // deprecated in 2004 and replaced by this one — so relying on the JDK method alone leaves
        // every private IPv6 network wide open while looking thoroughly handled. The test is the
        // top seven bits: 0xFC or 0xFD in the first byte.
        if (bytes.length == 16 && (bytes[0] & 0xFE) == 0xFC) {
            return true;
        }

        // 100.64.0.0/10 — carrier-grade NAT. Also not on the original list. It is shared address
        // space used inside ISP and cloud networks, and some Kubernetes setups hand it to pods, so
        // it can be genuinely internal while looking like an ordinary public address.
        if (bytes.length == 4 && (bytes[0] & 0xFF) == 100 && (bytes[1] & 0xC0) == 0x40) {
            return true;
        }

        return false;
    }
}
