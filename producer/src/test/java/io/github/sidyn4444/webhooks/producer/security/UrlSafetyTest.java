package io.github.sidyn4444.webhooks.producer.security;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the SSRF gate blocks what it claims to, and only what it claims to.
 *
 * <p>Delivering to a URL somebody else chose is not a feature of this service, it is the service.
 * The vulnerable operation cannot be removed, so the only lever is judging the address a name
 * resolves to rather than the name itself.
 *
 * <p><strong>Every case here is deterministic and offline.</strong> Each URL uses a literal IP
 * address, and every direct address check builds an {@link InetAddress} from a literal, so no DNS
 * query leaves the machine. The roughly eleven checks from the original {@code jshell} harness
 * that resolve real hostnames — {@code example.com}, {@code localtest.me}, {@code *.nip.io} and
 * the NXDOMAIN case — are deliberately <em>not</em> here. They are excellent tests and they
 * belong in a manual run: a build machine with no network, or an ISP that hijacks NXDOMAIN into a
 * search page, would fail them for reasons that have nothing to do with this code. A suite that
 * fails on a train is a suite people learn to ignore.
 *
 * <p>The single most important assertion in this file is {@code aPublicAddressIsAllowed}. Every
 * other test checks that something is refused, and all of them would pass against a method that
 * refused everything.
 */
class UrlSafetyTest {

    private static InetAddress addr(String literal) {
        try {
            // A literal IP never triggers a lookup — InetAddress parses it directly.
            return InetAddress.getByName(literal);
        } catch (UnknownHostException e) {
            throw new AssertionError("literal address failed to parse: " + literal, e);
        }
    }

    // =====================================================================
    // The blocked ranges, checked one address at a time
    // =====================================================================

    @Nested
    @DisplayName("blocked address ranges")
    class BlockedRanges {

        @ParameterizedTest(name = "{0} is blocked")
        @ValueSource(strings = {
                "127.0.0.1",            // loopback — our own machine
                "127.0.0.53",           // loopback is a whole /8, not one address
                "0.0.0.0",              // "this host"
                "10.0.0.5",             // private class A
                "172.16.0.1",           // private class B
                "172.31.255.254",       // the far end of that same block
                "192.168.1.1",          // private class C
                "169.254.169.254",      // THE cloud metadata endpoint
                "169.254.1.1",          // link-local generally
                "100.64.0.1",           // carrier-grade NAT
                "224.0.0.1",            // multicast
                "239.255.255.250"       // multicast, upper end
        })
        void ipv4RangesAreBlocked(String literal) {
            assertThat(UrlSafety.isBlockedAddress(addr(literal))).isTrue();
        }

        @ParameterizedTest(name = "{0} is blocked")
        @ValueSource(strings = {
                "::1",                  // IPv6 loopback
                "fc00::1",              // unique local — the modern private IPv6
                "fd12:3456::1",         // also unique local, different second nibble
                "fe80::1",              // link-local
                "ff02::1"               // multicast
        })
        void ipv6RangesAreBlocked(String literal) {
            assertThat(UrlSafety.isBlockedAddress(addr(literal))).isTrue();
        }

        @Test
        @DisplayName("loopback wearing an IPv6 costume is still loopback")
        void ipv4MappedIpv6IsBlocked() {
            // ::ffff:127.0.0.1 is 127.0.0.1 expressed in IPv6 notation. A check that looked at the
            // text rather than the parsed address would see nothing familiar here.
            assertThat(UrlSafety.isBlockedAddress(addr("::ffff:127.0.0.1"))).isTrue();
            assertThat(UrlSafety.isBlockedAddress(addr("::ffff:10.0.0.5"))).isTrue();
        }

        @Test
        @DisplayName("a null address fails closed")
        void nullAddressIsBlocked() {
            assertThat(UrlSafety.isBlockedAddress(null)).isTrue();
        }
    }

    // =====================================================================
    // Boundaries. These are what prove each range is exactly as wide as
    // claimed, rather than a function that blocks the whole internet.
    // =====================================================================

    @Nested
    @DisplayName("range boundaries, one address either side")
    class Boundaries {

        @Test
        @DisplayName("10.0.0.0/8 starts and stops where it should")
        void privateClassABoundary() {
            assertThat(UrlSafety.isBlockedAddress(addr("9.255.255.255"))).isFalse();
            assertThat(UrlSafety.isBlockedAddress(addr("10.0.0.0"))).isTrue();
            assertThat(UrlSafety.isBlockedAddress(addr("10.255.255.255"))).isTrue();
            assertThat(UrlSafety.isBlockedAddress(addr("11.0.0.0"))).isFalse();
        }

        @Test
        @DisplayName("172.16.0.0/12 covers 172.16-172.31 and nothing either side")
        void privateClassBBoundary() {
            assertThat(UrlSafety.isBlockedAddress(addr("172.15.255.255"))).isFalse();
            assertThat(UrlSafety.isBlockedAddress(addr("172.16.0.0"))).isTrue();
            assertThat(UrlSafety.isBlockedAddress(addr("172.31.255.255"))).isTrue();
            assertThat(UrlSafety.isBlockedAddress(addr("172.32.0.0"))).isFalse();
        }

        @Test
        @DisplayName("192.168.0.0/16 does not spill into 192.167 or 192.169")
        void privateClassCBoundary() {
            assertThat(UrlSafety.isBlockedAddress(addr("192.167.255.255"))).isFalse();
            assertThat(UrlSafety.isBlockedAddress(addr("192.168.0.0"))).isTrue();
            assertThat(UrlSafety.isBlockedAddress(addr("192.168.255.255"))).isTrue();
            assertThat(UrlSafety.isBlockedAddress(addr("192.169.0.0"))).isFalse();
        }

        @Test
        @DisplayName("100.64.0.0/10 covers the CGNAT block and stops at 100.128")
        void carrierGradeNatBoundary() {
            assertThat(UrlSafety.isBlockedAddress(addr("100.63.255.255"))).isFalse();
            assertThat(UrlSafety.isBlockedAddress(addr("100.64.0.0"))).isTrue();
            assertThat(UrlSafety.isBlockedAddress(addr("100.127.255.255"))).isTrue();
            assertThat(UrlSafety.isBlockedAddress(addr("100.128.0.0"))).isFalse();
        }

        @Test
        @DisplayName("fc00::/7 is matched on its top seven bits, not on a guessed prefix")
        void uniqueLocalIpv6Boundary() {
            // This is the range Java's own isSiteLocalAddress() misses: for IPv6 it tests
            // fec0::/10, deprecated in 2004. Modern private IPv6 is fc00::/7, which is fc and fd
            // in the first byte — and nothing either side of that pair.
            assertThat(UrlSafety.isBlockedAddress(addr("fb00::1"))).isFalse();
            assertThat(UrlSafety.isBlockedAddress(addr("fc00::1"))).isTrue();
            assertThat(UrlSafety.isBlockedAddress(addr("fdff::1"))).isTrue();
            assertThat(UrlSafety.isBlockedAddress(addr("fe00::1"))).isFalse();
        }

        @Test
        @DisplayName("169.254.0.0/16 does not spill into 169.253 or 169.255")
        void linkLocalBoundary() {
            assertThat(UrlSafety.isBlockedAddress(addr("169.253.255.255"))).isFalse();
            assertThat(UrlSafety.isBlockedAddress(addr("169.254.0.0"))).isTrue();
            assertThat(UrlSafety.isBlockedAddress(addr("169.254.255.255"))).isTrue();
            assertThat(UrlSafety.isBlockedAddress(addr("169.255.0.0"))).isFalse();
        }
    }

    // =====================================================================
    // 🔴 Anti-vacuity. Without this block the whole file proves nothing.
    // =====================================================================

    @Nested
    @DisplayName("addresses that must be ALLOWED")
    class MustBeAllowed {

        @ParameterizedTest(name = "{0} is allowed")
        @ValueSource(strings = {
                "8.8.8.8",              // Google DNS
                "1.1.1.1",              // Cloudflare DNS
                "93.184.216.34",        // a plain public host
                "203.0.113.5"           // TEST-NET-3, public-shaped
        })
        void publicAddressesAreAllowed(String literal) {
            assertThat(UrlSafety.isBlockedAddress(addr(literal))).isFalse();
        }

        @Test
        @DisplayName("a public URL comes back VALID end to end")
        void aPublicAddressIsAllowed() {
            UrlSafety.Result result = UrlSafety.check("https://8.8.8.8/webhooks");

            assertThat(result.isSafe()).isTrue();
            assertThat(result.verdict()).isEqualTo(UrlVerdict.VALID);
        }
    }

    // =====================================================================
    // Whole-URL checks through the public entry point
    // =====================================================================

    @Nested
    @DisplayName("check(url) end to end")
    class WholeUrls {

        @ParameterizedTest(name = "{0} is refused as a blocked address")
        @ValueSource(strings = {
                "http://127.0.0.1/hook",
                "http://127.0.0.1:8089/hook",
                "http://10.0.0.5/hook",
                "http://192.168.1.1/hook",
                "http://169.254.169.254/latest/meta-data/iam/security-credentials/",
                "http://[::1]/hook",
                "http://[fc00::1]/hook",
                "https://100.64.0.1/hook"
        })
        void hostileUrlsAreBlocked(String url) {
            UrlSafety.Result result = UrlSafety.check(url);

            assertThat(result.isSafe()).isFalse();
            assertThat(result.verdict()).isEqualTo(UrlVerdict.BLOCKED_ADDRESS);
        }

        @Test
        @DisplayName("the userinfo trick does not disguise the real host")
        void userinfoDoesNotHideTheHost() {
            // Everything before the @ is userinfo. A human skimming this reads the wrong half;
            // the host is 127.0.0.1.
            UrlSafety.Result result = UrlSafety.check("http://real-subscriber.com@127.0.0.1/hook");

            assertThat(result.isSafe())
                    .as("a URL whose real host is loopback must be refused however it is dressed up")
                    .isFalse();
        }

        @ParameterizedTest(name = "{0} is refused for its scheme")
        @ValueSource(strings = {
                "file:///etc/passwd",
                "ftp://8.8.8.8/x",
                "gopher://8.8.8.8/x"
        })
        void nonHttpSchemesAreRefused(String url) {
            UrlSafety.Result result = UrlSafety.check(url);

            assertThat(result.isSafe()).isFalse();
            assertThat(result.verdict()).isEqualTo(UrlVerdict.SCHEME_NOT_ALLOWED);
        }

        @Test
        @DisplayName("an uppercase scheme is accepted — schemes are case-insensitive")
        void uppercaseSchemeIsAccepted() {
            assertThat(UrlSafety.check("HTTPS://8.8.8.8/webhooks").isSafe()).isTrue();
        }

        @ParameterizedTest(name = "\"{0}\" is refused as unparseable")
        @ValueSource(strings = {
                "",
                "   ",
                "not a url at all",
                "/webhooks",
                "8.8.8.8/webhooks"
        })
        void malformedInputIsRefused(String url) {
            UrlSafety.Result result = UrlSafety.check(url);

            assertThat(result.isSafe()).isFalse();
            assertThat(result.verdict()).isEqualTo(UrlVerdict.NOT_A_URL);
        }

        @Test
        @DisplayName("null is refused rather than throwing")
        void nullIsRefused() {
            UrlSafety.Result result = UrlSafety.check(null);

            assertThat(result.isSafe()).isFalse();
            assertThat(result.verdict()).isEqualTo(UrlVerdict.NOT_A_URL);
        }
    }

    // =====================================================================
    // 🔴 The two-audience rule: our log gets the address, the caller gets
    // a sentence that names nothing.
    // =====================================================================

    @Nested
    @DisplayName("what the caller is allowed to learn")
    class NoInformationLeak {

        @Test
        @DisplayName("two different blocked addresses produce identical public messages")
        void repliesAreIndistinguishable() {
            String privateNetwork = UrlSafety.check("http://10.0.0.5/hook")
                    .verdict().publicMessage();
            String metadataEndpoint = UrlSafety.check("http://169.254.169.254/latest/meta-data/")
                    .verdict().publicMessage();

            // If these differed, an attacker could submit addresses one at a time and read the
            // replies to map a network they cannot reach — making our own error messages the
            // reconnaissance half of the attack.
            assertThat(privateNetwork).isEqualTo(metadataEndpoint);
        }

        @Test
        @DisplayName("the public message never names an address, but the detail does")
        void detailIsPrivateAndMessageIsVague() {
            UrlSafety.Result result = UrlSafety.check("http://169.254.169.254/latest/meta-data/");

            assertThat(result.verdict().publicMessage())
                    .as("the caller must not be told which address it resolved to")
                    .doesNotContain("169.254")
                    .doesNotContain("10.0.0");

            assertThat(result.detail())
                    .as("our own log keeps the one fact that cannot be recovered later")
                    .contains("169.254.169.254");
        }
    }

    // =====================================================================
    // The shape of the verdict type itself
    // =====================================================================

    @Nested
    @DisplayName("the verdict type")
    class VerdictShape {

        @Test
        @DisplayName("there are exactly five verdicts")
        void fiveVerdictsExist() {
            assertThat(UrlVerdict.values()).containsExactlyInAnyOrder(
                    UrlVerdict.VALID,
                    UrlVerdict.NOT_A_URL,
                    UrlVerdict.SCHEME_NOT_ALLOWED,
                    UrlVerdict.UNRESOLVABLE,
                    UrlVerdict.BLOCKED_ADDRESS);
        }

        @Test
        @DisplayName("VALID carries no message, and every rejection carries one")
        void everyRejectionHasAMessage() {
            assertThat(UrlVerdict.VALID.publicMessage()).isEmpty();

            for (UrlVerdict verdict : UrlVerdict.values()) {
                if (verdict != UrlVerdict.VALID) {
                    assertThat(verdict.publicMessage())
                            .as("%s must have a caller-facing message", verdict)
                            .isNotBlank();
                }
            }
        }

        @Test
        @DisplayName("isSafe() agrees with the verdict in both directions")
        void isSafeAgreesWithVerdict() {
            UrlSafety.Result safe = UrlSafety.check("https://8.8.8.8/hook");
            assertThat(safe.isSafe()).isTrue();
            assertThat(safe.verdict()).isEqualTo(UrlVerdict.VALID);

            UrlSafety.Result unsafe = UrlSafety.check("http://127.0.0.1/hook");
            assertThat(unsafe.isSafe()).isFalse();
            assertThat(unsafe.verdict()).isNotEqualTo(UrlVerdict.VALID);
        }

        @Test
        @DisplayName("neither path ever returns null in place of a verdict")
        void neverReturnsNull() {
            assertThat(UrlSafety.check("https://8.8.8.8/hook")).isNotNull()
                    .extracting(UrlSafety.Result::verdict).isNotNull();
            assertThat(UrlSafety.check("http://127.0.0.1/hook")).isNotNull()
                    .extracting(UrlSafety.Result::verdict).isNotNull();
        }
    }
}
