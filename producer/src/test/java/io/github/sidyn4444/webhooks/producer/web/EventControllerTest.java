package io.github.sidyn4444.webhooks.producer.web;

import io.github.sidyn4444.webhooks.common.model.Event;
import io.github.sidyn4444.webhooks.producer.queue.QueueService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Slice tests for {@code POST /events} — the producer's only endpoint.
 *
 * <p>The property under test is a PAIR, and the second half is the one that matters: a good event
 * must return 202 <b>and be handed to the queue</b>; a bad event must return 400 <b>and the queue
 * must never be touched</b>. A status code alone proves nothing here — a controller that answers
 * 400 and enqueues anyway passes a status-only assertion, which is exactly the trap 16b's
 * {@code LLEN} reading existed to close. Here that same reading is
 * {@code verify(queueService, never()).enqueue(any())}.
 *
 * <p><b>Nothing in this file touches the network.</b> Every URL is a literal IP address, so
 * {@code UrlSafety}'s {@code InetAddress.getAllByName} parses it rather than asking a resolver —
 * including the accepted case, which uses the public literal {@code 8.8.8.8}. A suite that needs
 * DNS fails on a train, and a suite that fails for reasons unrelated to the code is one people
 * learn to ignore.
 */
@WebMvcTest(EventController.class)
@DisplayName("POST /events")
class EventControllerTest {

    /** A well-formed event id, so nothing is ever refused for the wrong reason. */
    private static final String VALID_ID = "3f2504e0-4f89-11d3-9a0c-0305e82c3301";

    /** Public, routable, and a literal — so it is ACCEPTED and no DNS lookup happens. */
    private static final String SAFE_URL = "https://8.8.8.8/webhooks";

    @Autowired
    private MockMvc mockMvc;

    /**
     * The stand-in for everything downstream of the controller.
     *
     * <p>Without this the test would not start at all: {@code @WebMvcTest} builds a container
     * holding the web layer only, so the real {@code @Service} is not present and the controller's
     * constructor has nothing to be given. This supplies a fake with the same shape whose
     * {@code enqueue} does nothing and records that it was called.
     */
    @MockBean
    private QueueService queueService;

    private String body(String eventId, String subscriberUrl, String payload) {
        return """
                {"event_id":"%s","subscriber_url":"%s","payload":"%s"}
                """.formatted(eventId, subscriberUrl, payload);
    }

    // -----------------------------------------------------------------------------------------
    // The anti-vacuity case. Every other test in this file expects a refusal, and all of them
    // would pass against a controller that refused everything. This one test is what makes the
    // rest mean something — it is the same role the public-address case plays in UrlSafetyTest.
    // -----------------------------------------------------------------------------------------

    @Nested
    @DisplayName("a good event is accepted AND handed to the queue")
    class Accepted {

        @Test
        @DisplayName("202 Accepted, the id echoed back, and exactly one enqueue")
        void goodEventIsAcceptedAndEnqueued() throws Exception {
            mockMvc.perform(post("/events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(body(VALID_ID, SAFE_URL, "hello")))
                    .andExpect(status().isAccepted())
                    .andExpect(jsonPath("$.event_id").value(VALID_ID))
                    .andExpect(jsonPath("$.status").value("accepted"));

            // times(1), not just "was called": a controller enqueuing twice would deliver the
            // webhook twice, and a plain verify() would not notice.
            verify(queueService, times(1)).enqueue(any(Event.class));
        }

        @Test
        @DisplayName("the event handed to the queue is the one that was sent, unaltered")
        void theEnqueuedEventIsTheOneReceived() throws Exception {
            mockMvc.perform(post("/events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(body(VALID_ID, SAFE_URL, "hello")))
                    .andExpect(status().isAccepted());

            // An ArgumentCaptor records the actual argument the mock was called with, so it can be
            // asserted afterwards. Verifying only that enqueue happened would pass if the
            // controller enqueued a blank event, or somebody else's.
            ArgumentCaptor<Event> captor = ArgumentCaptor.forClass(Event.class);
            verify(queueService).enqueue(captor.capture());

            Event enqueued = captor.getValue();
            assertThat(enqueued.eventId()).isEqualTo(VALID_ID);
            assertThat(enqueued.subscriberUrl()).isEqualTo(SAFE_URL);
            assertThat(enqueued.payload()).isEqualTo("hello");
        }

        @Test
        @DisplayName("🔴 if the enqueue FAILS the caller does not get 202 — they get 500")
        void a202IsNeverReturnedWhenTheEnqueueFailed() throws Exception {
            // This is what actually pins "202 means the job is queued." Every other test here uses
            // a mock whose enqueue quietly does nothing, so none of them can tell "it worked" from
            // "it did nothing." Making it throw is the only way to ask the question.
            //
            // Note it is NOT tested with ordered verification. The 202 is this method's RETURN
            // VALUE, not a call on a collaborator — the method cannot return before its body has
            // run, so the ordering is guaranteed by control flow and there is nothing to order.
            // What can fail is the enqueue itself, and that is what this asserts.
            doThrow(new RuntimeException("redis unreachable"))
                    .when(queueService).enqueue(any(Event.class));

            mockMvc.perform(post("/events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(body(VALID_ID, SAFE_URL, "hello")))
                    .andExpect(status().isInternalServerError())
                    .andExpect(jsonPath("$.message").value("An unexpected error occurred"));

            verify(queueService, times(1)).enqueue(any(Event.class));
        }

        @Test
        @DisplayName("a failed enqueue leaks nothing about why")
        void aFailedEnqueueLeaksNoInternals() throws Exception {
            doThrow(new RuntimeException("redis unreachable at 10.0.0.7:6379"))
                    .when(queueService).enqueue(any(Event.class));

            String responseBody = mockMvc.perform(post("/events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(body(VALID_ID, SAFE_URL, "hello")))
                    .andExpect(status().isInternalServerError())
                    .andReturn().getResponse().getContentAsString();

            // The operator gets the stack trace in the log; the caller gets a fixed sentence.
            assertThat(responseBody).doesNotContain("redis", "10.0.0.7", "RuntimeException");
        }

        /*
         * RFC 3986 §3.1: "scheme names are case-insensitive." HTTPS://example.com and
         * https://example.com are the SAME URL, and every browser, curl and HTTP client treats
         * them that way.
         *
         * The @Pattern on Event.subscriberUrl was case-SENSITIVE, so an uppercase scheme was
         * refused with 400 "subscriber_url must start with http:// or https://" — a message that
         * is actively confusing, because the caller did exactly that.
         *
         * Worth being precise about what kind of bug this is: it is an OVER-rejection, not a
         * bypass. The SSRF gate one layer down already lowercases with Locale.ROOT before
         * comparing, so nothing hostile ever got through by changing case. A validator that errs
         * toward refusing is wrong in the safe direction — which is why this was a correctness
         * fix rather than a security one.
         *
         * A mixed-case entry is included deliberately: "HTTPS" alone would also pass a naive fix
         * that just added a second all-caps alternative to the regex.
         */
        @ParameterizedTest(name = "{0} is accepted")
        @ValueSource(strings = {
                "HTTPS://8.8.8.8/webhooks",
                "HTTP://8.8.8.8/webhooks",
                "HtTpS://8.8.8.8/webhooks"
        })
        @DisplayName("🔴 an uppercase or mixed-case scheme is ACCEPTED — schemes are case-insensitive")
        void aCaseInsensitiveSchemeIsAccepted(String url) throws Exception {
            mockMvc.perform(post("/events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(body(VALID_ID, url, "hello")))
                    .andExpect(status().isAccepted());

            // The pair, as everywhere else in this file: accepted AND actually queued. A 202 that
            // enqueued nothing would be a worse bug than the 400 this replaces.
            verify(queueService, times(1)).enqueue(any(Event.class));
        }

        @Test
        @DisplayName("a 2xx-only check would miss it: 202 exactly, not 200 or 201")
        void theStatusIsPrecisely202() throws Exception {
            mockMvc.perform(post("/events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(body(VALID_ID, SAFE_URL, "hello")))
                    .andExpect(status().is(202));
        }
    }

    // -----------------------------------------------------------------------------------------
    // Layer 1: @Valid. These fail BEFORE the controller method body is entered, so UrlSafety is
    // never consulted. Kept separate from the SSRF tests deliberately — see WhichLayerRefused.
    // -----------------------------------------------------------------------------------------

    @Nested
    @DisplayName("rejected by @Valid, before the method body runs")
    class RejectedByValidation {

        @Test
        @DisplayName("an event id that is not a UUID is a 400 and never enqueued")
        void badEventId() throws Exception {
            mockMvc.perform(post("/events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(body("not-a-uuid", SAFE_URL, "hello")))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.field_errors.event_id").value("event_id must be a UUID"));

            verify(queueService, never()).enqueue(any());
        }

        @Test
        @DisplayName("a blank payload is a 400 and never enqueued")
        void blankPayload() throws Exception {
            mockMvc.perform(post("/events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(body(VALID_ID, SAFE_URL, "")))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.field_errors.payload").exists());

            verify(queueService, never()).enqueue(any());
        }

        @Test
        @DisplayName("a url with no scheme is a 400 and never enqueued")
        void urlWithoutScheme() throws Exception {
            mockMvc.perform(post("/events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(body(VALID_ID, "8.8.8.8/webhooks", "hello")))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.field_errors.subscriber_url")
                            .value("subscriber_url must start with http:// or https://"));

            verify(queueService, never()).enqueue(any());
        }

        @Test
        @DisplayName("three bad fields at once are ALL reported, not just the first")
        void everyBrokenFieldIsReported() throws Exception {
            mockMvc.perform(post("/events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(body("nope", "ftp://8.8.8.8/x", "")))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.field_errors.event_id").exists())
                    .andExpect(jsonPath("$.field_errors.subscriber_url").exists())
                    .andExpect(jsonPath("$.field_errors.payload").exists());

            verify(queueService, never()).enqueue(any());
        }

        @Test
        @DisplayName("a body that is not JSON at all is a clean 400, not a stack trace")
        void malformedJson() throws Exception {
            mockMvc.perform(post("/events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("{\"event_id\": "))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.message")
                            .value("Malformed or unreadable JSON request body"));

            verify(queueService, never()).enqueue(any());
        }
    }

    // -----------------------------------------------------------------------------------------
    // Layer 2: the SSRF gate. Every URL below passes @Valid — well-formed, http/https — so the
    // ONLY thing that can refuse them is UrlSafety. That is what makes these tests about SSRF.
    // -----------------------------------------------------------------------------------------

    @Nested
    @DisplayName("rejected by the SSRF gate, inside the method body")
    class RejectedBySsrfGate {

        @ParameterizedTest(name = "{0} is refused and never enqueued")
        @ValueSource(strings = {
                "http://169.254.169.254/latest/meta-data/",  // AWS credentials endpoint
                "http://127.0.0.1/hook",                     // loopback
                "http://10.0.0.5/hook",                      // private range
                "http://192.168.1.10/hook",                  // private range
                "http://172.16.0.9/hook",                    // private range
                "http://0.0.0.0/hook",                       // any-local
                "http://100.64.0.1/hook",                    // carrier-grade NAT
                "http://[fc00::1]/hook",                     // private IPv6 the JDK misses
                "http://[::1]/hook",                         // IPv6 loopback
                "http://real-subscriber.com@127.0.0.1/hook"  // userinfo hiding the real host
        })
        void hostileUrlsAreRefused(String hostileUrl) throws Exception {
            mockMvc.perform(post("/events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(body(VALID_ID, hostileUrl, "hello")))
                    .andExpect(status().isBadRequest());

            verify(queueService, never()).enqueue(any());
        }

        @Test
        @DisplayName("the 400 names no address — the error must not become a network scanner")
        void theRejectionLeaksNothing() throws Exception {
            String responseBody = mockMvc.perform(post("/events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(body(VALID_ID, "http://169.254.169.254/latest/meta-data/", "hello")))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.field_errors.subscriber_url")
                            .value("subscriber_url is not an allowed destination"))
                    .andReturn()
                    .getResponse()
                    .getContentAsString();

            // Absence, asserted on the whole response rather than one field — the address must not
            // appear anywhere, including in a message somebody adds later.
            assertThat(responseBody).doesNotContain("169.254");
        }

        @Test
        @DisplayName("two different hostile URLs get a byte-identical reply")
        void everyRejectionReadsTheSame() throws Exception {
            String metadata = refusalMessageFor("http://169.254.169.254/latest/meta-data/");
            String privateNet = refusalMessageFor("http://10.0.0.5/hook");

            // If these ever differed, an attacker could submit addresses one at a time and read the
            // replies to map a network they cannot reach.
            assertThat(metadata).isEqualTo(privateNet);
        }

        private String refusalMessageFor(String url) throws Exception {
            return mockMvc.perform(post("/events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(body(VALID_ID, url, "hello")))
                    .andExpect(status().isBadRequest())
                    .andReturn()
                    .getResponse()
                    .getContentAsString()
                    .replaceAll("\"timestamp\":\"[^\"]*\"", "");  // the clock differs between calls
        }
    }

    // -----------------------------------------------------------------------------------------
    // The 16b lesson, promoted into a permanent test. Three event ids in that run were generated
    // as "2222222" + i, which is NINE hex characters once i reaches 10 — so @Valid refused them on
    // event_id and the SSRF gate never ran. All three still returned 400, and a status-only
    // assertion counted them as evidence for a code path that never executed.
    // -----------------------------------------------------------------------------------------

    @Nested
    @DisplayName("which layer refused it — not merely that something did")
    class WhichLayerRefused {

        @Test
        @DisplayName("a hostile URL with a BAD id is refused by @Valid, not by the SSRF gate")
        void aBadIdMasksTheUrlCheck() throws Exception {
            mockMvc.perform(post("/events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(body("22222222-2222-2222-2222-22222222222", // 35 chars, one short
                                    "http://169.254.169.254/latest/meta-data/", "hello")))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.field_errors.event_id").exists())
                    // The SSRF gate never ran, so its message must be absent. This is the
                    // assertion that tells the two 400s apart.
                    .andExpect(jsonPath("$.field_errors.subscriber_url").doesNotExist());

            verify(queueService, never()).enqueue(any());
        }

        @Test
        @DisplayName("the same URL with a GOOD id is refused by the SSRF gate")
        void aGoodIdLetsTheUrlCheckRun() throws Exception {
            mockMvc.perform(post("/events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(body(VALID_ID, "http://169.254.169.254/latest/meta-data/", "hello")))
                    .andExpect(status().isBadRequest())
                    .andExpect(jsonPath("$.field_errors.event_id").doesNotExist())
                    .andExpect(jsonPath("$.field_errors.subscriber_url")
                            .value("subscriber_url is not an allowed destination"));

            verify(queueService, never()).enqueue(any());
        }
    }

    // -----------------------------------------------------------------------------------------
    // Spring's own handlers, which 8d nearly broke: a bare @ExceptionHandler(Exception.class)
    // would have swallowed the 405 and turned it into a 500.
    // -----------------------------------------------------------------------------------------

    @Nested
    @DisplayName("the wrong request shape reaches the right handler")
    class FrameworkHandlers {

        @Test
        @DisplayName("GET /events is 405, not 500 and not 404")
        void wrongMethodIs405() throws Exception {
            mockMvc.perform(get("/events"))
                    .andExpect(status().isMethodNotAllowed());

            verify(queueService, never()).enqueue(any());
        }

        @Test
        @DisplayName("an unknown path is 404 and never enqueued")
        void unknownPathIs404() throws Exception {
            mockMvc.perform(post("/not-events")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(body(VALID_ID, SAFE_URL, "hello")))
                    .andExpect(status().isNotFound());

            verify(queueService, never()).enqueue(any());
        }
    }
}
