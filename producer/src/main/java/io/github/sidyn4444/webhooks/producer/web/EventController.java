package io.github.sidyn4444.webhooks.producer.web;

import io.github.sidyn4444.webhooks.common.model.Event;
import io.github.sidyn4444.webhooks.producer.queue.QueueService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * The producer's only HTTP endpoint: {@code POST /events}.
 *
 * <p>Its job is to accept an event, confirm it is well-formed, and hand it off for delivery —
 * then return immediately. It does not attempt the delivery itself. Delivery is slow and
 * failure-prone (it depends on a third party's server), so making the caller wait for it would
 * tie the caller's fate to a subscriber they have nothing to do with. Instead the event is
 * enqueued and a worker delivers it out of band. This is the producer–consumer decoupling from
 * the design notes made concrete.
 */
@RestController
public class EventController {

    private final QueueService queueService;

    /**
     * The {@link QueueService} is handed in through the constructor rather than created here.
     * This is constructor injection: Spring sees the single constructor, finds the managed
     * {@code QueueService} bean, and passes it in. The controller never calls {@code new} on its
     * collaborator, which is what lets it be tested with a stand-in and keeps the wiring out of
     * the class itself.
     */
    public EventController(QueueService queueService) {
        this.queueService = queueService;
    }

    /**
     * Receives an event, validates it, and acknowledges acceptance.
     *
     * <p><b>{@code @RequestBody}</b> tells Spring to take the HTTP request body and deserialize
     * the JSON into an {@link Event}, using the field-name mapping declared on the record.
     *
     * <p><b>{@code @Valid}</b> is the trigger that runs the validation engine over the parsed
     * object. The {@code @NotBlank} and {@code @Pattern} rules on {@code Event} are inert
     * annotations until something invokes a validator; this is that something. If any rule
     * fails, Spring never enters this method — it raises a
     * {@code MethodArgumentNotValidException}, which becomes a 400. Malformed input therefore
     * cannot reach the queue.
     *
     * <p><b>The response is 202 Accepted, not 200 or 201.</b> 202 means "I have taken
     * responsibility for this, but the work is not finished." That is exactly the truth here:
     * the event has been accepted for delivery, but delivery happens later in a worker. 200
     * would imply the work is done; 201 would imply a resource now exists at some URL the
     * caller can fetch. Both would be promises this endpoint cannot keep. The status code is a
     * contract about what actually happened, and for a queue-backed accept-now/deliver-later
     * system, 202 is the honest one.
     *
     * <p>Only {@code event_id} is logged, never the payload. The payload may contain personal
     * data and is treated as opaque throughout the system (data minimization, per the design
     * notes). The event id is a non-sensitive UUID and is exactly what you need to trace a
     * request through the logs.
     */
    @PostMapping("/events")
    public ResponseEntity<EventResponse> receive(@Valid @RequestBody Event event) {
        queueService.enqueue(event);
        return ResponseEntity
                .status(HttpStatus.ACCEPTED)
                .body(new EventResponse(event.eventId(), "accepted"));
    }
}
