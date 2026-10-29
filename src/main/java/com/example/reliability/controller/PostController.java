package com.example.reliability.controller;

import com.example.reliability.consumer.DlqConsumer;
import com.example.reliability.model.ScheduledPostEvent;
import com.example.reliability.producer.PostProducer;
import com.example.reliability.service.IdempotencyService;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api")
public class PostController {

    public record PostRequest(String eventId, String postId, String content, String failureMode) {}

    private final PostProducer producer;
    private final IdempotencyService idempotency;
    private final DlqConsumer dlqConsumer;

    public PostController(PostProducer producer, IdempotencyService idempotency, DlqConsumer dlqConsumer) {
        this.producer = producer;
        this.idempotency = idempotency;
        this.dlqConsumer = dlqConsumer;
    }

    @PostMapping("/posts")
    public Map<String, Object> schedule(@RequestBody PostRequest req) throws Exception {
        String eventId = (req.eventId() == null || req.eventId().isBlank())
                ? UUID.randomUUID().toString() : req.eventId();

        ScheduledPostEvent event = new ScheduledPostEvent(
                eventId,
                req.postId() == null ? "post-" + eventId.substring(0, 4) : req.postId(),
                req.content(),
                Instant.now().toString(),
                req.failureMode() == null ? "none" : req.failureMode());

        producer.send(event);
        return Map.of("status", "SENT", "eventId", eventId, "failureMode", event.failureMode());
    }

    @GetMapping("/status")
    public Map<String, Object> status() {
        return Map.of(
                "processedEvents", idempotency.getProcessedEvents(),
                "deadLetterCount", dlqConsumer.getDeadLetters().size(),
                "deadLetters", dlqConsumer.getDeadLetters());
    }
}