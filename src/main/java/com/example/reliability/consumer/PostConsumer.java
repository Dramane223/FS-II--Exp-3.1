package com.example.reliability.consumer;

import com.example.reliability.config.KafkaConfig;
import com.example.reliability.model.ScheduledPostEvent;
import com.example.reliability.service.IdempotencyService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class PostConsumer {

    private static final Logger log = LoggerFactory.getLogger(PostConsumer.class);

    private final IdempotencyService idempotency;
    private final ObjectMapper mapper;
    private final Map<String, AtomicInteger> attempts = new ConcurrentHashMap<>();

    public PostConsumer(IdempotencyService idempotency, ObjectMapper mapper) {
        this.idempotency = idempotency;
        this.mapper = mapper;
    }

    @KafkaListener(topics = KafkaConfig.MAIN_TOPIC)
    public void consume(String payload) throws Exception {
        ScheduledPostEvent event = mapper.readValue(payload, ScheduledPostEvent.class);

        // Idempotency check: skip events that were already processed successfully
        if (idempotency.isProcessed(event.eventId())) {
            log.warn("[CONSUMER] DUPLICATE event {} detected -> skipping", event.eventId());
            return;
        }

        int attempt = attempts.computeIfAbsent(event.eventId(), k -> new AtomicInteger()).incrementAndGet();
        log.info("[CONSUMER] Processing event {} (attempt {}, mode={})",
                event.eventId(), attempt, event.failureMode());

        switch (event.failureMode() == null ? "none" : event.failureMode()) {
            case "permanent" -> throw new RuntimeException("Permanent failure for event " + event.eventId());
            case "transient" -> {
                if (attempt < 3) {
                    throw new RuntimeException("Transient failure for event " + event.eventId());
                }
            }
            default -> { /* success */ }
        }

        // Business logic: publish the scheduled post
        log.info("[CONSUMER] SUCCESS: published post '{}' ({})", event.content(), event.postId());

        idempotency.markProcessed(event.eventId());
        attempts.remove(event.eventId());
    }
}