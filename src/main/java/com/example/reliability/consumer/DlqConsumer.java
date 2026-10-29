package com.example.reliability.consumer;

import com.example.reliability.config.KafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
public class DlqConsumer {

    private static final Logger log = LoggerFactory.getLogger(DlqConsumer.class);

    private final List<String> deadLetters = new CopyOnWriteArrayList<>();

    @KafkaListener(topics = KafkaConfig.DLQ_TOPIC, groupId = "dlq-monitor")
    public void consumeDlq(String payload,
                           @Header(KafkaHeaders.DLT_EXCEPTION_MESSAGE) String reason) {
        log.error("[DLQ] Message moved to dead-letter queue. Reason: {} | Payload: {}", reason, payload);
        deadLetters.add(payload);
    }

    public List<String> getDeadLetters() {
        return deadLetters;
    }
}