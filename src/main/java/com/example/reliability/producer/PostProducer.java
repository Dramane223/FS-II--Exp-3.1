package com.example.reliability.producer;

import com.example.reliability.config.KafkaConfig;
import com.example.reliability.model.ScheduledPostEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class PostProducer {

    private static final Logger log = LoggerFactory.getLogger(PostProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper mapper;

    public PostProducer(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper mapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.mapper = mapper;
    }

    public void send(ScheduledPostEvent event) throws JsonProcessingException {
        String json = mapper.writeValueAsString(event);
        kafkaTemplate.send(KafkaConfig.MAIN_TOPIC, event.eventId(), json);
        log.info("[PRODUCER] Sent event {} (mode={})", event.eventId(), event.failureMode());
    }
}