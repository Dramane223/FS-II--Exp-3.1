package com.example.reliability.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration
public class KafkaConfig {

    public static final String MAIN_TOPIC = "scheduled-posts";
    public static final String DLQ_TOPIC = "scheduled-posts-dlq";

    @Bean
    public NewTopic mainTopic() {
        return TopicBuilder.name(MAIN_TOPIC).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic dlqTopic() {
        return TopicBuilder.name(DLQ_TOPIC).partitions(1).replicas(1).build();
    }

    /**
     * Retry with exponential backoff: 1s -> 2s -> 4s (3 retries),
     * then publish the failed record to the DLQ topic.
     */
    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, String> template) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                template, (record, ex) -> new TopicPartition(DLQ_TOPIC, 0));

        ExponentialBackOff backOff = new ExponentialBackOff(1000L, 2.0);
        backOff.setMaxElapsedTime(7000L); // 1s + 2s + 4s => 3 retries

        return new DefaultErrorHandler(recoverer, backOff);
    }
}