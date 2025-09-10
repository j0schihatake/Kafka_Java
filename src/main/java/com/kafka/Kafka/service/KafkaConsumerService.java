package com.kafka.Kafka.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class KafkaConsumerService {

    @KafkaListener(topics = "test-topic", groupId = "my-group")
    public void consume(String message) {
        log.info("Received message: {}", message);
    }

    @KafkaListener(topics = "user-events", groupId = "my-group")
    public void consumeUserEvent(String message) {
        log.info("User event received: {}", message);
    }
}