package com.kafka.Kafka.service;

import com.kafka.Kafka.model.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage(String topic, String key, String message) {
        kafkaTemplate.send(topic, key, message);
    }

    public void sendMessage(Message message) {
        kafkaTemplate.send(message.getTopic(), message.getKey(), message.getValue());
    }
}