package com.kafka.Kafka.controller;

import com.kafka.Kafka.model.Message;
import com.kafka.Kafka.service.KafkaProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/kafka")
public class KafkaController {

    @Autowired
    private KafkaProducerService producerService;

    @PostMapping("/send")
    public ResponseEntity<String> sendMessage(@RequestBody Message message) {
        try {
            producerService.sendMessage(message);
            return ResponseEntity.ok("Message sent successfully");
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Error sending message");
        }
    }

    @PostMapping("/send/{topic}")
    public ResponseEntity<String> sendToTopic(
            @PathVariable String topic,
            @RequestParam String message,
            @RequestParam(required = false) String key) {

        producerService.sendMessage(topic, key != null ? key : "default-key", message);
        return ResponseEntity.ok("Message sent to topic: " + topic);
    }
}