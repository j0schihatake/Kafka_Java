package com.kafka.Kafka.controller;

import com.kafka.Kafka.model.Order;
import com.kafka.Kafka.model.Payment;
import com.kafka.Kafka.model.UserEvent;
import com.kafka.Kafka.service.DemoKafkaService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api/kafka-demo")
@RequiredArgsConstructor
public class DemoController {

    private final DemoKafkaService kafkaService;

    @PostMapping("/user-event")
    public CompletableFuture<ResponseEntity<String>> sendUserEvent(@RequestBody UserEvent event) {
        return kafkaService.sendUserEvent(event)
                .thenApply(result -> ResponseEntity.ok("User event sent successfully"))
                .exceptionally(ex -> ResponseEntity.status(500)
                        .body("Failed to send user event: " + ex.getMessage()));
    }

    @PostMapping("/order")
    public ResponseEntity<String> sendOrder(@RequestBody Order order) {
        kafkaService.sendOrder(order);
        return ResponseEntity.ok("Order sent successfully");
    }

    @PostMapping("/payment")
    public ResponseEntity<String> sendPayment(@RequestBody Payment payment) {
        kafkaService.sendPayment(payment);
        return ResponseEntity.ok("Payment sent successfully");
    }

    @PostMapping("/with-headers")
    public CompletableFuture<ResponseEntity<String>> sendWithHeaders(@RequestBody UserEvent event) {
        Map<String, String> headers = Map.of(
                "correlation-id", "12345",
                "source", "web-app",
                "version", "1.0"
        );

        return kafkaService.sendWithHeaders(event, headers)
                .thenApply(result -> ResponseEntity.ok("Message with headers sent successfully"))
                .exceptionally(ex -> ResponseEntity.status(500)
                        .body("Failed to send with headers: " + ex.getMessage()));
    }

    @PostMapping("/bulk-events")
    public CompletableFuture<ResponseEntity<String>> sendBulkEvents(@RequestBody List<UserEvent> events) {
        return kafkaService.sendBulkEvents(events)
                .thenApply(v -> ResponseEntity.ok("Bulk events sent successfully"))
                .exceptionally(ex -> ResponseEntity.status(500)
                        .body("Failed to send bulk events: " + ex.getMessage()));
    }
}