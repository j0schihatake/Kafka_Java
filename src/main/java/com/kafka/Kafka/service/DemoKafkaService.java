package com.kafka.Kafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.kafka.Kafka.model.Order;
import com.kafka.Kafka.model.Payment;
import com.kafka.Kafka.model.UserEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
@Slf4j
public class DemoKafkaService {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Value("${kafka.topics.user-events}")
    private String userEventsTopic;

    @Value("${kafka.topics.orders}")
    private String ordersTopic;

    @Value("${kafka.topics.payments}")
    private String paymentsTopic;

    // 1. Асинхронная отправка с CompletableFuture
    public CompletableFuture<SendResult<String, String>> sendUserEvent(UserEvent event) {
        String message = convertToJson(event);
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(
                userEventsTopic, event.getUserId(), message
        ).toCompletableFuture();

        return future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to send message: {}", ex.getMessage(), ex);
            } else {
                RecordMetadata metadata = result.getRecordMetadata();
                log.info("Message sent successfully to topic: {}, partition: {}, offset: {}",
                        metadata.topic(), metadata.partition(), metadata.offset());
            }
        });
    }

    // 2. Асинхронная отправка с кастомной обработкой
    public void sendUserEventWithCallback(UserEvent event) {
        String message = convertToJson(event);

        kafkaTemplate.send(userEventsTopic, event.getUserId(), message)
                .toCompletableFuture()
                .thenAccept(result -> {
                    RecordMetadata metadata = result.getRecordMetadata();
                    log.info("✅ Message delivered: topic={}, partition={}, offset={}",
                            metadata.topic(), metadata.partition(), metadata.offset());
                })
                .exceptionally(ex -> {
                    log.error("❌ Failed to send message: {}", ex.getMessage(), ex);
                    return null;
                });
    }

    // 3. Синхронная отправка с ожиданием результата
    public SendResult<String, String> sendUserEventSync(UserEvent event) {
        String message = convertToJson(event);
        try {
            SendResult<String, String> result = kafkaTemplate.send(
                    userEventsTopic, event.getUserId(), message
            ).get(5, TimeUnit.SECONDS);

            log.info("Synchronous send successful: partition={}, offset={}",
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());

            return result;
        } catch (TimeoutException e) {
            log.error("Timeout while sending message", e);
            throw new RuntimeException("Delivery timeout", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Delivery interrupted", e);
        } catch (ExecutionException e) {
            log.error("Execution failed while sending message", e);
            throw new RuntimeException("Delivery execution failed", e);
        }
    }

    // 4. Bulk отправка с обработкой результатов
    public CompletableFuture<Void> sendBulkEvents(List<UserEvent> events) {
        List<CompletableFuture<SendResult<String, String>>> futures = events.stream()
                .map(event -> {
                    String message = convertToJson(event);
                    return kafkaTemplate.send(
                            userEventsTopic, event.getUserId(), message
                    ).toCompletableFuture();
                })
                .toList();

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Some bulk messages failed: {}", ex.getMessage());
                    } else {
                        log.info("All {} bulk messages sent successfully", events.size());
                    }
                });
    }

    // 5. Отправка с кастомными заголовками
    public CompletableFuture<SendResult<String, String>> sendWithHeaders(UserEvent event, Map<String, String> headers) {
        String message = convertToJson(event);

        ProducerRecord<String, String> record = new ProducerRecord<>(
                userEventsTopic, event.getUserId(), message
        );

        headers.forEach((key, value) ->
                record.headers().add(key, value.getBytes(StandardCharsets.UTF_8))
        );

        return kafkaTemplate.send(record).toCompletableFuture()
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("Message with headers sent successfully");
                    }
                });
    }

    // 6. Отправка с подтверждением (acknowledgment)
    public CompletableFuture<Boolean> sendWithAck(UserEvent event) {
        String message = convertToJson(event);

        return kafkaTemplate.send(userEventsTopic, event.getUserId(), message)
                .toCompletableFuture()
                .thenApply(result -> {
                    log.info("Message acknowledged: topic={}, partition={}, offset={}",
                            result.getRecordMetadata().topic(),
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                    return true;
                })
                .exceptionally(ex -> {
                    log.error("Message not acknowledged: {}", ex.getMessage());
                    return false;
                });
    }

    // 7. Отправка заказа (без транзакций)
    public void sendOrder(Order order) {
        String message = convertToJson(order);
        kafkaTemplate.send(ordersTopic, order.getOrderId(), message)
                .toCompletableFuture()
                .thenAccept(result -> {
                    log.info("Order sent: {}", order.getOrderId());
                })
                .exceptionally(ex -> {
                    log.error("Failed to send order: {}", order.getOrderId(), ex);
                    return null;
                });
    }

    // 8. Отправка платежа (без транзакций)
    public void sendPayment(Payment payment) {
        String message = convertToJson(payment);
        kafkaTemplate.send(paymentsTopic, payment.getPaymentId(), message)
                .toCompletableFuture()
                .thenAccept(result -> {
                    log.info("Payment sent: {}", payment.getPaymentId());
                })
                .exceptionally(ex -> {
                    log.error("Failed to send payment: {}", payment.getPaymentId(), ex);
                    return null;
                });
    }

    private String convertToJson(Object object) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
            return mapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to convert to JSON", e);
        }
    }
}