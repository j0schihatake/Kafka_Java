package com.kafka.Kafka;

import com.kafka.Kafka.model.Order;
import com.kafka.Kafka.model.OrderStatus;
import com.kafka.Kafka.model.Payment;
import com.kafka.Kafka.model.PaymentStatus;
import com.kafka.Kafka.model.UserEvent;
import com.kafka.Kafka.service.DemoKafkaService;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.SendResult;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@SpringBootApplication
@RequiredArgsConstructor
public class KafkaApplication {

    private final DemoKafkaService demoKafkaService;

    public static void main(String[] args) {
        SpringApplication.run(KafkaApplication.class, args);
    }

    @Bean
    public CommandLineRunner testKafkaMethods() {
        return args -> {
            // Ждем инициализации Kafka
            System.out.println("⏳ Waiting for Kafka initialization...");
            Thread.sleep(3000);

            System.out.println("🚀 Starting Kafka methods demonstration...");

            // 1. Тест базовой отправки UserEvent
            testUserEventSending();

            // 2. Тест синхронной отправки
            testSyncSending();

            // 3. Тест отправки с callback
            testCallbackSending();

            // 4. Тест отправки с подтверждением
            testAckSending();

            // 5. Тест отправки с заголовками
            testHeadersSending();

            // 6. Тест bulk отправки
            testBulkSending();

            // 7. Тест отправки заказа
            testOrderSending();

            // 8. Тест отправки платежа
            testPaymentSending();

            // 9. Тест асинхронной отправки с CompletableFuture
            testAsyncSending();

            System.out.println("✅ All tests completed successfully!");
        };
    }

    private void testUserEventSending() {
        System.out.println("\n1. 📨 Testing basic UserEvent sending...");
        UserEvent event = new UserEvent(
                "user-123",
                "login",
                Map.of("browser", "Chrome", "ip", "192.168.1.1"),
                LocalDateTime.now()
        );
        demoKafkaService.sendUserEvent(event);
        System.out.println("   ✅ UserEvent sent asynchronously");
    }

    private void testSyncSending() {
        System.out.println("\n2. ⚡ Testing synchronous sending...");
        UserEvent event = new UserEvent(
                "user-456",
                "purchase",
                Map.of("item", "laptop", "price", 999.99),
                LocalDateTime.now()
        );
        try {
            demoKafkaService.sendUserEventSync(event);
            System.out.println("   ✅ Synchronous send completed");
        } catch (Exception e) {
            System.out.println("   ❌ Synchronous send failed: " + e.getMessage());
        }
    }

    private void testCallbackSending() {
        System.out.println("\n3. 📞 Testing callback sending...");
        UserEvent event = new UserEvent(
                "user-789",
                "logout",
                Map.of("session_duration", "5m", "reason", "timeout"),
                LocalDateTime.now()
        );
        demoKafkaService.sendUserEventWithCallback(event);
        System.out.println("   ✅ Callback send initiated");
    }

    private void testAckSending() {
        System.out.println("\n4. ✅ Testing acknowledgment sending...");
        UserEvent event = new UserEvent(
                "user-101",
                "view",
                Map.of("page", "product", "product_id", "p-123"),
                LocalDateTime.now()
        );
        demoKafkaService.sendWithAck(event)
                .thenAccept(acknowledged -> {
                    if (acknowledged) {
                        System.out.println("   ✅ Message acknowledged by broker");
                    } else {
                        System.out.println("   ❌ Message not acknowledged");
                    }
                });
    }

    private void testHeadersSending() {
        System.out.println("\n5. 🏷️ Testing headers sending...");
        UserEvent event = new UserEvent(
                "user-202",
                "search",
                Map.of("query", "spring boot", "results", 15),
                LocalDateTime.now()
        );
        Map<String, String> headers = Map.of(
                "correlation-id", "test-correlation-123",
                "source-service", "demo-app",
                "version", "1.0.0",
                "environment", "development"
        );
        demoKafkaService.sendWithHeaders(event, headers)
                .thenAccept(result -> {
                    System.out.println("   ✅ Message with headers sent successfully");
                })
                .exceptionally(ex -> {
                    System.out.println("   ❌ Failed to send with headers: " + ex.getMessage());
                    return null;
                });
    }

    private void testBulkSending() {
        System.out.println("\n6. 📦 Testing bulk sending...");
        List<UserEvent> events = List.of(
                new UserEvent("user-bulk-1", "click", Map.of("element", "button"), LocalDateTime.now()),
                new UserEvent("user-bulk-2", "scroll", Map.of("position", "50%"), LocalDateTime.now()),
                new UserEvent("user-bulk-3", "hover", Map.of("element", "menu"), LocalDateTime.now()),
                new UserEvent("user-bulk-4", "focus", Map.of("field", "email"), LocalDateTime.now()),
                new UserEvent("user-bulk-5", "blur", Map.of("field", "password"), LocalDateTime.now())
        );
        demoKafkaService.sendBulkEvents(events)
                .thenAccept(voidResult -> {
                    System.out.println("   ✅ Bulk send completed: " + events.size() + " events");
                })
                .exceptionally(ex -> {
                    System.out.println("   ❌ Bulk send failed: " + ex.getMessage());
                    return null;
                });
    }

    private void testOrderSending() {
        System.out.println("\n7. 🛒 Testing order sending...");
        Order order = new Order(
                "order-001",
                "user-123",
                BigDecimal.valueOf(149.99),
                List.of("product-1", "product-2"),
                OrderStatus.CREATED
        );
        demoKafkaService.sendOrder(order);
        System.out.println("   ✅ Order sent asynchronously");
    }

    private void testPaymentSending() {
        System.out.println("\n8. 💳 Testing payment sending...");
        Payment payment = new Payment(
                "payment-001",
                "order-001",
                BigDecimal.valueOf(149.99),
                PaymentStatus.PENDING
        );
        demoKafkaService.sendPayment(payment);
        System.out.println("   ✅ Payment sent asynchronously");
    }

    private void testAsyncSending() throws InterruptedException {
        System.out.println("\n9. ⚡ Testing async CompletableFuture sending...");
        UserEvent event = new UserEvent(
                "user-async-1",
                "async_test",
                Map.of("test_type", "completable_future"),
                LocalDateTime.now()
        );
        CompletableFuture<SendResult<String, String>> future = demoKafkaService.sendUserEvent(event);

        future.thenAccept(result -> {
            System.out.println("   ✅ Async send completed successfully");
            System.out.println("      Topic: " + result.getRecordMetadata().topic());
            System.out.println("      Partition: " + result.getRecordMetadata().partition());
            System.out.println("      Offset: " + result.getRecordMetadata().offset());
        }).exceptionally(ex -> {
            System.out.println("   ❌ Async send failed: " + ex.getMessage());
            return null;
        });

        // Ждем немного чтобы увидеть результат
        Thread.sleep(1000);
    }
}