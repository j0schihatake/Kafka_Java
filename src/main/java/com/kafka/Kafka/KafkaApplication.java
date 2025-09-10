package com.kafka.Kafka;

import com.kafka.Kafka.service.KafkaProducerService;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class KafkaApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaApplication.class, args);
    }

    @Bean
    public CommandLineRunner commandLineRunner(KafkaProducerService producerService) {
        return args -> {
            // Добавим небольшую задержку перед отправкой
            try {
                Thread.sleep(2000); // Ждем 2 секунды для инициализации
                producerService.sendMessage("test-topic", "startup-key", "Application started!");
                System.out.println("Test message sent successfully!");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };
    }

}