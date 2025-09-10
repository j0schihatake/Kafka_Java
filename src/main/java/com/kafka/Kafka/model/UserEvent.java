package com.kafka.Kafka.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserEvent {
    private String userId;
    private String eventType;
    private Map<String, Object> payload;
    private LocalDateTime timestamp;
}