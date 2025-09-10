package com.kafka.Kafka.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Message {
    private String topic;
    private String key;
    private String value;
    private Long timestamp;
}