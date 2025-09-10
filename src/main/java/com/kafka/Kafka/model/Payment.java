package com.kafka.Kafka.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Payment {
    private String paymentId;
    private String orderId;
    private BigDecimal amount;
    private PaymentStatus status;
}