package com.spring.poc.kafka.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

/** Response returned immediately after an order is accepted and published to Kafka. */
@Data
@AllArgsConstructor
public class OrderResponse {
    private String orderId;
    private String status;
    private String message;
}
