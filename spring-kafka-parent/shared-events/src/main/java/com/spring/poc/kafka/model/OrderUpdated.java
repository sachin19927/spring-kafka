package com.spring.poc.kafka.model;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record OrderUpdated(
        String eventId,
        String orderId,
        String customerId,
        OrderStatus status,
        Map<String, String> updatedFields,
        Instant timestamp,
        String eventType
) implements OrderEvent {

    public OrderUpdated(
            String orderId,
            String customerId,
            OrderStatus status
    ) {
        this(
                UUID.randomUUID().toString(),
                orderId,
                customerId,
                status,
                Map.of(),
                Instant.now(),
                "OrderUpdated"
        );
    }

    public enum OrderStatus {
        PENDING,
        CONFIRMED,
        PROCESSING,
        SHIPPED,
        DELIVERED,
        CANCELLED
    }
}
