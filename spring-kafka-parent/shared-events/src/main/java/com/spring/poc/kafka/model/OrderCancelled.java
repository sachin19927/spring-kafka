package com.spring.poc.kafka.model;

import java.time.Instant;
import java.util.UUID;

public record OrderCancelled(
        String eventId,
        String orderId,
        String customerId,
        String reason,
        String cancelledBy,
        Instant timestamp,
        String eventType
) implements OrderEvent {

    public OrderCancelled(
            String orderId,
            String customerId,
            String reason,
            String cancelledBy
    ) {
        this(
                UUID.randomUUID().toString(),
                orderId,
                customerId,
                reason,
                cancelledBy,
                Instant.now(),
                "OrderCancelled"
        );
    }
}