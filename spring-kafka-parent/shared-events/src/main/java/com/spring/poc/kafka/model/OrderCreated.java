package com.spring.poc.kafka.model;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record OrderCreated(
        String eventId,
        String orderId,
        String customerId,
        List<OrderItem> items,
        double totalAmount,
        String currency,
        Instant timestamp,
        String eventType
) implements OrderEvent {

    public OrderCreated(
            String orderId,
            String customerId,
            List<OrderItem> items,
            double totalAmount
    ) {
        this(
                UUID.randomUUID().toString(),
                orderId,
                customerId,
                items,
                totalAmount,
                "USD",
                Instant.now(),
                "OrderCreated"
        );
    }

    public record OrderItem(
            String productId,
            int quantity,
            double unitPrice
    ) {
        public double subtotal() {
            return quantity * unitPrice;
        }
    }
}
