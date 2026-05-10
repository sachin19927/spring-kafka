package  com.spring.poc.kafka.model;

import java.time.Instant;
import java.util.UUID;

public record OrderShipped(
        String eventId,
        String orderId,
        String customerId,
        String trackingNumber,
        String carrier,
        Instant estimatedDelivery,
        Instant timestamp,
        String eventType
) implements OrderEvent {

    public OrderShipped(
            String orderId,
            String customerId,
            String trackingNumber,
            String carrier,
            Instant estimatedDelivery
    ) {
        this(
                UUID.randomUUID().toString(),
                orderId,
                customerId,
                trackingNumber,
                carrier,
                null,
                Instant.now(),
                "OrderShipped"
        );
    }
}