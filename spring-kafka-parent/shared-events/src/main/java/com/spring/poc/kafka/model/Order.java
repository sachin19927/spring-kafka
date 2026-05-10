package com.spring.poc.kafka.model;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

public record Order(

        UUID orderId,
        String customerId,
        String productId,
        Integer quantity,
        BigDecimal price,
        OrderStatus status,

        @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
        LocalDateTime timestamp

) {

    public enum OrderStatus {
        PENDING,
        CONFIRMED,
        SHIPPED,
        DELIVERED,
        CANCELLED
    }

    public Order(
            UUID orderId,
            String customerId,
            String productId,
            Integer quantity,
            BigDecimal price,
            OrderStatus status
    ) {
        this(
                orderId,
                customerId,
                productId,
                quantity,
                price,
                status,
                LocalDateTime.now()
        );
    }
}


