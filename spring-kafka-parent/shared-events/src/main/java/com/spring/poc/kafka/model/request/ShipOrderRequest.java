package com.spring.poc.kafka.model.request;

import java.time.Instant;

public record ShipOrderRequest(
        String customerId,
        String trackingNumber,
        String carrier,
        Instant estimatedDelivery
) {}
