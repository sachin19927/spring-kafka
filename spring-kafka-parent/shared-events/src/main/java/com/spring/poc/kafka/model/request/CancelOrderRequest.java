package com.spring.poc.kafka.model.request;

public record CancelOrderRequest(
        String customerId,
        String reason,
        String cancelledBy
) {}
