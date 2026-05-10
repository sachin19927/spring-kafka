package com.spring.poc.kafka.model.request;

public record ItemRequest(
        String productId,
        int quantity,
        double unitPrice
) {}