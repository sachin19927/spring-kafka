package com.spring.poc.kafka.model.request;

public record UpdateStatusRequest(
        String customerId,
        String status
) {}
