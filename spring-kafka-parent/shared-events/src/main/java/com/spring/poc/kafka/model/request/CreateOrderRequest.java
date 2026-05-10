package com.spring.poc.kafka.model.request;

import java.util.List;

public record CreateOrderRequest(
        String customerId,
        List<ItemRequest> items
) {}