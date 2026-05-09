package com.spring.poc.kafka.model;

import java.math.BigDecimal;

public record OrderRequest(String customerId, String productId, Integer quantity, BigDecimal price) {
}
