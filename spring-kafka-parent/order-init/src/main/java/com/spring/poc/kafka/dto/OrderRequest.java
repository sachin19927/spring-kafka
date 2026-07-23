package com.spring.poc.kafka.dto;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Incoming payload for POST /api/orders. */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderRequest {

    @NotBlank(message = "customerId is required")
    private String customerId;

    @NotBlank(message = "product is required")
    private String product;

    @Min(value = 1, message = "quantity must be at least 1")
    private int quantity;

    @DecimalMin(value = "0.01", message = "amount must be greater than 0")
    private double amount;
}
