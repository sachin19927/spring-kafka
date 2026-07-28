package com.spring.poc.kafka.controller;

import com.spring.poc.kafka.dto.OrderRequest;
import com.spring.poc.kafka.dto.OrderResponse;
import com.spring.poc.kafka.service.OrderInitService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderInitService orderInitService;

    @PostMapping
    public ResponseEntity<OrderResponse> createOrder(@Valid @RequestBody OrderRequest request) {
        OrderResponse response = orderInitService.createOrder(request);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }
}
