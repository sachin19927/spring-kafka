package com.spring.poc.kafka.controller;

import com.spring.poc.kafka.model.Order;
import com.spring.poc.kafka.model.OrderRequest;
import com.spring.poc.kafka.producer.OrderProducerService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor
@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderProducerService producerService;

    @PostMapping
    public ResponseEntity<String> createOrder(@RequestBody OrderRequest request) {
        Order order = new Order(
                UUID.randomUUID(),
                request.customerId(),
                request.productId(),
                request.quantity(),
                request.price(),
                Order.OrderStatus.PENDING
        );

        CompletableFuture<?> future = producerService.sendOrder(order);

        return ResponseEntity.accepted()
                .body("Order " + order.orderId() + " accepted for processing");
    }

    @GetMapping("/test")
    public ResponseEntity<String> testOrder() {
        OrderRequest request = new OrderRequest(
                "CUST-001",
                "Wireless Keyboard",
                1,
                new BigDecimal("9.99")
        );
        createOrder(request);
        return ResponseEntity.ok("Test order fired");
    }
}
