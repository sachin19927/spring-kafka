package com.spring.poc.kafka.controller;

import com.spring.poc.kafka.model.*;
import com.spring.poc.kafka.model.request.CancelOrderRequest;
import com.spring.poc.kafka.model.request.CreateOrderRequest;
import com.spring.poc.kafka.model.request.ShipOrderRequest;
import com.spring.poc.kafka.model.request.UpdateStatusRequest;
import com.spring.poc.kafka.producer.OrderProducerService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

@RequiredArgsConstructor
@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderProducerService producerService;

    @PostMapping
    public ResponseEntity<String> createOrder(@Valid @RequestBody CreateOrderRequest request) {
        List<OrderCreated.OrderItem> items = request.items().stream()
                .map(i -> new OrderCreated.OrderItem(i.productId(), i.quantity(), i.unitPrice()))
                .toList();

        double totalAmount = items.stream()
                .mapToDouble(OrderCreated.OrderItem::subtotal)
                .sum();

        OrderCreated event = new OrderCreated(
                UUID.randomUUID().toString(),
                request.customerId(),
                items,
                totalAmount
        );

        producerService.sendOrderEvent(event);
        return ResponseEntity.accepted().body("Order created: " + event.orderId());
    }

    @PatchMapping("/{orderId}/status")
    public ResponseEntity<String> updateStatus(
            @PathVariable String orderId,
            @RequestBody UpdateStatusRequest request) {

        OrderUpdated event = new OrderUpdated(
                orderId,
                request.customerId(),
                OrderUpdated.OrderStatus.valueOf(request.status())
        );

        producerService.sendOrderEvent(event);
        return ResponseEntity.accepted().body("Status updated: " + orderId);
    }

    @PostMapping("/{orderId}/cancel")
    public ResponseEntity<String> cancelOrder(
            @PathVariable String orderId,
            @RequestBody CancelOrderRequest request) {

        OrderCancelled event = new OrderCancelled(
                orderId,
                request.customerId(),
                request.reason(),
                request.cancelledBy()
        );

        producerService.sendOrderEvent(event);
        return ResponseEntity.accepted().body("Order cancelled: " + orderId);
    }

    @PostMapping("/{orderId}/ship")
    public ResponseEntity<String> shipOrder(
            @PathVariable String orderId,
            @RequestBody ShipOrderRequest request) {

        OrderShipped event = new OrderShipped(
                orderId,
                request.customerId(),
                request.trackingNumber(),
                request.carrier(),
                request.estimatedDelivery()
        );

        producerService.sendOrderEvent(event);
        return ResponseEntity.accepted().body("Order shipped: " + orderId);
    }

}
