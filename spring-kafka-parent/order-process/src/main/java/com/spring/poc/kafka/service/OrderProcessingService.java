package com.spring.poc.kafka.service;

import com.spring.poc.kafka.events.Order;
import com.spring.poc.kafka.events.OrderStatus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class OrderProcessingService {

    @KafkaListener(
            topics = "${app.kafka.topics.order-events}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void processOrder(
            @Payload Order order,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_KEY) String key) {

        log.info("PROCESSING SERVICE | Partition: {} | Offset: {} | Key: {}", partition, offset, key);
        log.info("PROCESSING SERVICE | Processing Order: {}", order);

        try {
            // Business logic: validate inventory, process payment, update DB
            validateInventory(order);
            processPayment(order);
            updateOrderStatus(order, OrderStatus.COMPLETED);

            log.info("PROCESSING SERVICE | Order {} processed successfully", order.getOrderId());

        } catch (Exception e) {
            log.error("PROCESSING SERVICE | Failed to process order {}: {}",
                    order.getOrderId(), e.getMessage());
            // Dead Letter Queue logic here
            throw e; // Triggers retry/DLQ based on configuration
        }

    }

    private void validateInventory(Order order) {
        log.info("Validating inventory for product: {}, qty: {}",
                order.getProduct(), order.getQuantity());
        // Inventory check logic
    }

    private void processPayment(Order order) {
        log.info("Processing payment of ${} for order: {}",
                order.getAmount(), order.getOrderId());
        // Payment gateway integration
    }

    private void updateOrderStatus(Order order, OrderStatus status) {
        log.info("Updating order {} status to: {}", order.getOrderId(), status);
        // Database update
    }

}
