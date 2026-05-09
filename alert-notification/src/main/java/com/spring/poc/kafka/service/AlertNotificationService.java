package com.spring.poc.kafka.service;

import com.spring.poc.kafka.model.Order;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class AlertNotificationService {

    @KafkaListener(
            topics = "orders",
            groupId = "alert-notification-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void handleOrderForAlert(
            @Payload Order order,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_KEY) String key) {

        log.info("ALERT SERVICE | Partition: {} | Offset: {} | Key: {}", partition, offset, key);
        log.info("ALERT SERVICE | Received Order: {}", order);

        // Alert logic: High value orders, cancelled orders, etc.
        if (order.price().doubleValue() > 1000.00) {
            sendHighValueAlert(order);
        }

        if (order.status() == Order.OrderStatus.CANCELLED) {
            sendCancellationAlert(order);
        }

        // Send email/push notification logic here
        log.info("ALERT SERVICE | Alert processed for order: {}", order.orderId());
    }

    private void sendHighValueAlert(Order order) {
        log.warn("HIGH VALUE ORDER ALERT: Order {} from customer {} amount: ${}",
                order.orderId(), order.customerId(), order.price());
        // Implement email/SMS/push notification
    }

    private void sendCancellationAlert(Order order) {
        log.warn("ORDER CANCELLED ALERT: Order {} by customer {}",
                order.orderId(), order.customerId());
    }
}
