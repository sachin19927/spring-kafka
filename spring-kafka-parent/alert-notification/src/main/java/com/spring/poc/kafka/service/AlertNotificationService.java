package com.spring.poc.kafka.service;

import com.spring.poc.kafka.model.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class AlertNotificationService {

    @KafkaListener(
            topics = "${kafka.topic}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void sendAlert(
            @Payload OrderEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        log.info("[ALERT] Partition={}, Offset={}, EventType={}, OrderId={}",
                partition, offset, event.eventType(), event.orderId());

        switch (event) {
            case OrderCreated orderCreated ->
                    handleOrderCreated(orderCreated);

            case OrderUpdated orderUpdated ->
                    handleOrderUpdated(orderUpdated);

            case OrderCancelled orderCancelled ->
                    handleOrderCancelled(orderCancelled);

            case OrderShipped orderShipped ->
                    handleOrderShipped(orderShipped);
        }
    }

    private void handleOrderCreated(OrderCreated event) {
        log.info("[ALERT] New order created: {}, amount: ${}",
                event.orderId(), event.totalAmount());

        if (event.totalAmount() > 10000) {
            sendHighValueAlert(event);
        }

        sendCustomerNotification(event.customerId(),
                "Your order " + event.orderId() + " has been received");
    }

    private void handleOrderUpdated(OrderUpdated event) {
        log.info("[ALERT] Order updated: {}, status: {}",
                event.orderId(), event.status());

        String message = switch (event.status()) {
            case CONFIRMED ->
                    "Your order " + event.orderId() + " is confirmed";
            case SHIPPED ->
                    "Your order " + event.orderId() + " has been shipped";
            case DELIVERED ->
                    "Your order " + event.orderId() + " has been delivered";
            case CANCELLED ->
                    "Your order " + event.orderId() + " has been cancelled";
            default -> throw new IllegalStateException("Unexpected value: " + event.status());
        };
        sendCustomerNotification(event.customerId(), message);
    }

    private void handleOrderCancelled(OrderCancelled event) {
        log.warn("[ALERT] Order cancelled: {}, reason: {}",
                event.orderId(), event.reason());

        sendAdminAlert("Order " + event.orderId() + " cancelled by " + event.cancelledBy());
    }

    private void handleOrderShipped(OrderShipped event) {
        log.info("[ALERT] Order shipped: {}, tracking: {}, carrier: {}",
                event.orderId(), event.trackingNumber(), event.carrier());

        sendCustomerNotification(event.customerId(),
                "Your order " + event.orderId() + " shipped. Track: " + event.trackingNumber());
    }

    private void sendHighValueAlert(OrderCreated event) {
        log.warn("🚨 HIGH VALUE ORDER ALERT: Order {} - ${} from customer {}",
                event.orderId(), event.totalAmount(), event.customerId());
    }

    private void sendCustomerNotification(String customerId, String message) {
        log.info("[ALERT] 📧 To customer {}: {}", customerId, message);
    }

    private void sendAdminAlert(String message) {
        log.warn("[ALERT] 🔔 To admin: {}", message);
    }

    @DltHandler
    public void handleDlt(
            @Payload OrderEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String dltTopic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.EXCEPTION_MESSAGE) String exceptionMessage,
            @Header(KafkaHeaders.EXCEPTION_FQCN) String exceptionClass) {

        log.error("╔════════════════════════════════════════════════════════════╗");
        log.error("║  [ALERT-DLQ] NOTIFICATION FAILED - MANUAL ACTION NEEDED    ║");
        log.error("╠════════════════════════════════════════════════════════════╣");
        log.error("║ DLQ Topic:       {}", dltTopic);
        log.error("║ Partition:       {}", partition);
        log.error("║ Offset:          {}", offset);
        log.error("║ Exception:       {} - {}", exceptionClass, exceptionMessage);
        log.error("║ OrderId:         {}", event.orderId());
        log.error("║ EventType:       {}", event.eventType());
        log.error("╚════════════════════════════════════════════════════════════╝");

        // Save to notification retry table for manual processing
        saveFailedNotification(event, exceptionMessage);
    }

    private void saveFailedNotification(OrderEvent event, String error) {
        log.info("[ALERT-DLQ] Saving failed notification for manual recovery");
    }
}

