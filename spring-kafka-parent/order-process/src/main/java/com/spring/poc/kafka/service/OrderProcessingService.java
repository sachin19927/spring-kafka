package com.spring.poc.kafka.service;

import com.spring.poc.kafka.model.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Service
public class OrderProcessingService {

    // ===== FAILURE SIMULATION FLAGS =====
    // These can be toggled via REST API to test DLQ behavior
    private final AtomicBoolean simulateRetryableFailure = new AtomicBoolean(false);
    private final AtomicBoolean simulateNonRetryableFailure = new AtomicBoolean(false);
    private final AtomicBoolean simulateNullPointer = new AtomicBoolean(false);
    private final AtomicBoolean simulateIllegalArgument = new AtomicBoolean(false);

    @Value("${kafka.dlt.topic}")
    private String dltTopic;

    @KafkaListener(
            topics = "${kafka.topic}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void processOrder(
            @Payload OrderEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        log.info("[PROCESSING] Partition={}, Offset={}, EventType={}, OrderId={}",
                   partition, offset, event.eventType(), event.orderId());

        // ===== SIMULATE FAILURES =====
        checkSimulatedFailures(event);

        switch (event) {
            case OrderCreated orderCreated ->
                    processOrderCreated(orderCreated);

            case OrderUpdated orderUpdated ->
                    processOrderUpdated(orderUpdated);

            case OrderCancelled orderCancelled ->
                    processOrderCancelled(orderCancelled);

            case OrderShipped orderShipped ->
                    processOrderShipped(orderShipped);
        }
    }

    private void processOrderCreated(OrderCreated event) {
        log.info("[PROCESSING] Creating order: {}, amount: ${}",
                         event.orderId(), event.totalAmount());

        // Simulate processing
        validateInventory(event);
        reservePayment(event);
        saveToDatabase(event);

        log.info("[PROCESSING] Order {} processed successfully", event.orderId());
    }

    private void processOrderUpdated(OrderUpdated event) {
        log.info("[PROCESSING] Updating order: {}, status: {}",
                event.orderId(), event.status());

        updateDatabaseStatus(event);

        if (event.status() == OrderUpdated.OrderStatus.CONFIRMED) {
            triggerFulfillment(event);
        }
    }

    private void processOrderCancelled(OrderCancelled event) {
        log.info("[PROCESSING] Cancelling order: {}, reason: {}",
                event.orderId(), event.reason());

        releaseInventory(event);
        processRefund(event);
        updateStatusCancelled(event);
    }

    private void processOrderShipped(OrderShipped event) {
        log.info("[PROCESSING] Order shipped: {}, tracking: {}, carrier: {}",
                event.orderId(), event.trackingNumber(), event.carrier());

        updateShippingInfo(event);
    }

    // Business logic stubs
    private void validateInventory(OrderCreated event) {
        log.info("[PROCESSING] Validating inventory for {} items", event.items().size());
    }

    private void reservePayment(OrderCreated event) {
        log.info("[PROCESSING] Reserving payment: ${}", event.totalAmount());
    }

    private void saveToDatabase(OrderCreated event) {
        log.info("[PROCESSING] Saving order to database");
    }

    private void updateDatabaseStatus(OrderUpdated event) {
        log.info("[PROCESSING] Updating status to {}", event.status());
    }

    private void triggerFulfillment(OrderUpdated event) {
        log.info("[PROCESSING] Triggering fulfillment for order {}", event.orderId());
    }

    private void releaseInventory(OrderCancelled event) {
        log.info("[PROCESSING] Releasing inventory for order {}", event.orderId());
    }

    private void processRefund(OrderCancelled event) {
        log.info("[PROCESSING] Processing refund for order {}", event.orderId());
    }

    private void updateStatusCancelled(OrderCancelled event) {
        log.info("[PROCESSING] Marking order {} as CANCELLED", event.orderId());
    }

    private void updateShippingInfo(OrderShipped event) {
        log.info("[PROCESSING] Updating shipping info: tracking={}", event.trackingNumber());
    }

    @DltHandler
    public void handleDlt(
            @Payload OrderEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String dltTopic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.EXCEPTION_MESSAGE) String exceptionMessage,
            @Header(KafkaHeaders.EXCEPTION_FQCN) String exceptionClass,
            @Header(value = "x-dlt-consumer-group", required = false) String dltGroup,
            @Header(value = "x-dlt-service", required = false) String dltService,
            @Header(value = "x-dlt-exception-type", required = false) String dltExceptionType) {

        log.error("╔════════════════════════════════════════════════════════════╗");
        log.error("║  [PROCESSING-DLQ] PERMANENT FAILURE - MANUAL ACTION NEEDED ║");
        log.error("╠════════════════════════════════════════════════════════════╣");
        log.error("║ DLQ Topic:       {}", dltTopic);
        log.error("║ DLQ Group:       {}", dltGroup);
        log.error("║ DLQ Service:     {}", dltService);
        log.error("║ Partition:       {}", partition);
        log.error("║ Offset:          {}", offset);
        log.error("║ Exception:       {} - {}", exceptionClass, exceptionMessage);
        log.error("║ OrderId:         {}", event.orderId());
        log.error("║ EventType:       {}", event.eventType());
        log.error("╚════════════════════════════════════════════════════════════╝");

        // Persist to dead letter table for manual recovery
        persistFailedEvent(event, exceptionMessage);

        // Alert operations team
        alertOpsTeam(event, exceptionMessage);
    }

    private void persistFailedEvent(OrderEvent event, String error) {
        log.info("[PROCESSING-DLQ] Persisting failed event for manual recovery");
    }

    private void alertOpsTeam(OrderEvent event, String error) {
        log.error("[OPS-ALERT] Order processing failed permanently: order={}, error={}",
                event.orderId(), error);
    }

    // ===== FAILURE SIMULATION METHODS =====

    private void checkSimulatedFailures(OrderEvent event) {

        // 1. RETRYABLE FAILURE - Will retry 3 times (1s, 2s, 4s) then go to DLQ
        if (simulateRetryableFailure.get()) {
            // Only fail for specific order to avoid blocking all messages
            if (event.orderId().startsWith("FAIL-RETRY")) {
                log.warn("[PROCESSING] SIMULATING RETRYABLE FAILURE for order: {}",
                        event.orderId());
                throw new RuntimeException("Database connection timeout - RETRYABLE");
            }
        }

        // 2. NON-RETRYABLE FAILURE - Goes directly to DLQ (no retries)
        if (simulateNonRetryableFailure.get()) {
            if (event.orderId().startsWith("FAIL-FATAL")) {
                log.error("[PROCESSING] SIMULATING NON-RETRYABLE FAILURE for order: {}",
                        event.orderId());
                throw new IllegalArgumentException("Invalid order data - NON-RETRYABLE");
            }
        }

        // 3. NULL POINTER - Non-retryable, direct to DLQ
        if (simulateNullPointer.get()) {
            if (event.orderId().startsWith("FAIL-NULL")) {
                log.error("[PROCESSING] SIMULATING NULL POINTER for order: {}",
                        event.orderId());
                String nullString = null;
                nullString.length(); // This will throw NPE
            }
        }

        // 4. ILLEGAL ARGUMENT - Non-retryable, direct to DLQ
        if (simulateIllegalArgument.get()) {
            if (event.orderId().startsWith("FAIL-ARG")) {
                log.error("[PROCESSING] SIMULATING ILLEGAL ARGUMENT for order: {}",
                        event.orderId());
                throw new IllegalArgumentException("Business rule violation - NON-RETRYABLE");
            }
        }
    }


    // ===== CONTROL METHODS (Called by TestController) =====

    public void enableRetryableFailure(boolean enable) {
        simulateRetryableFailure.set(enable);
        log.info("[SIMULATION] Retryable failure simulation: {}", enable);
    }

    public void enableNonRetryableFailure(boolean enable) {
        simulateNonRetryableFailure.set(enable);
        log.info("[SIMULATION] Non-retryable failure simulation: {}", enable);
    }

    public void enableNullPointerFailure(boolean enable) {
        simulateNullPointer.set(enable);
        log.info("[SIMULATION] Null pointer failure simulation: {}", enable);
    }

    public void enableIllegalArgumentFailure(boolean enable) {
        simulateIllegalArgument.set(enable);
        log.info("[SIMULATION] Illegal argument failure simulation: {}", enable);
    }

    public String getSimulationStatus() {
        return String.format(
                "Retryable: %s, NonRetryable: %s, NullPointer: %s, IllegalArg: %s",
                simulateRetryableFailure.get(),
                simulateNonRetryableFailure.get(),
                simulateNullPointer.get(),
                simulateIllegalArgument.get()
        );
    }

    // ==================== SIMULATION HELPERS ====================

    private boolean shouldSimulateInventoryTimeout() {
        return false;
    }

    private boolean shouldSimulateInventoryError() {
        return false;
    }

    private boolean shouldSimulatePaymentTimeout() {
        return false;
    }
}
