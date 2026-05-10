package com.spring.poc.kafka.controller;

import com.spring.poc.kafka.model.OrderCreated;
import com.spring.poc.kafka.model.OrderEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/produce")
@RequiredArgsConstructor
public class ProducerTestController {


    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    @Value("${kafka.topic}")
    private String topic;

    /**
     * Send a normal order - should process successfully
     */
    @PostMapping("/normal")
    public Map<String, String> sendNormalOrder() {
        OrderCreated event = new OrderCreated(
                "ORD-" + System.currentTimeMillis(),
                "CUST-001",
                List.of(new OrderCreated.OrderItem("PROD-1", 2, 50.0)),
                100.0
        );

        kafkaTemplate.send(topic, event.orderId(), event);
        return Map.of("status", "Sent", "orderId", event.orderId(), "type", "NORMAL");
    }

    /**
     * Send order that triggers RETRYABLE failure (retries 3 times, then DLQ)
     */
    @PostMapping("/retryable")
    public Map<String, String> sendRetryableFailure() {
        OrderCreated event = new OrderCreated(
                "FAIL-RETRY-" + System.currentTimeMillis(),
                "CUST-001",
                List.of(new OrderCreated.OrderItem("PROD-1", 1, 100.0)),
                100.0
        );

        kafkaTemplate.send(topic, event.orderId(), event);
        return Map.of(
                "status", "Sent",
                "orderId", event.orderId(),
                "type", "RETRYABLE FAILURE",
                "expected", "3 retries (1s, 2s, 4s) then DLQ"
        );
    }

    /**
     * Send order that triggers NON-RETRYABLE failure (direct to DLQ, no retries)
     */
    @PostMapping("/nonretryable")
    public Map<String, String> sendNonRetryableFailure() {
        OrderCreated event = new OrderCreated(
                "FAIL-FATAL-" + System.currentTimeMillis(),
                "CUST-001",
                List.of(new OrderCreated.OrderItem("PROD-1", 1, 100.0)),
                100.0
        );

        kafkaTemplate.send(topic, event.orderId(), event);
        return Map.of(
                "status", "Sent",
                "orderId", event.orderId(),
                "type", "NON-RETRYABLE FAILURE",
                "expected", "Direct to DLQ (no retries)"
        );
    }

    /**
     * Send order that triggers NullPointerException (direct to DLQ)
     */
    @PostMapping("/nullpointer")
    public Map<String, String> sendNullPointerFailure() {
        OrderCreated event = new OrderCreated(
                "FAIL-NULL-" + System.currentTimeMillis(),
                "CUST-001",
                List.of(new OrderCreated.OrderItem("PROD-1", 1, 100.0)),
                100.0
        );

        kafkaTemplate.send(topic, event.orderId(), event);
        return Map.of(
                "status", "Sent",
                "orderId", event.orderId(),
                "type", "NULL POINTER",
                "expected", "Direct to DLQ (no retries)"
        );
    }

    /**
     * Send order that triggers IllegalArgumentException (direct to DLQ)
     */
    @PostMapping("/illegalarg")
    public Map<String, String> sendIllegalArgFailure() {
        OrderCreated event = new OrderCreated(
                "FAIL-ARG-" + System.currentTimeMillis(),
                "CUST-001",
                List.of(new OrderCreated.OrderItem("PROD-1", 1, 100.0)),
                100.0
        );

        kafkaTemplate.send(topic, event.orderId(), event);
        return Map.of(
                "status", "Sent",
                "orderId", event.orderId(),
                "type", "ILLEGAL ARGUMENT",
                "expected", "Direct to DLQ (no retries)"
        );
    }

    /**
     * Send multiple orders at once
     */
    @PostMapping("/batch/{count}")
    public Map<String, String> sendBatch(@PathVariable int count) {
        for (int i = 0; i < count; i++) {
            OrderCreated event = new OrderCreated(
                    "ORD-BATCH-" + i + "-" + System.currentTimeMillis(),
                    "CUST-00" + (i % 3 + 1),
                    List.of(new OrderCreated.OrderItem("PROD-" + i, 1, 10.0 * (i + 1))),
                    10.0 * (i + 1)
            );
            kafkaTemplate.send(topic, event.orderId(), event);
        }
        return Map.of("status", "Sent", "count", String.valueOf(count));
    }
}
