package com.spring.poc.kafka.producer;

import com.spring.poc.kafka.model.Order;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.springframework.kafka.support.KafkaHeaders.TOPIC;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderProducerService {

    private final KafkaTemplate<String, Order> kafkaTemplate;

    public CompletableFuture<SendResult<String, Order>> sendOrder(Order order) {
        // Use customerId as key for partition affinity (same customer → same partition)
        String key = order.customerId() != null ? order.customerId() : UUID.randomUUID().toString();

        log.info("Sending order: {} with key: {}", order.orderId(), key);

        CompletableFuture<SendResult<String, Order>> future =
                kafkaTemplate.send("orders", key, order);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Sent order=[{}] to partition=[{}] with offset=[{}]",
                        order.orderId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("Unable to send order=[{}] due to: {}", order.orderId(), ex.getMessage());
            }
        });

        return future;
    }
}
