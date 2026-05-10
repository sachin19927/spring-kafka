package com.spring.poc.kafka.producer;

import com.spring.poc.kafka.model.Order;
import com.spring.poc.kafka.model.OrderEvent;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
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

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    @Value("${kafka.topic}")
    private String topic;

    public CompletableFuture<SendResult<String, OrderEvent>> sendOrderEvent(OrderEvent event) {
        String key = event.orderId();

        log.info("[PRODUCER] Sending {} event for order: {}", event.eventType(), key);

        CompletableFuture<SendResult<String, OrderEvent>> future =
                kafkaTemplate.send(topic, key, event);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("[PRODUCER] Sent {} to partition={}, offset={}",
                        event.eventType(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("[PRODUCER] Failed to send {}: {}",
                        event.eventType(), ex.getMessage());
            }
        });

        return future;
    }
}
