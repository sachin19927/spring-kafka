package com.spring.poc.kafka.service;

import com.spring.poc.kafka.dto.OrderRequest;
import com.spring.poc.kafka.dto.OrderResponse;
import com.spring.poc.kafka.events.Order;
import com.spring.poc.kafka.events.OrderStatus;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderInitService {

    private final KafkaTemplate<String, Order> kafkaTemplate;

    @Value("${app.kafka.topics.order-events}")
    private String orderEventsTopic;

    /**
     * Builds a new Order in CREATED status and publishes it to the
     * order-events topic, keyed by orderId so all events for the same order
     * land on the same partition and stay in order.
     */
    public OrderResponse createOrder(OrderRequest request) {
        String orderId = UUID.randomUUID().toString();
        long now = System.currentTimeMillis();

        Order order = Order.newBuilder()
                .setOrderId(orderId)
                .setCustomerId(request.getCustomerId())
                .setProduct(request.getProduct())
                .setQuantity(request.getQuantity())
                .setAmount(request.getAmount())
                .setStatus(OrderStatus.CREATED)
                .setMessage("Order received and queued for processing")
                .setCreatedTimestamp(now)
                .setUpdatedTimestamp(now)
                .build();

        try {
            var sendResult =
                    kafkaTemplate.send(orderEventsTopic, orderId, order).get();
            log.info(
                    "Published order {} to {} (partition={}, offset={})",
                    orderId,
                    orderEventsTopic,
                    sendResult.getRecordMetadata().partition(),
                    sendResult.getRecordMetadata().offset());
        } catch (Exception ex) {
            log.error("Failed to publish order {} to {}", orderId, orderEventsTopic, ex);
            throw new RuntimeException("Failed to create order: " + ex.getMessage(), ex);
        }

        return new OrderResponse(orderId, OrderStatus.CREATED.name(), "Order accepted");
    }
}
