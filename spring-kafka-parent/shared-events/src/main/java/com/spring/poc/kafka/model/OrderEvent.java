package com.spring.poc.kafka.model;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.time.Instant;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "eventType"
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = OrderCreated.class, name = "OrderCreated"),
        @JsonSubTypes.Type(value = OrderUpdated.class, name = "OrderUpdated"),
        @JsonSubTypes.Type(value = OrderCancelled.class, name = "OrderCancelled"),
        @JsonSubTypes.Type(value = OrderShipped.class, name = "OrderShipped")
})
public sealed interface OrderEvent
        permits OrderCreated, OrderUpdated, OrderCancelled, OrderShipped {

    String eventId();

    String orderId();

    String customerId();

    String eventType();

    Instant timestamp();
}
