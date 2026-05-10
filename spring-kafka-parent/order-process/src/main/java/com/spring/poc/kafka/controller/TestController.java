package com.spring.poc.kafka.controller;

import com.spring.poc.kafka.service.OrderProcessingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/test")
@RequiredArgsConstructor
public class TestController {


    private final OrderProcessingService consumer;

    // ===== ENABLE/DISABLE FAILURE SIMULATIONS =====

    @PostMapping("/fail-retryable/{enable}")
    public Map<String, String> toggleRetryableFailure(@PathVariable boolean enable) {
        consumer.enableRetryableFailure(enable);
        return Map.of(
                "status", "OK",
                "simulation", "Retryable failure (RuntimeException)",
                "enabled", String.valueOf(enable),
                "trigger", "Send order with orderId starting with 'FAIL-RETRY-'"
        );
    }

    @PostMapping("/fail-nonretryable/{enable}")
    public Map<String, String> toggleNonRetryableFailure(@PathVariable boolean enable) {
        consumer.enableNonRetryableFailure(enable);
        return Map.of(
                "status", "OK",
                "simulation", "Non-retryable failure (IllegalArgumentException)",
                "enabled", String.valueOf(enable),
                "trigger", "Send order with orderId starting with 'FAIL-FATAL-'"
        );
    }

    @PostMapping("/fail-nullpointer/{enable}")
    public Map<String, String> toggleNullPointerFailure(@PathVariable boolean enable) {
        consumer.enableNullPointerFailure(enable);
        return Map.of(
                "status", "OK",
                "simulation", "NullPointerException",
                "enabled", String.valueOf(enable),
                "trigger", "Send order with orderId starting with 'FAIL-NULL-'"
        );
    }

    @PostMapping("/fail-illegalarg/{enable}")
    public Map<String, String> toggleIllegalArgFailure(@PathVariable boolean enable) {
        consumer.enableIllegalArgumentFailure(enable);
        return Map.of(
                "status", "OK",
                "simulation", "IllegalArgumentException",
                "enabled", String.valueOf(enable),
                "trigger", "Send order with orderId starting with 'FAIL-ARG-'"
        );
    }

    @GetMapping("/status")
    public Map<String, String> getStatus() {
        return Map.of(
                "service", "order-processing-service",
                "simulations", consumer.getSimulationStatus()
        );
    }

    @PostMapping("/reset-all")
    public Map<String, String> resetAll() {
        consumer.enableRetryableFailure(false);
        consumer.enableNonRetryableFailure(false);
        consumer.enableNullPointerFailure(false);
        consumer.enableIllegalArgumentFailure(false);
        return Map.of("status", "All simulations disabled");
    }
}