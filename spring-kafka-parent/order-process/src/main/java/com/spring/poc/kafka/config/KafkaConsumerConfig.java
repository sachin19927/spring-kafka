package com.spring.poc.kafka.config;

import com.spring.poc.kafka.model.Order;
import com.spring.poc.kafka.model.OrderEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

import java.util.HashMap;
import java.util.Map;


@Slf4j
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${kafka.dlt.topic}")
    private String dltTopic;

    @Bean
    public ConsumerFactory<String, OrderEvent> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class);
        props.put(JacksonJsonDeserializer.TRUSTED_PACKAGES, "com.spring.poc.kafka.model");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);
        return new DefaultKafkaConsumerFactory<>(
                props,
                new StringDeserializer(),
                new JacksonJsonDeserializer<>(OrderEvent.class)
        );

    }

    @Bean
    public ProducerFactory<String, OrderEvent> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, OrderEvent> dlqKafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderEvent> kafkaListenerContainerFactory(
            ConsumerFactory<String, OrderEvent> consumerFactory,
            KafkaTemplate<String, OrderEvent> kafkaTemplate) {

        ConcurrentKafkaListenerContainerFactory<String, OrderEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(3);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.getContainerProperties().setPollTimeout(3000);

        // Error handler with retry and DLQ
        factory.setCommonErrorHandler(createErrorHandler(kafkaTemplate));

        return factory;
    }

    private DefaultErrorHandler createErrorHandler(KafkaTemplate<String, OrderEvent> kafkaTemplate) {

        // Retry 3 times with exponential backoff: 1s, 2s, 4s
        ExponentialBackOffWithMaxRetries backOff =
                new ExponentialBackOffWithMaxRetries(3);
        backOff.setInitialInterval(1000);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(10000);

        DeadLetterPublishingRecoverer recoverer =
                new DeadLetterPublishingRecoverer(kafkaTemplate,
                        (record, ex) -> {
                            log.error("[DLQ] Sending failed record to {}: key={}, exception={}",
                                    dltTopic, record.key(), ex.getClass().getSimpleName());
                            return new TopicPartition(dltTopic, record.partition());
                        });

        // Add custom headers to DLQ message
        recoverer.setHeadersFunction((consumerRecord, exception) -> {
            RecordHeaders headers = new RecordHeaders();
            headers.add("x-dlt-consumer-group", groupId.getBytes());
            headers.add("x-dlt-service", getServiceName().getBytes());
            headers.add("x-dlt-failure-time",
                    String.valueOf(System.currentTimeMillis()).getBytes());
            headers.add("x-dlt-exception-type",
                    exception.getClass().getSimpleName().getBytes());
            headers.add("x-dlt-exception-message",
                    truncate(exception.getMessage(), 500).getBytes());
            return headers;
        });

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);

        // Non-retryable exceptions - go directly to DLQ
        errorHandler.addNotRetryableExceptions(
                IllegalArgumentException.class,
                NullPointerException.class,
                ClassCastException.class,
                UnsupportedOperationException.class
        );

        return errorHandler;
    }

    private String getServiceName() {
        // Return appropriate service name based on which config this is
        if (groupId.contains("alert")) {
            return "alert-notification-service";
        }
        return "order-processing-service";
    }

    private String truncate(String str, int maxLength) {
        if (str == null) return "null";
        return str.length() > maxLength ? str.substring(0, maxLength) : str;
    }
}
