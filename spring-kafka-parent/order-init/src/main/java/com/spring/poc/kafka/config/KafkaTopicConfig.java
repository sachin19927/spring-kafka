package com.spring.poc.kafka.config;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

@Configuration
public class KafkaTopicConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${app.kafka.topics.order-events}")
    private String orderEventsTopic;

    @Value("${spring.kafka.topic-configs.order-events.partitions:3}")
    private Integer orderEventsPartitions;

    @Value("${spring.kafka.topic-configs.order-events.replicas:3}")
    private Short orderEventsReplicas;

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return new KafkaAdmin(configs);
    }

    @Bean
    public NewTopic ordersTopic() {
        return new NewTopic(orderEventsTopic, orderEventsPartitions, orderEventsReplicas);
    }
}
