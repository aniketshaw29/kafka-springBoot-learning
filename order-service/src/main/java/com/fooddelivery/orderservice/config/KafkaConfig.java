package com.fooddelivery.orderservice.config;

import com.fooddelivery.shared.events.OrderItem;
import com.fooddelivery.shared.events.OrderPlacedEvent;
import com.fooddelivery.shared.events.OrderStatusUpdatedEvent;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    // ─────────────────────────────────────────────────────────────────────
    // TOPIC DEFINITIONS
    // Spring will create these topics on startup if they don't exist.
    // Explicitly defining topics is better than relying on auto-create:
    // it enforces the correct partition count and replication factor.
    // ─────────────────────────────────────────────────────────────────────

    /**
     * Topic for new order events.
     * 3 partitions → up to 3 Kitchen Service instances can work in parallel.
     * Retention: 7 days (messages stay even after consumption, can be replayed).
     */
    @Bean
    public NewTopic orderPlacedTopic() {
        return TopicBuilder.name("order.placed")
            .partitions(3)
            .replicas(1)  // Only 1 broker in our Docker setup; use 3 in production
            .config(TopicConfig.RETENTION_MS_CONFIG, "604800000")  // 7 days
            .build();
    }

    /**
     * Topic for status change events (ACCEPTED, PREPARING, DELIVERED, CANCELLED).
     * Same partitions as order.placed — all status events for an order
     * go to the same partition as the original order.placed event (same orderId key).
     */
    @Bean
    public NewTopic orderStatusUpdatedTopic() {
        return TopicBuilder.name("order.status-updated")
            .partitions(3)
            .replicas(1)
            .build();
    }

    /**
     * Dead Letter Topic for order.placed.
     * When Kitchen Service can't process an order after retries, it lands here.
     * Engineer reviews and decides whether to reprocess or refund.
     */
    @Bean
    public NewTopic orderPlacedDltTopic() {
        return TopicBuilder.name("order.placed.DLT")
            .partitions(1)  // Single partition is enough for DLT — not high throughput
            .replicas(1)
            .config(TopicConfig.RETENTION_MS_CONFIG, "2592000000")  // 30 days — keep for investigation
            .build();
    }

    // ─────────────────────────────────────────────────────────────────────
    // PRODUCER CONFIGURATION
    // Most settings come from application.yaml, but we can also set them here.
    // ─────────────────────────────────────────────────────────────────────

    /**
     * ProducerFactory creates the actual Kafka producer instances.
     * KafkaTemplate wraps this factory and provides a convenient API.
     */
    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // Key serializer: orderId is a String
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // Value serializer: event objects serialized to JSON
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        // acks=all: wait for ALL in-sync replicas to acknowledge — safest for orders
        config.put(ProducerConfig.ACKS_CONFIG, "all");

        // Idempotent producer: Kafka assigns sequence numbers to prevent duplicates on retry
        // Requires acks=all
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // Retry up to 3 times on transient failures (network blip, broker restart)
        config.put(ProducerConfig.RETRIES_CONFIG, 3);

        // Batch messages for up to 5ms — slight latency increase, better throughput
        config.put(ProducerConfig.LINGER_MS_CONFIG, 5);

        // Compress with snappy — fast compression, reduces network bandwidth
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        // Include Java class type in JSON header — consumer knows what type to deserialize
        config.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, true);

        // Trust our event package for serialization
        config.put(JsonSerializer.TYPE_MAPPINGS,
            "orderPlaced:com.fooddelivery.shared.events.OrderPlacedEvent," +
            "orderStatusUpdated:com.fooddelivery.shared.events.OrderStatusUpdatedEvent");

        return new DefaultKafkaProducerFactory<>(config);
    }

    /**
     * KafkaTemplate is Spring's main abstraction for publishing messages.
     * Think of it like JdbcTemplate but for Kafka.
     */
    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
