package com.fooddelivery.kitchenservice.config;

import com.fooddelivery.shared.events.OrderPlacedEvent;
import com.fooddelivery.shared.events.OrderStatusUpdatedEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
@Slf4j
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    // ─────────────────────────────────────────────────────────────────────
    // CONSUMER FACTORY — for OrderPlacedEvent messages
    // ─────────────────────────────────────────────────────────────────────

    /**
     * Creates consumers that read OrderPlacedEvent messages.
     * Consumer factories are per-type — if you consume multiple event types,
     * you can either use Object type or create separate factories.
     */
    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> config = new HashMap<>();

        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // Consumer group — all instances of Kitchen Service share this.
        // Kafka will distribute partitions evenly among group members.
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "kitchen-service-group");

        // Key is always orderId (String)
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // Value is JSON — Spring will use type headers to determine the Java class
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        // Security: only allow our event classes to be deserialized
        // Without this, any class name in the JSON header could be deserialized (security risk)
        config.put(JsonDeserializer.TRUSTED_PACKAGES, "com.fooddelivery.shared.events");

        // Do NOT auto-commit — we manually commit after successful processing
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // Start from earliest message if this group has no committed offsets
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Process up to 50 messages per poll (batch size)
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);

        // Max time between calls to poll() before Kafka thinks we're dead
        // If your processing takes longer, increase this
        config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300_000); // 5 minutes

        return new DefaultKafkaConsumerFactory<>(config,
            new StringDeserializer(),
            new JsonDeserializer<>(Object.class, false));
    }

    // ─────────────────────────────────────────────────────────────────────
    // ERROR HANDLER — retry + dead letter topic
    // ─────────────────────────────────────────────────────────────────────

    /**
     * Error handler with:
     *  - Exponential backoff retry (1s, 2s, 4s — up to 3 retries)
     *  - After max retries: route to Dead Letter Topic (order.placed.DLT)
     *
     * This prevents one bad message from blocking the entire partition.
     * The DLT message can be replayed after the root cause is fixed.
     */
    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        // Route failed messages to "<topic>.DLT" after retries exhausted
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);

        // Exponential backoff: retry 3 times, waiting 1s → 2s → 4s between attempts
        ExponentialBackOff backOff = new ExponentialBackOff(1_000L, 2.0);
        backOff.setMaxAttempts(3);

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);

        // Permanent failures — don't retry, go straight to DLT
        // These exceptions indicate data problems, not transient infrastructure issues
        handler.addNotRetryableExceptions(
            org.springframework.kafka.support.serializer.DeserializationException.class, // Can't parse JSON
            com.fasterxml.jackson.core.JsonProcessingException.class  // Malformed JSON
        );

        return handler;
    }

    // ─────────────────────────────────────────────────────────────────────
    // LISTENER CONTAINER FACTORY — ties consumer factory + error handler together
    // ─────────────────────────────────────────────────────────────────────

    /**
     * Container factory used by @KafkaListener annotations.
     * Controls: ack mode, concurrency, error handling.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object>
            kafkaListenerContainerFactory(
                ConsumerFactory<String, Object> consumerFactory,
                DefaultErrorHandler errorHandler) {

        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
            new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);

        // MANUAL_IMMEDIATE: commit offset exactly when ack.acknowledge() is called
        // This gives us control over exactly when a message is marked as "processed"
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        // 3 concurrent listener threads — each thread handles assigned partitions
        // With 3 partitions and 3 threads: full parallelism
        factory.setConcurrency(3);

        // Attach our retry + DLT error handler
        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }
}
