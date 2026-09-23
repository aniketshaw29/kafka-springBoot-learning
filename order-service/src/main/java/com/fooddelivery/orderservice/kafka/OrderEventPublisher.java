package com.fooddelivery.orderservice.kafka;

import com.fooddelivery.shared.events.OrderPlacedEvent;
import com.fooddelivery.shared.events.OrderStatusUpdatedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * Handles all Kafka publishing for the Order Service.
 *
 * Design decision: separate publisher class keeps Kafka concerns isolated
 * from business logic — OrderService doesn't know how events are published.
 */
@Service
@Slf4j
public class OrderEventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${app.kafka.topics.order-placed}")
    private String orderPlacedTopic;

    @Value("${app.kafka.topics.order-status-updated}")
    private String orderStatusUpdatedTopic;

    public OrderEventPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Publishes a new order event to the "order.placed" topic.
     *
     * Key = orderId → ensures all events for this order land on the same partition,
     * so consumers see them in order: PLACED → ACCEPTED → PREPARING → DELIVERED.
     *
     * Delivery semantics: at-least-once (retries enabled + acks=all).
     * Kitchen Service must be idempotent (check if order already exists before processing).
     */
    public void publishOrderPlaced(OrderPlacedEvent event) {
        CompletableFuture<SendResult<String, Object>> future =
            kafkaTemplate.send(orderPlacedTopic, event.getOrderId(), event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                // In production: also write to outbox table for guaranteed delivery
                log.error("FAILED to publish OrderPlacedEvent: orderId={}, error={}",
                    event.getOrderId(), ex.getMessage(), ex);
            } else {
                log.info("OrderPlacedEvent published: orderId={}, topic={}, partition={}, offset={}",
                    event.getOrderId(),
                    result.getRecordMetadata().topic(),
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());
            }
        });
    }

    /**
     * Publishes an order status change event to "order.status-updated".
     * Called by Kitchen Service via REST, or by Order Service for cancellations.
     *
     * Example status transitions:
     *   PLACED → ACCEPTED (restaurant accepts)
     *   ACCEPTED → PREPARING (kitchen starts cooking)
     *   PREPARING → READY_FOR_PICKUP (food ready)
     *   READY_FOR_PICKUP → PICKED_UP (driver collects)
     *   PICKED_UP → DELIVERED (customer receives)
     *   any → CANCELLED
     */
    public void publishOrderStatusUpdated(OrderStatusUpdatedEvent event) {
        kafkaTemplate.send(orderStatusUpdatedTopic, event.getOrderId(), event)
            .whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("FAILED to publish OrderStatusUpdatedEvent: orderId={}, status={}, error={}",
                        event.getOrderId(), event.getNewStatus(), ex.getMessage());
                } else {
                    log.info("OrderStatusUpdatedEvent published: orderId={}, status={}, partition={}, offset={}",
                        event.getOrderId(),
                        event.getNewStatus(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
                }
            });
    }
}
