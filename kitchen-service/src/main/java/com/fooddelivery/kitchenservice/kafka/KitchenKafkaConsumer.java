package com.fooddelivery.kitchenservice.kafka;

import com.fooddelivery.kitchenservice.service.KitchenOrderService;
import com.fooddelivery.shared.events.OrderPlacedEvent;
import com.fooddelivery.shared.events.OrderStatus;
import com.fooddelivery.shared.events.OrderStatusUpdatedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * Kafka consumer for the Kitchen Service.
 *
 * This class subscribes to order events and delegates business logic
 * to KitchenOrderService. Keeping Kafka concerns separate from business logic
 * makes the service easier to test and understand.
 *
 * IMPORTANT: Each @KafkaListener method runs in its own thread (up to concurrency=3).
 * If order.placed has 3 partitions and we have concurrency=3,
 * each thread processes one partition independently.
 */
@Component
@Slf4j
public class KitchenKafkaConsumer {

    private final KitchenOrderService kitchenOrderService;

    public KitchenKafkaConsumer(KitchenOrderService kitchenOrderService) {
        this.kitchenOrderService = kitchenOrderService;
    }

    /**
     * Listens for new orders placed by customers.
     *
     * Real-life analogy: The Kitchen Display System (KDS) — a tablet mounted in the
     * kitchen that beeps and shows a new ticket whenever an order comes in.
     *
     * Error handling (configured in KafkaConsumerConfig):
     *   - Retries up to 3 times with 1s/2s/4s backoff on transient failures
     *   - After 3 retries: message goes to order.placed.DLT for manual review
     *   - Permanent errors (bad JSON): go directly to DLT without retry
     *
     * @param event     the deserialized order event from Kafka
     * @param ack       used to manually commit the offset after processing
     * @param partition which partition this message came from (for logging)
     * @param offset    the offset of this message in the partition (for logging)
     */
    @KafkaListener(
        id = "kitchenOrderListener",          // Used by KafkaListenerEndpointRegistry for pause/resume
        topics = "order.placed",
        groupId = "kitchen-service-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void onOrderPlaced(
            OrderPlacedEvent event,
            Acknowledgment ack,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        log.info("Kitchen received order: orderId={}, restaurantId={}, items={}, partition={}, offset={}",
            event.getOrderId(),
            event.getRestaurantId(),
            event.getItems() != null ? event.getItems().size() : 0,
            partition,
            offset);

        // Process the order — add to kitchen queue
        kitchenOrderService.acceptOrder(event);

        // Only commit the offset AFTER successful DB write.
        // If we crash here before acknowledging, Kafka will redeliver — and
        // KitchenOrderService.acceptOrder() will see it's a duplicate and skip it.
        ack.acknowledge();

        log.debug("Kitchen order processed and offset committed: orderId={}, offset={}",
            event.getOrderId(), offset);
    }

    /**
     * Listens for order status updates.
     * Kitchen cares specifically about CANCELLED status — stop cooking if possible.
     *
     * Same consumer group as above — both listeners share the "kitchen-service-group".
     * Each consumer thread may be handling either topic.
     */
    @KafkaListener(
        topics = "order.status-updated",
        groupId = "kitchen-service-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void onOrderStatusUpdated(
            OrderStatusUpdatedEvent event,
            Acknowledgment ack) {

        log.info("Kitchen received status update: orderId={}, {} → {}",
            event.getOrderId(), event.getPreviousStatus(), event.getNewStatus());

        // Kitchen only acts on cancellations
        if (event.getNewStatus() == OrderStatus.CANCELLED) {
            kitchenOrderService.cancelOrder(event.getOrderId());
        }

        ack.acknowledge();
    }

    /**
     * Dead Letter Topic consumer — monitor and alert on failed messages.
     *
     * This listener uses a DIFFERENT consumer group so it doesn't interfere
     * with the main kitchen processing group.
     *
     * In production: this would alert on-call, store in a monitoring DB,
     * and possibly trigger automatic replay after the fix is deployed.
     */
    @KafkaListener(
        topics = "order.placed.DLT",
        groupId = "kitchen-dlt-monitor-group"
    )
    public void onDeadLetterMessage(
            String rawMessage,  // Raw string — might be malformed, don't deserialize
            @Header(KafkaHeaders.ORIGINAL_TOPIC) String originalTopic,
            @Header(KafkaHeaders.ORIGINAL_OFFSET) long originalOffset,
            @Header(value = KafkaHeaders.EXCEPTION_MESSAGE, required = false) String exceptionMessage,
            Acknowledgment ack) {

        // In production: send alert to PagerDuty, Slack, etc.
        log.error("DEAD LETTER: Failed to process order after all retries. " +
                  "Original topic={}, offset={}, error={}",
            originalTopic, originalOffset, exceptionMessage);

        // Log enough info for an engineer to manually reprocess or investigate
        log.error("Dead letter message payload: {}", rawMessage);

        ack.acknowledge();
    }
}
