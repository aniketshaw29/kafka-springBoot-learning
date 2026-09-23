package com.fooddelivery.notificationservice.kafka;

import com.fooddelivery.notificationservice.model.NotificationType;
import com.fooddelivery.notificationservice.service.NotificationService;
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
 * Notification Service Kafka consumer.
 *
 * KEY CONCEPT — Fan-Out Pattern:
 *   This service uses groupId = "notification-service-group"
 *   Kitchen Service uses groupId = "kitchen-service-group"
 *
 *   Both subscribe to "order.placed". Kafka delivers EVERY message to BOTH groups.
 *   They are completely independent — Notification Service never blocks Kitchen Service.
 *
 *   This is the power of Kafka vs point-to-point messaging (RabbitMQ):
 *   with Kafka, you can add a third consumer (Analytics Service) without
 *   changing anything in Order Service or Kitchen Service.
 */
@Component
@Slf4j
public class NotificationKafkaConsumer {

    private final NotificationService notificationService;

    public NotificationKafkaConsumer(NotificationService notificationService) {
        this.notificationService = notificationService;
    }

    /**
     * Sends an SMS/push notification when a customer places an order.
     *
     * Note: We use groupId = "notification-service-group" here.
     * Compare with KitchenKafkaConsumer which uses "kitchen-service-group".
     * Both consume the SAME "order.placed" topic but independently.
     *
     * In a real app: we'd look up the customer's phone number from a CustomerService
     * or from data denormalized in the event. For demo purposes, we use the customerId
     * as the "phone number."
     */
    @KafkaListener(
        topics = "order.placed",
        groupId = "notification-service-group"
    )
    public void onOrderPlaced(
            OrderPlacedEvent event,
            Acknowledgment ack,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        log.info("Notification service received order: orderId={}, customerId={}, partition={}, offset={}",
            event.getOrderId(), event.getCustomerId(), partition, offset);

        String message = String.format(
            "Your order #%s has been placed! Estimated delivery: %d mins. " +
            "Track your order in the app.",
            event.getOrderId().substring(0, 8),  // Short ID for readability
            event.getEstimatedDeliveryMinutes()
        );

        // Send notification — idempotency handled inside NotificationService
        notificationService.sendNotification(
            event.getOrderId(),
            event.getCustomerId(),
            NotificationType.ORDER_PLACED,
            "SMS",
            event.getCustomerId(),  // In real app: customer.getPhoneNumber()
            message
        );

        // Commit offset — message processed successfully
        ack.acknowledge();
    }

    /**
     * Sends status update notifications as the order progresses.
     *
     * This is the main "real-time tracking" notifications that customers expect:
     * "Restaurant accepted your order", "Your order is being prepared", etc.
     */
    @KafkaListener(
        topics = "order.status-updated",
        groupId = "notification-service-group"
    )
    public void onOrderStatusUpdated(
            OrderStatusUpdatedEvent event,
            Acknowledgment ack) {

        log.info("Notification service received status update: orderId={}, {} → {}",
            event.getOrderId(), event.getPreviousStatus(), event.getNewStatus());

        // Map each status to a customer-friendly notification message
        String message = buildStatusMessage(event);

        if (message != null) {
            NotificationType notificationType = mapStatusToNotificationType(event.getNewStatus());

            notificationService.sendNotification(
                event.getOrderId(),
                event.getCustomerId(),
                notificationType,
                "SMS",
                event.getCustomerId(),  // In real app: customer phone number
                message
            );
        }

        ack.acknowledge();
    }

    /**
     * Generates customer-friendly messages for each order status.
     * Returns null for statuses that don't warrant a notification.
     */
    private String buildStatusMessage(OrderStatusUpdatedEvent event) {
        return switch (event.getNewStatus()) {
            case ACCEPTED ->
                "Good news! Your order has been accepted by the restaurant. " +
                "They'll start cooking soon!";

            case PREPARING ->
                "Your order is being prepared. Get ready — it'll be with you soon!";

            case READY_FOR_PICKUP ->
                "Your food is ready! A driver is on the way to pick it up.";

            case PICKED_UP ->
                "Your order has been picked up by the driver and is on the way!";

            case DELIVERED ->
                "Your order has been delivered. Enjoy your meal! " +
                "Rate your experience in the app.";

            case CANCELLED -> {
                String reason = event.getReason() != null ?
                    " Reason: " + event.getReason() : "";
                yield "We're sorry, your order #" + event.getOrderId().substring(0, 8) +
                    " has been cancelled." + reason + " A refund will be processed shortly.";
            }

            // Don't send notification for PLACED (handled in onOrderPlaced)
            default -> null;
        };
    }

    private NotificationType mapStatusToNotificationType(OrderStatus status) {
        return switch (status) {
            case ACCEPTED          -> NotificationType.ORDER_ACCEPTED;
            case PREPARING         -> NotificationType.ORDER_PREPARING;
            case READY_FOR_PICKUP  -> NotificationType.ORDER_READY_FOR_PICKUP;
            case PICKED_UP         -> NotificationType.ORDER_PICKED_UP;
            case DELIVERED         -> NotificationType.ORDER_DELIVERED;
            case CANCELLED         -> NotificationType.ORDER_CANCELLED;
            default                -> NotificationType.ORDER_PLACED;
        };
    }
}
