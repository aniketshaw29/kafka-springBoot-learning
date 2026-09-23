package com.fooddelivery.shared.events;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Event published to "order.status-updated" whenever an order transitions between states.
 *
 * Published by: Kitchen Service (ACCEPTED, PREPARING, READY_FOR_PICKUP)
 *               Driver Service  (PICKED_UP, DELIVERED)
 *               Order Service   (CANCELLED)
 *
 * Consumed by: Notification Service (sends SMS/push to customer)
 *              Customer App        (real-time order tracking)
 *              Analytics Service   (delivery time metrics)
 *
 * Kafka key: orderId
 *   → Status updates for the same order are always in the same partition
 *   → Consumer processes them in order: PLACED→ACCEPTED→PREPARING→DELIVERED
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class OrderStatusUpdatedEvent {

    private String orderId;
    private String customerId;    // Denormalized for consumer convenience — avoids a DB lookup

    /** The status BEFORE this change (useful for audit trails) */
    private OrderStatus previousStatus;

    /** The new status AFTER this change */
    private OrderStatus newStatus;

    /** Who triggered this update: "restaurant", "driver", "customer", "system" */
    private String updatedBy;

    /** Human-readable reason — especially important for CANCELLED status */
    private String reason;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    private Instant timestamp;

    @Builder.Default
    private String schemaVersion = "v1";
}
