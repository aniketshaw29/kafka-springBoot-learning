package com.fooddelivery.shared.events;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/**
 * Event published to Kafka topic "order.placed" when a customer places an order.
 *
 * Published by: Order Service
 * Consumed by:  Kitchen Service, Notification Service, Analytics Service, Loyalty Service
 *
 * Kafka key: orderId
 *   → All events for the same order land in the same partition
 *   → Consumers see order events in chronological order per order
 *
 * @JsonIgnoreProperties(ignoreUnknown = true)
 *   → If a new field is added in a future version, old consumers won't crash
 *   → This is "forward compatibility" — old consumer can process new producer's messages
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class OrderPlacedEvent {

    /** Unique ID for this order — also the Kafka message key */
    private String orderId;

    /** ID of the customer who placed the order */
    private String customerId;

    /** ID of the restaurant fulfilling the order */
    private String restaurantId;

    /** Items ordered — snapshot of menu items at time of ordering */
    private List<OrderItem> items;

    /** Total amount including all items, taxes, delivery fee */
    private BigDecimal totalAmount;

    /** Where the food should be delivered */
    private String deliveryAddress;

    /** Estimated delivery time in minutes (calculated by Order Service) */
    private int estimatedDeliveryMinutes;

    /** Special instructions for the restaurant (e.g., "ring the doorbell") */
    private String specialInstructions;

    /** ISO 8601 timestamp of when the order was placed */
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    private Instant timestamp;

    /**
     * Schema version — bump this when the event structure changes.
     * Consumers can use this to handle different versions gracefully.
     * Analogous to API versioning but for Kafka events.
     */
    @Builder.Default
    private String schemaVersion = "v1";
}
