package com.fooddelivery.kitchenservice.model;

import com.fooddelivery.shared.events.OrderItem;
import jakarta.persistence.*;
import lombok.Data;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.Instant;
import java.util.List;

/**
 * Kitchen Service's view of an order — only what the kitchen cares about.
 * This is a completely separate DB entity from Order Service's Order table.
 * Each service has its own data model — "database per service" pattern.
 */
@Entity
@Table(name = "kitchen_orders", schema = "kitchen_service")
@Data
public class KitchenOrder {

    @Id
    @Column(name = "order_id")
    private String orderId;

    @Column(name = "restaurant_id")
    private String restaurantId;

    /**
     * Items to be cooked — denormalized from the event payload.
     * Kitchen doesn't need customer details, delivery address, etc.
     */
    @Column(name = "items", columnDefinition = "TEXT")
    private String itemsJson;  // Stored as JSON string (see KitchenOrderService for serialization)

    @Column(name = "special_instructions")
    private String specialInstructions;

    @Enumerated(EnumType.STRING)
    @Column(name = "status")
    private KitchenOrderStatus status;

    @Column(name = "cook_id")
    private String cookId;  // Which cook is assigned

    @CreationTimestamp
    @Column(name = "received_at", updatable = false)
    private Instant receivedAt;

    @UpdateTimestamp
    @Column(name = "updated_at")
    private Instant updatedAt;

    @Column(name = "cooking_started_at")
    private Instant cookingStartedAt;

    @Column(name = "completed_at")
    private Instant completedAt;
}
