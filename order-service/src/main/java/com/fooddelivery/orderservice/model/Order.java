package com.fooddelivery.orderservice.model;

import com.fooddelivery.shared.events.OrderItem;
import com.fooddelivery.shared.events.OrderStatus;
import jakarta.persistence.*;
import lombok.Data;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/**
 * Order entity — stored in the order_service schema in PostgreSQL.
 * This is the Order Service's "source of truth" for order data.
 * Other services only hear about orders via Kafka events.
 */
@Entity
@Table(name = "orders", schema = "order_service")
@Data
public class Order {

    @Id
    @Column(name = "order_id")
    private String orderId;

    @Column(name = "customer_id", nullable = false)
    private String customerId;

    @Column(name = "restaurant_id", nullable = false)
    private String restaurantId;

    /**
     * Order items stored as JSON in a single column.
     * In a real app you'd likely have a separate order_items table,
     * but for learning purposes this keeps the schema simpler.
     */
    @Column(name = "items", columnDefinition = "TEXT")
    @Convert(converter = OrderItemsConverter.class)
    private List<OrderItem> items;

    @Column(name = "total_amount", precision = 10, scale = 2)
    private BigDecimal totalAmount;

    @Column(name = "delivery_address")
    private String deliveryAddress;

    @Column(name = "special_instructions")
    private String specialInstructions;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private OrderStatus status;

    @Column(name = "estimated_delivery_minutes")
    private int estimatedDeliveryMinutes;

    @CreationTimestamp
    @Column(name = "created_at", updatable = false)
    private Instant createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at")
    private Instant updatedAt;
}
