package com.fooddelivery.notificationservice.model;

import jakarta.persistence.*;
import lombok.Data;
import org.hibernate.annotations.CreationTimestamp;

import java.time.Instant;

/**
 * Record of every notification sent.
 * Useful for:
 * - Deduplication (don't send the same notification twice)
 * - Auditing (did the customer actually receive the notification?)
 * - Debugging (what notification was sent for order X?)
 */
@Entity
@Table(name = "notifications", schema = "notification_service")
@Data
public class NotificationRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private String id;

    @Column(name = "order_id")
    private String orderId;

    @Column(name = "customer_id")
    private String customerId;

    @Enumerated(EnumType.STRING)
    @Column(name = "type")
    private NotificationType type;

    @Column(name = "channel")
    private String channel;  // "SMS", "PUSH", "EMAIL"

    @Column(name = "recipient")
    private String recipient; // Phone number or email (masked in real app)

    @Column(name = "message", columnDefinition = "TEXT")
    private String message;

    @Enumerated(EnumType.STRING)
    @Column(name = "status")
    private NotificationStatus status;

    /**
     * Idempotency key: orderId + type uniquely identifies a notification.
     * Used to prevent sending "order placed" SMS twice to the same customer.
     * Unique constraint in DB ensures at-most-one record per (orderId, type).
     */
    @Column(name = "idempotency_key", unique = true)
    private String idempotencyKey;  // "${orderId}:${type}"

    @CreationTimestamp
    @Column(name = "sent_at", updatable = false)
    private Instant sentAt;
}
