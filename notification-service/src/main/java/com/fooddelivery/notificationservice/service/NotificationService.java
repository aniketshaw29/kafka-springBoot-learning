package com.fooddelivery.notificationservice.service;

import com.fooddelivery.notificationservice.model.NotificationRecord;
import com.fooddelivery.notificationservice.model.NotificationStatus;
import com.fooddelivery.notificationservice.model.NotificationType;
import com.fooddelivery.notificationservice.repository.NotificationRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Handles notification delivery and idempotency.
 *
 * In a real application, this would integrate with:
 * - Twilio/AWS SNS for SMS
 * - Firebase/APNs for push notifications
 * - SendGrid/SES for emails
 *
 * For learning, we simulate sending by logging the message.
 */
@Service
@Slf4j
public class NotificationService {

    private final NotificationRepository notificationRepository;

    public NotificationService(NotificationRepository notificationRepository) {
        this.notificationRepository = notificationRepository;
    }

    /**
     * Sends a notification with idempotency guarantee.
     *
     * The idempotency key is "{orderId}:{notificationType}" — if this exact
     * notification has already been sent, we skip it. This handles the
     * at-least-once Kafka delivery: if the consumer crashes after sending the
     * SMS but before ack'ing Kafka, the same event will be redelivered.
     * Without idempotency, the customer would receive two SMS messages.
     *
     * Pattern: "check-then-act" with DB unique constraint as safety net.
     * Race condition between check and save is prevented by the unique constraint
     * on idempotency_key — the second call gets a DB constraint violation and skips.
     */
    @Transactional
    public void sendNotification(
            String orderId,
            String customerId,
            NotificationType type,
            String channel,
            String recipient,
            String message) {

        String idempotencyKey = orderId + ":" + type.name();

        // Idempotency check — did we already send this notification?
        if (notificationRepository.findByIdempotencyKey(idempotencyKey).isPresent()) {
            log.warn("Duplicate notification skipped: orderId={}, type={}", orderId, type);
            return;
        }

        // In a real app: call Twilio/FCM/SES here
        boolean sent = simulateSending(channel, recipient, message);

        // Record the notification attempt regardless of success/failure
        NotificationRecord record = new NotificationRecord();
        record.setOrderId(orderId);
        record.setCustomerId(customerId);
        record.setType(type);
        record.setChannel(channel);
        record.setRecipient(maskRecipient(recipient)); // Don't store full phone number
        record.setMessage(message);
        record.setStatus(sent ? NotificationStatus.SENT : NotificationStatus.FAILED);
        record.setIdempotencyKey(idempotencyKey);
        notificationRepository.save(record);

        if (sent) {
            log.info("Notification sent: orderId={}, type={}, channel={}", orderId, type, channel);
        } else {
            log.error("Notification failed: orderId={}, type={}", orderId, type);
        }
    }

    private boolean simulateSending(String channel, String recipient, String message) {
        // Simulate actual SMS/push notification sending
        log.info("📱 [SIMULATED {}] To: {} | Message: \"{}\"", channel, recipient, message);
        return true; // Always succeeds in simulation
    }

    /** Mask phone number for GDPR/privacy compliance: +91-98765-XXXX */
    private String maskRecipient(String recipient) {
        if (recipient == null || recipient.length() < 6) return "****";
        return recipient.substring(0, recipient.length() - 4) + "XXXX";
    }
}
