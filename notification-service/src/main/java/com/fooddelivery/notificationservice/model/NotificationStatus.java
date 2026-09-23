package com.fooddelivery.notificationservice.model;

public enum NotificationStatus {
    SENT,
    FAILED,
    SKIPPED  // Idempotency skip — notification already sent
}
