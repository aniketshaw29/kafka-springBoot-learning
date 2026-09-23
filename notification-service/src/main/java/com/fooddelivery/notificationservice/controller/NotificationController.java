package com.fooddelivery.notificationservice.controller;

import com.fooddelivery.notificationservice.model.NotificationRecord;
import com.fooddelivery.notificationservice.repository.NotificationRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * REST API for inspecting notification history.
 * Useful for debugging: "did the customer receive a notification for order X?"
 *
 * GET http://localhost:8083/api/notifications/order/{orderId}
 */
@RestController
@RequestMapping("/api/notifications")
public class NotificationController {

    private final NotificationRepository notificationRepository;

    public NotificationController(NotificationRepository notificationRepository) {
        this.notificationRepository = notificationRepository;
    }

    @GetMapping("/order/{orderId}")
    public ResponseEntity<List<NotificationRecord>> getNotificationsForOrder(
            @PathVariable String orderId) {
        return ResponseEntity.ok(notificationRepository.findByOrderId(orderId));
    }
}
