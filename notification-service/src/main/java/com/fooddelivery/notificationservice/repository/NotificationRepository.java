package com.fooddelivery.notificationservice.repository;

import com.fooddelivery.notificationservice.model.NotificationRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface NotificationRepository extends JpaRepository<NotificationRecord, String> {

    /** Used for idempotency check — has this notification type been sent for this order? */
    Optional<NotificationRecord> findByIdempotencyKey(String idempotencyKey);

    List<NotificationRecord> findByOrderId(String orderId);
}
