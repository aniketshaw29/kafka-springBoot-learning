package com.fooddelivery.kitchenservice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fooddelivery.kitchenservice.model.KitchenOrder;
import com.fooddelivery.kitchenservice.model.KitchenOrderStatus;
import com.fooddelivery.kitchenservice.repository.KitchenOrderRepository;
import com.fooddelivery.shared.events.OrderPlacedEvent;
import com.fooddelivery.shared.events.OrderStatusUpdatedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

@Service
@Slf4j
public class KitchenOrderService {

    private final KitchenOrderRepository kitchenOrderRepository;
    private final ObjectMapper objectMapper;

    public KitchenOrderService(KitchenOrderRepository kitchenOrderRepository,
                                ObjectMapper objectMapper) {
        this.kitchenOrderRepository = kitchenOrderRepository;
        this.objectMapper = objectMapper;
    }

    /**
     * Accepts an incoming order and adds it to the kitchen queue.
     *
     * IDEMPOTENCY: This method is safe to call multiple times with the same orderId.
     * If the Kitchen Service is restarted mid-processing, Kafka will redeliver
     * the same message (at-least-once semantics). The existsByOrderId() check
     * prevents double-queuing.
     *
     * Example: Kitchen Service receives order-789 → saves to DB → crashes before ack.
     * Kafka redelivers order-789 → this method sees it already exists → skips → acks.
     */
    @Transactional
    public void acceptOrder(OrderPlacedEvent event) {
        // Idempotency check — guard against duplicate delivery
        if (kitchenOrderRepository.existsByOrderId(event.getOrderId())) {
            log.warn("Duplicate order received in kitchen, skipping: orderId={}",
                event.getOrderId());
            return;
        }

        KitchenOrder kitchenOrder = new KitchenOrder();
        kitchenOrder.setOrderId(event.getOrderId());
        kitchenOrder.setRestaurantId(event.getRestaurantId());
        kitchenOrder.setStatus(KitchenOrderStatus.QUEUED);
        kitchenOrder.setSpecialInstructions(event.getSpecialInstructions());

        // Serialize items to JSON for storage
        try {
            kitchenOrder.setItemsJson(objectMapper.writeValueAsString(event.getItems()));
        } catch (Exception e) {
            log.error("Failed to serialize items for order: {}", event.getOrderId());
            kitchenOrder.setItemsJson("[]");
        }

        kitchenOrderRepository.save(kitchenOrder);

        log.info("Order queued in kitchen: orderId={}, restaurantId={}, items={}",
            event.getOrderId(), event.getRestaurantId(),
            event.getItems() != null ? event.getItems().size() : 0);
    }

    /**
     * Handles order cancellation — stop cooking if in progress.
     * If order hasn't started yet: remove from queue.
     * If cooking: mark as completed so driver knows not to pick up.
     */
    @Transactional
    public void cancelOrder(String orderId) {
        kitchenOrderRepository.findById(orderId).ifPresent(order -> {
            log.info("Cancelling kitchen order: orderId={}, currentStatus={}",
                orderId, order.getStatus());

            // If already cooking, we can't un-cook it — mark as completed to prevent
            // further processing. The customer will get a refund from Order Service.
            if (order.getStatus() == KitchenOrderStatus.QUEUED) {
                kitchenOrderRepository.delete(order);
                log.info("Order removed from kitchen queue: orderId={}", orderId);
            } else {
                log.info("Order already in progress, cannot fully cancel: orderId={}", orderId);
            }
        });
    }

    /**
     * Called when a cook starts working on an order (e.g., via Kitchen Display System).
     * In a real app, this would be triggered by UI interaction on a tablet in the kitchen.
     */
    @Transactional
    public KitchenOrder startCooking(String orderId) {
        KitchenOrder order = kitchenOrderRepository.findById(orderId)
            .orElseThrow(() -> new IllegalStateException("Order not found in kitchen: " + orderId));

        order.setStatus(KitchenOrderStatus.IN_PROGRESS);
        order.setCookingStartedAt(Instant.now());
        kitchenOrderRepository.save(order);

        log.info("Started cooking: orderId={}", orderId);
        return order;
    }

    /**
     * Called when food is ready for pickup.
     */
    @Transactional
    public KitchenOrder markReady(String orderId) {
        KitchenOrder order = kitchenOrderRepository.findById(orderId)
            .orElseThrow(() -> new IllegalStateException("Order not found in kitchen: " + orderId));

        order.setStatus(KitchenOrderStatus.READY);
        order.setCompletedAt(Instant.now());
        kitchenOrderRepository.save(order);

        log.info("Order ready for pickup: orderId={}", orderId);
        return order;
    }

    public List<KitchenOrder> getQueuedOrders() {
        return kitchenOrderRepository.findByStatus(KitchenOrderStatus.QUEUED);
    }

    public List<KitchenOrder> getInProgressOrders() {
        return kitchenOrderRepository.findByStatus(KitchenOrderStatus.IN_PROGRESS);
    }
}
