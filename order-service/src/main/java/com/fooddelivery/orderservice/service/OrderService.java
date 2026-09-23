package com.fooddelivery.orderservice.service;

import com.fooddelivery.orderservice.dto.PlaceOrderRequest;
import com.fooddelivery.orderservice.kafka.OrderEventPublisher;
import com.fooddelivery.orderservice.model.Order;
import com.fooddelivery.orderservice.repository.OrderRepository;
import com.fooddelivery.shared.events.OrderPlacedEvent;
import com.fooddelivery.shared.events.OrderStatus;
import com.fooddelivery.shared.events.OrderStatusUpdatedEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Service
@Slf4j
public class OrderService {

    private final OrderRepository orderRepository;
    private final OrderEventPublisher eventPublisher;

    public OrderService(OrderRepository orderRepository, OrderEventPublisher eventPublisher) {
        this.orderRepository = orderRepository;
        this.eventPublisher = eventPublisher;
    }

    /**
     * Places an order: persists to DB, then publishes to Kafka.
     *
     * Pattern: "DB first, then event"
     *   - If DB fails: no event published (consistent — order doesn't exist anywhere)
     *   - If Kafka publish fails after DB commit: order exists in DB but no event
     *     → Solution: implement Outbox pattern (see 08-advanced-patterns.md)
     *     → For learning: we log the error and the order can be re-published via an API
     *
     * @Transactional: if DB save fails, everything rolls back. Kafka publish is OUTSIDE
     *   the transaction (Kafka doesn't participate in JPA transactions by default).
     */
    @Transactional
    public Order placeOrder(PlaceOrderRequest request) {
        // Calculate total from items
        BigDecimal totalAmount = request.getItems().stream()
            .map(item -> item.getUnitPrice().multiply(BigDecimal.valueOf(item.getQuantity())))
            .reduce(BigDecimal.ZERO, BigDecimal::add);

        // Build and save the Order entity to PostgreSQL
        Order order = new Order();
        order.setOrderId(UUID.randomUUID().toString());
        order.setCustomerId(request.getCustomerId());
        order.setRestaurantId(request.getRestaurantId());
        order.setItems(request.getItems());
        order.setTotalAmount(totalAmount);
        order.setDeliveryAddress(request.getDeliveryAddress());
        order.setSpecialInstructions(request.getSpecialInstructions());
        order.setStatus(OrderStatus.PLACED);
        order.setEstimatedDeliveryMinutes(35); // Simplified — real app would calculate dynamically
        orderRepository.save(order);

        // Build the Kafka event (only include what other services need — not internal fields)
        OrderPlacedEvent event = OrderPlacedEvent.builder()
            .orderId(order.getOrderId())
            .customerId(order.getCustomerId())
            .restaurantId(order.getRestaurantId())
            .items(order.getItems())
            .totalAmount(order.getTotalAmount())
            .deliveryAddress(order.getDeliveryAddress())
            .specialInstructions(order.getSpecialInstructions())
            .estimatedDeliveryMinutes(order.getEstimatedDeliveryMinutes())
            .timestamp(Instant.now())
            .build();

        // Publish to Kafka — asynchronous, won't block this request
        eventPublisher.publishOrderPlaced(event);

        log.info("Order placed: orderId={}, customerId={}, restaurantId={}, total={}",
            order.getOrderId(), order.getCustomerId(), order.getRestaurantId(), totalAmount);

        return order;
    }

    /**
     * Updates order status and publishes the change as a Kafka event.
     * In a real app, status updates from Kitchen/Driver services would go through
     * their own Kafka events rather than REST calls to Order Service.
     * Simplified here for learning purposes.
     */
    @Transactional
    public Order updateOrderStatus(String orderId, OrderStatus newStatus, String reason) {
        Order order = orderRepository.findById(orderId)
            .orElseThrow(() -> new IllegalArgumentException("Order not found: " + orderId));

        OrderStatus previousStatus = order.getStatus();
        order.setStatus(newStatus);
        orderRepository.save(order);

        // Publish status change so all subscribers are notified
        OrderStatusUpdatedEvent event = OrderStatusUpdatedEvent.builder()
            .orderId(orderId)
            .customerId(order.getCustomerId())
            .previousStatus(previousStatus)
            .newStatus(newStatus)
            .updatedBy("system")
            .reason(reason)
            .timestamp(Instant.now())
            .build();

        eventPublisher.publishOrderStatusUpdated(event);

        log.info("Order status updated: orderId={}, {} → {}",
            orderId, previousStatus, newStatus);

        return order;
    }

    public Order getOrder(String orderId) {
        return orderRepository.findById(orderId)
            .orElseThrow(() -> new IllegalArgumentException("Order not found: " + orderId));
    }
}
