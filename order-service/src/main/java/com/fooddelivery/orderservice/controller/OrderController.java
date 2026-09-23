package com.fooddelivery.orderservice.controller;

import com.fooddelivery.orderservice.dto.PlaceOrderRequest;
import com.fooddelivery.orderservice.model.Order;
import com.fooddelivery.orderservice.service.OrderService;
import com.fooddelivery.shared.events.OrderStatus;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * REST API for the Order Service.
 * This is what the customer app calls to place orders.
 *
 * Example requests:
 *
 * Place order:
 *   POST http://localhost:8081/api/orders
 *   Body: {"customerId":"cust-1","restaurantId":"rest-1","items":[...],"deliveryAddress":"..."}
 *
 * Get order:
 *   GET http://localhost:8081/api/orders/{orderId}
 *
 * Update status (for demo purposes):
 *   PATCH http://localhost:8081/api/orders/{orderId}/status?status=ACCEPTED
 */
@RestController
@RequestMapping("/api/orders")
@Slf4j
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    /**
     * Place a new order. This is the main endpoint that triggers Kafka events.
     * After calling this, check the Kafka UI and Kitchen/Notification service logs.
     */
    @PostMapping
    public ResponseEntity<Order> placeOrder(@Valid @RequestBody PlaceOrderRequest request) {
        log.info("Received order request: customerId={}, restaurantId={}",
            request.getCustomerId(), request.getRestaurantId());

        Order order = orderService.placeOrder(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(order);
    }

    @GetMapping("/{orderId}")
    public ResponseEntity<Order> getOrder(@PathVariable String orderId) {
        return ResponseEntity.ok(orderService.getOrder(orderId));
    }

    /**
     * Manually update order status for demo/testing.
     * In production, Kitchen Service and Driver Service would publish status
     * updates via their own Kafka events rather than REST calls.
     */
    @PatchMapping("/{orderId}/status")
    public ResponseEntity<Order> updateStatus(
            @PathVariable String orderId,
            @RequestParam OrderStatus status,
            @RequestParam(required = false) String reason) {

        Order updated = orderService.updateOrderStatus(orderId, status, reason);
        return ResponseEntity.ok(updated);
    }
}
