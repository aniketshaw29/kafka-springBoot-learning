package com.fooddelivery.kitchenservice.controller;

import com.fooddelivery.kitchenservice.model.KitchenOrder;
import com.fooddelivery.kitchenservice.service.KitchenOrderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * REST API for the Kitchen Display System (KDS).
 * In a real app, kitchen staff would use a tablet app that calls these endpoints.
 *
 * For learning: these endpoints let you manually progress orders through the kitchen.
 *
 * Example usage:
 *   GET  http://localhost:8082/api/kitchen/queue       — see pending orders
 *   POST http://localhost:8082/api/kitchen/{id}/start  — start cooking
 *   POST http://localhost:8082/api/kitchen/{id}/ready  — mark food ready
 */
@RestController
@RequestMapping("/api/kitchen")
@Slf4j
public class KitchenController {

    private final KitchenOrderService kitchenOrderService;

    public KitchenController(KitchenOrderService kitchenOrderService) {
        this.kitchenOrderService = kitchenOrderService;
    }

    /** Show all orders waiting to be cooked */
    @GetMapping("/queue")
    public ResponseEntity<List<KitchenOrder>> getQueue() {
        return ResponseEntity.ok(kitchenOrderService.getQueuedOrders());
    }

    /** Show orders currently being cooked */
    @GetMapping("/in-progress")
    public ResponseEntity<List<KitchenOrder>> getInProgress() {
        return ResponseEntity.ok(kitchenOrderService.getInProgressOrders());
    }

    /** Cook starts working on this order */
    @PostMapping("/{orderId}/start")
    public ResponseEntity<KitchenOrder> startCooking(@PathVariable String orderId) {
        return ResponseEntity.ok(kitchenOrderService.startCooking(orderId));
    }

    /** Food is ready for pickup */
    @PostMapping("/{orderId}/ready")
    public ResponseEntity<KitchenOrder> markReady(@PathVariable String orderId) {
        return ResponseEntity.ok(kitchenOrderService.markReady(orderId));
    }
}
