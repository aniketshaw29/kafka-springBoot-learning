package com.fooddelivery.kitchenservice.model;

/**
 * Kitchen-specific status values — different from the global OrderStatus.
 * Kitchen Service has its own vocabulary for what "in progress" means.
 */
public enum KitchenOrderStatus {
    QUEUED,          // Order received, waiting for cook
    IN_PROGRESS,     // Cook is actively preparing
    READY,           // Food ready, waiting for driver
    COMPLETED        // Driver picked up the food
}
