package com.fooddelivery.shared.events;

/**
 * Possible states an order can be in throughout its lifecycle.
 *
 * State machine:
 *   PLACED → ACCEPTED → PREPARING → READY_FOR_PICKUP → PICKED_UP → DELIVERED
 *   PLACED → CANCELLED (customer cancelled before acceptance)
 *   ACCEPTED → CANCELLED (restaurant rejected)
 *   PREPARING → CANCELLED (exceptional circumstances)
 */
public enum OrderStatus {
    PLACED,             // Customer placed the order — awaiting restaurant acceptance
    ACCEPTED,           // Restaurant accepted — cooking will begin soon
    PREPARING,          // Kitchen is actively cooking the order
    READY_FOR_PICKUP,   // Food is ready — waiting for driver to collect
    PICKED_UP,          // Driver collected the food — en route to customer
    DELIVERED,          // Customer received the order — workflow complete
    CANCELLED           // Order was cancelled at some step — reason stored separately
}
