package com.fooddelivery.shared.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * A single line item within an order.
 * Immutable snapshot of the item at the time of ordering —
 * if the restaurant later changes prices, past orders are unaffected.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class OrderItem {
    private String itemId;       // Restaurant's menu item ID
    private String name;         // "Pizza Margherita" (snapshot, not a foreign key)
    private int quantity;        // How many
    private BigDecimal unitPrice; // Price at time of ordering
    private String specialInstructions; // "No onions", "Extra spicy"
}
