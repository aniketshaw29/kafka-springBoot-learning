package com.fooddelivery.orderservice.dto;

import com.fooddelivery.shared.events.OrderItem;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

import java.util.List;

/**
 * Request body for POST /api/orders.
 * Input validation annotations prevent invalid orders from being persisted or published.
 */
@Data
public class PlaceOrderRequest {

    @NotBlank(message = "Customer ID is required")
    private String customerId;

    @NotBlank(message = "Restaurant ID is required")
    private String restaurantId;

    @NotEmpty(message = "Order must contain at least one item")
    @Valid
    private List<OrderItem> items;

    @NotBlank(message = "Delivery address is required")
    private String deliveryAddress;

    private String specialInstructions;  // Optional
}
