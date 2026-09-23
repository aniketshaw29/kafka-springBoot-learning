package com.fooddelivery.kitchenservice.repository;

import com.fooddelivery.kitchenservice.model.KitchenOrder;
import com.fooddelivery.kitchenservice.model.KitchenOrderStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface KitchenOrderRepository extends JpaRepository<KitchenOrder, String> {
    List<KitchenOrder> findByStatus(KitchenOrderStatus status);
    List<KitchenOrder> findByRestaurantId(String restaurantId);
    boolean existsByOrderId(String orderId); // Used for idempotency check
}
