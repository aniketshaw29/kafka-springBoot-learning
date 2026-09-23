package com.fooddelivery.orderservice.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fooddelivery.shared.events.OrderItem;
import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;

import java.util.Collections;
import java.util.List;

/**
 * JPA converter — stores List<OrderItem> as a JSON string in the DB column.
 * Jackson serializes/deserializes transparently.
 */
@Converter
public class OrderItemsConverter implements AttributeConverter<List<OrderItem>, String> {

    private static final ObjectMapper mapper = new ObjectMapper()
        .registerModule(new JavaTimeModule());

    @Override
    public String convertToDatabaseColumn(List<OrderItem> items) {
        if (items == null) return null;
        try {
            return mapper.writeValueAsString(items);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to serialize order items", e);
        }
    }

    @Override
    public List<OrderItem> convertToEntityAttribute(String json) {
        if (json == null) return Collections.emptyList();
        try {
            return mapper.readValue(json, new TypeReference<List<OrderItem>>() {});
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to deserialize order items", e);
        }
    }
}
