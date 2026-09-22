# 07 — Schema & Serialization

## The Problem with "Just Use JSON"

When Order Service publishes an event and Kitchen Service consumes it, they must agree on the message format. Early in development, you just use JSON and everything works. Then...

```
Sprint 1: Order Service publishes:
  {"orderId": "123", "customerId": "c1", "items": [...]}

Sprint 5: Order Service adds a new field:
  {"orderId": "123", "customerId": "c1", "items": [...], "deliveryAddress": {...}}

Old Kitchen Service: works fine (ignores unknown fields)

Sprint 8: Order Service renames a field:
  {"orderId": "123", "customer_id": "c1", ...}  ← camelCase to snake_case
                         ↑
  Kitchen Service breaks! It still reads "customerId" but field is gone.
```

This is **schema evolution** — the challenge of changing message formats over time without breaking consumers.

---

## Serialization Options

### Option 1: JSON (Good for Development)

```java
// Producer config:
config.put(VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

// Consumer config:
config.put(VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
config.put(JsonDeserializer.TRUSTED_PACKAGES, "com.fooddelivery.shared.events");
```

**Pros:**
- Human-readable — easy to debug in Kafka UI
- No schema registry needed
- Java objects ↔ JSON out-of-the-box with Jackson

**Cons:**
- No schema enforcement — anyone can publish garbage
- Larger payload size (field names repeated in every message)
- No schema evolution contract

### Option 2: Avro + Schema Registry (Production Standard)

Avro is a binary serialization format with a schema. The Schema Registry stores schemas and enforces compatibility.

```
Producer:
  OrderPlacedEvent (Java) → [Avro serializer] → bytes + schema ID
                              sends to Kafka

Consumer:
  bytes + schema ID → [Avro deserializer] → fetches schema from registry
                    → OrderPlacedEvent (Java)
```

**Pros:**
- Binary format — 3-5x smaller than JSON
- Schema enforced — invalid messages rejected
- Schema evolution with compatibility rules
- Forward/backward compatibility guarantees

**Cons:**
- Requires Schema Registry (Confluent)
- Learning curve
- Not human-readable (need tooling to inspect)

### Option 3: Protobuf (Google Protocol Buffers)

Similar to Avro but from Google. Used heavily at Google/Uber.

```proto
// order_events.proto
message OrderPlacedEvent {
  string order_id = 1;
  string customer_id = 2;
  repeated OrderItem items = 3;
  int64 timestamp_ms = 4;
}
```

**Pros:** Very compact, strong typing, language-agnostic
**Cons:** Requires protoc compiler, Schema Registry for evolution tracking

---

## JSON in This Project

For this learning project, we use **JSON** — it's visible in Kafka UI, easy to debug, and doesn't require running a Schema Registry. The code is structured so you could swap in Avro later.

### Shared Event Classes

```java
// OrderPlacedEvent.java
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)  // Forward compatibility: ignore future fields
public class OrderPlacedEvent {

    private String orderId;
    private String customerId;
    private String restaurantId;
    private List<OrderItem> items;
    private BigDecimal totalAmount;
    private String deliveryAddress;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    private Instant timestamp;

    // Schema version — consumers can check this and handle differently
    // e.g., v1 had no totalAmount field, v2 added it
    @Builder.Default
    private String schemaVersion = "v2";
}

// OrderItem.java (nested object)
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderItem {
    private String itemId;
    private String name;
    private int quantity;
    private BigDecimal unitPrice;
}

// OrderStatusUpdatedEvent.java
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class OrderStatusUpdatedEvent {
    private String orderId;
    private OrderStatus previousStatus;
    private OrderStatus newStatus;
    private String updatedBy;    // "restaurant" | "driver" | "system"
    private String reason;       // optional (e.g., for cancellations)
    private Instant timestamp;
}

// OrderStatus.java (enum)
public enum OrderStatus {
    PLACED,
    ACCEPTED,
    PREPARING,
    READY_FOR_PICKUP,
    PICKED_UP,
    DELIVERED,
    CANCELLED
}
```

### Jackson Configuration for Kafka

```java
@Configuration
public class JacksonConfig {

    /**
     * Custom ObjectMapper for Kafka messages.
     * Separate from the main Spring MVC ObjectMapper — Kafka has specific needs.
     */
    @Bean("kafkaObjectMapper")
    public ObjectMapper kafkaObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();

        // Handle Java 8 dates (Instant, LocalDateTime)
        mapper.registerModule(new JavaTimeModule());

        // Write dates as ISO strings, not epoch numbers
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        // Don't fail if a new field appears in a consumed message
        // (forward compatibility — new producer sends field old consumer doesn't know)
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        // Don't fail if an enum value is unknown
        // (e.g., producer adds new OrderStatus, old consumer doesn't have it)
        mapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);

        return mapper;
    }
}
```

---

## Schema Evolution Best Practices

Even with JSON, follow these rules to keep services compatible:

### Safe Changes (Non-Breaking)

```java
// Adding a new optional field — SAFE
// Old consumers ignore it, new consumers can use it
public class OrderPlacedEvent {
    private String orderId;
    private String customerId;
    private String restaurantId;
    private List<OrderItem> items;
    private BigDecimal totalAmount;          // Added in v2
    private String promotionCode;            // Added in v3 — totally fine
}
```

### Unsafe Changes (Breaking)

```java
// BREAKING: Removing a field consumers depend on
// public class OrderPlacedEvent {
//     private String orderId;
//     // private String customerId; ← Kitchen Service reads this!
// }

// BREAKING: Changing a field type
// String orderId → Integer orderId  ← Consumers crash on deserialization

// BREAKING: Renaming a field
// customerId → customer_id  ← Consumers read null
```

### Safe Approach for "Renaming"

```java
// Never rename — add the new field alongside the old one
public class OrderPlacedEvent {
    @Deprecated  // Still here for old consumers
    private String customerId;

    // New preferred field name — new consumers use this
    @JsonProperty("customer_id")
    private String newCustomerId;

    // After all consumers migrate, then remove customerId
}
```

---

## Message Header Usage

Headers carry **metadata** without polluting the message body:

```java
// Producer: add headers to track the event
@Service
public class OrderEventPublisher {

    public void publishOrderPlaced(OrderPlacedEvent event) {
        // Attach headers: trace ID, source service, schema version
        ProducerRecord<String, Object> record = new ProducerRecord<>(
            "order.placed",
            null,              // partition (null = let Kafka decide)
            event.getOrderId(),
            event
        );

        // Distributed tracing — correlate logs across services
        record.headers().add("correlationId",
            UUID.randomUUID().toString().getBytes());

        // Which service published this (useful for debugging)
        record.headers().add("sourceService", "order-service".getBytes());

        // Schema version — consumers can adapt behavior
        record.headers().add("schemaVersion", "v2".getBytes());

        // Environment — prevents staging events hitting production consumers
        record.headers().add("environment",
            System.getenv("ENVIRONMENT").getBytes());

        kafkaTemplate.send(record);
    }
}

// Consumer: read headers for tracing
@KafkaListener(topics = "order.placed", ...)
public void onOrderPlaced(
        OrderPlacedEvent event,
        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
        @Header("correlationId") String correlationId,
        @Header("sourceService") String sourceService,
        Acknowledgment ack) {

    // Add correlationId to MDC for log tracing
    MDC.put("correlationId", correlationId);
    log.info("Processing order from {}: orderId={}", sourceService, event.getOrderId());
    // ... process
    MDC.clear();
    ack.acknowledge();
}
```

---

## Avro Setup (For Reference — Not in This Project)

If you wanted to add Avro, here's the setup:

```xml
<!-- pom.xml dependencies -->
<dependency>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-avro-serializer</artifactId>
    <version>7.5.0</version>
</dependency>
```

```yaml
# application.yaml
spring:
  kafka:
    properties:
      schema.registry.url: http://localhost:8081
    producer:
      value-serializer: io.confluent.kafka.serializers.KafkaAvroSerializer
    consumer:
      value-deserializer: io.confluent.kafka.serializers.KafkaAvroDeserializer
      properties:
        specific.avro.reader: true
```

```json
// order_placed.avsc (Avro schema)
{
  "type": "record",
  "name": "OrderPlacedEvent",
  "namespace": "com.fooddelivery.events",
  "fields": [
    {"name": "orderId",      "type": "string"},
    {"name": "customerId",   "type": "string"},
    {"name": "restaurantId", "type": "string"},
    {"name": "totalAmount",  "type": "double"},
    // Optional field with default — safe addition
    {"name": "promotionCode", "type": ["null", "string"], "default": null}
  ]
}
```

---

## Summary

| Format | Dev ease | Payload size | Schema enforcement | Evolution |
|--------|---------|-------------|-------------------|-----------|
| JSON | Easy | Large | None | Manual |
| Avro | Moderate | Small (3-5x) | Schema Registry | Automatic |
| Protobuf | Hard | Very small | Proto files | Automatic |

**Rule:** Use JSON to learn and prototype. Switch to Avro when you have multiple teams, schema changes need to be safe, or payload size matters.

**Next:** [08-advanced-patterns.md](08-advanced-patterns.md) — Dead letter queues, retries, idempotency
