# 02 — Producers

## What is a Producer?

A **producer** is any application that writes (publishes) messages to a Kafka topic. In our food delivery platform, the **Order Service** is the primary producer — it emits events whenever something happens to an order.

---

## How a Producer Works

```
                    ┌──────────────────────────────────────┐
                    │          Producer (Order Service)     │
                    │                                       │
                    │  1. Create message                    │
                    │  2. Serialize (JSON/Avro)             │
                    │  3. Decide partition (by key/round-   │
                    │     robin / custom partitioner)       │
                    │  4. Batch messages                    │
                    │  5. Compress (optional)               │
                    │  6. Send to broker                    │
                    │  7. Wait for acknowledgment (acks)    │
                    └──────────────────────────────────────┘
                                      │
                                      ▼
                    ┌──────────────────────────────────────┐
                    │            Kafka Broker              │
                    │  Topic: order.placed                 │
                    │  Partition 0: [...][OrderPlaced]     │
                    └──────────────────────────────────────┘
```

---

## Key Producer Concepts

### 1. Acknowledgment Modes (acks)

When you send a message, how confident do you need to be that it was actually stored?

| `acks` value | Meaning | Risk | Use Case |
|-------------|---------|------|----------|
| `0` | Fire-and-forget, no ack | Can lose messages | Metrics, logs where loss is OK |
| `1` | Leader broker confirms | Lose if leader crashes before replication | Moderate importance |
| `all` or `-1` | All replicas confirm | Lowest risk | Orders, payments — critical data |

**In our food delivery app:** Order events use `acks=all` — losing an order would be a disaster.

### 2. Partitioning Strategy

When a producer sends a message, Kafka must decide which partition it goes to:

- **With a key** (e.g., `orderId`): Hash the key to always route to the same partition.
  - All events for `order-789` land in the same partition → strict ordering per order
- **Without a key**: Round-robin across partitions (maximizes throughput, no order guarantee)
- **Custom partitioner**: You write logic (e.g., route VIP customers to dedicated partitions)

```
Message key: "order-789"
Hash("order-789") % 3 partitions = partition 1
→ All events for order-789 always go to partition 1
→ Consumer sees them in order: PLACED → ACCEPTED → PREPARING → DELIVERED
```

### 3. Batching & Linger

Producers don't send each message immediately. They batch messages to improve throughput:

- `linger.ms` — wait up to N milliseconds to collect more messages before sending
- `batch.size` — send when batch reaches N bytes

```
# Low latency (e.g., alerts):
linger.ms=0, batch.size=small

# High throughput (e.g., analytics):
linger.ms=10, batch.size=large
```

### 4. Retries & Idempotency

Networks fail. Kafka producers automatically retry failed sends.

Problem: If the broker received the message but the ack got lost, a retry causes a **duplicate**.

Solution: **Idempotent producer** (`enable.idempotence=true`)
- Kafka assigns each message a sequence number
- Broker detects and discards duplicates
- You get exactly-once delivery to the broker

### 5. Compression

Producers can compress messages before sending:
- `snappy` — fast, moderate compression (good default)
- `gzip` — slower, better compression
- `lz4` — very fast
- `zstd` — best compression ratio

Useful when sending large JSON payloads (like order details with many items).

---

## Spring Boot Producer: Order Service

### KafkaProducerConfig.java

```java
@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    /**
     * Factory for creating KafkaTemplate instances.
     * KafkaTemplate is the Spring abstraction over KafkaProducer.
     */
    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> config = new HashMap<>();

        // Address of the Kafka broker(s). In production: comma-separated list of all brokers.
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // Key is the orderId (String) — used for partition routing
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // Value is our event object serialized as JSON
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        // Wait for all replicas to acknowledge — safe for order events
        config.put(ProducerConfig.ACKS_CONFIG, "all");

        // Automatically deduplicates retries — prevents duplicate orders
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // Retry up to 3 times on transient failures
        config.put(ProducerConfig.RETRIES_CONFIG, 3);

        // Wait 5ms to batch more messages together (slight latency for better throughput)
        config.put(ProducerConfig.LINGER_MS_CONFIG, 5);

        // Compress with snappy (fast compression for JSON payloads)
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
```

### OrderEventPublisher.java

```java
@Service
@Slf4j
public class OrderEventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OrderEventPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Publishes an "order placed" event.
     *
     * The key is orderId — this ensures all events for the same order
     * land in the same partition, preserving chronological order per order.
     *
     * Example flow:
     *   Customer places order → this method called → event in order.placed topic
     *   → Kitchen Service picks it up → order starts being prepared
     */
    public void publishOrderPlaced(OrderPlacedEvent event) {
        // Topic name as key — Spring routes to "order.placed" topic
        // Key = orderId — guarantees all events for this order go to same partition
        CompletableFuture<SendResult<String, Object>> future =
            kafkaTemplate.send("order.placed", event.getOrderId(), event);

        // Handle success/failure asynchronously
        future.whenComplete((result, ex) -> {
            if (ex != null) {
                // Production: send to alerting, dead letter queue, or fallback DB
                log.error("Failed to publish OrderPlacedEvent for orderId={}: {}",
                    event.getOrderId(), ex.getMessage());
            } else {
                // Log the offset where the message was stored — useful for debugging
                log.info("OrderPlacedEvent published: orderId={}, topic={}, partition={}, offset={}",
                    event.getOrderId(),
                    result.getRecordMetadata().topic(),
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());
            }
        });
    }

    /**
     * Publishes an order status update.
     * Called when: restaurant accepts, driver picks up, order delivered, etc.
     */
    public void publishOrderStatusUpdated(OrderStatusUpdatedEvent event) {
        kafkaTemplate.send("order.status-updated", event.getOrderId(), event)
            .whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to publish OrderStatusUpdatedEvent: {}", ex.getMessage());
                } else {
                    log.info("Status update published: orderId={}, status={}",
                        event.getOrderId(), event.getNewStatus());
                }
            });
    }
}
```

### OrderService.java (calls the publisher)

```java
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
     * Creates an order and publishes the event to Kafka.
     *
     * Transactional pattern: save to DB first, then publish event.
     * If Kafka publish fails, we still have the order in DB and can retry.
     */
    public Order placeOrder(PlaceOrderRequest request) {
        // 1. Save to database (source of truth)
        Order order = new Order();
        order.setOrderId(UUID.randomUUID().toString());
        order.setCustomerId(request.getCustomerId());
        order.setRestaurantId(request.getRestaurantId());
        order.setItems(request.getItems());
        order.setStatus(OrderStatus.PLACED);
        order.setCreatedAt(Instant.now());
        orderRepository.save(order);

        // 2. Build the event (what we broadcast to other services)
        OrderPlacedEvent event = OrderPlacedEvent.builder()
            .orderId(order.getOrderId())
            .customerId(order.getCustomerId())
            .restaurantId(order.getRestaurantId())
            .items(order.getItems())
            .totalAmount(order.getTotalAmount())
            .deliveryAddress(request.getDeliveryAddress())
            .timestamp(Instant.now())
            .build();

        // 3. Publish to Kafka — kitchen, notifications, analytics all get notified
        eventPublisher.publishOrderPlaced(event);

        log.info("Order placed and event published: orderId={}", order.getOrderId());
        return order;
    }
}
```

---

## Producer Internal Workflow

When `kafkaTemplate.send(...)` is called:

```
1. Serialization
   OrderPlacedEvent → JSON bytes
   "order-789" (key) → UTF-8 bytes

2. Partitioner
   hash("order-789") % numPartitions → partition 1

3. RecordAccumulator (batch buffer)
   Add to batch for broker-1, partition-1

4. Sender thread (background)
   When linger.ms expires OR batch is full:
   → Send batch to broker
   → Wait for acks

5. Broker stores message
   Assigns offset (e.g., offset 42)
   Replicates to follower brokers

6. Ack returned
   CompletableFuture resolves with offset 42
```

---

## Common Producer Mistakes

### 1. Fire-and-forget for critical events

```java
// BAD — no error handling
kafkaTemplate.send("order.placed", event);

// GOOD — handle failures
kafkaTemplate.send("order.placed", event)
    .whenComplete((result, ex) -> {
        if (ex != null) { /* handle error */ }
    });
```

### 2. Wrong acks setting for critical data

```yaml
# BAD for orders — can lose messages
spring.kafka.producer.acks: 1

# GOOD for orders
spring.kafka.producer.acks: all
```

### 3. Not using a key when ordering matters

```java
// BAD — round-robin, order events can arrive out of order
kafkaTemplate.send("order.status-updated", event);

// GOOD — same orderId always goes to same partition → in-order delivery
kafkaTemplate.send("order.status-updated", event.getOrderId(), event);
```

---

## Summary

| Setting | Development | Production (orders) |
|---------|------------|---------------------|
| `acks` | `1` | `all` |
| `enable.idempotence` | `false` | `true` |
| `retries` | `0` | `3+` |
| `linger.ms` | `0` | `5-10` |
| `compression` | none | `snappy` or `lz4` |

**Next:** [03-consumers.md](03-consumers.md) — How Kitchen and Notification services receive events
