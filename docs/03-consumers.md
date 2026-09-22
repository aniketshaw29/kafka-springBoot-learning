# 03 — Consumers

## What is a Consumer?

A **consumer** reads (subscribes to) messages from a Kafka topic. Unlike HTTP, the consumer decides *when* to read — Kafka never pushes. The consumer **polls** Kafka at its own pace.

In our food delivery app:
- **Kitchen Service** — consumes `order.placed` to start preparing food
- **Notification Service** — consumes `order.placed` and `order.status-updated` to send SMS/email
- **Analytics Service** — consumes multiple topics to build dashboards

---

## How Consumers Work

```
                    ┌──────────────────────────────────────┐
                    │            Kafka Broker              │
                    │  Topic: order.placed                 │
                    │  Partition 0:  [0:evt][1:evt][2:evt] │
                    │  Partition 1:  [0:evt][1:evt]        │
                    └──────────────────────────────────────┘
                                      │
                          Consumer polls every 500ms
                                      │
                                      ▼
                    ┌──────────────────────────────────────┐
                    │        Consumer (Kitchen Service)    │
                    │                                      │
                    │  poll() → get batch of records       │
                    │  for each record:                    │
                    │    deserialize JSON                  │
                    │    process (update kitchen display)  │
                    │    commit offset (mark as done)      │
                    └──────────────────────────────────────┘
```

---

## Key Consumer Concepts

### 1. Poll Loop

Consumers use a **poll loop** — they repeatedly ask Kafka "give me the next batch of messages."

```java
// Conceptual poll loop (Spring Kafka handles this for you automatically)
while (true) {
    ConsumerRecords<String, OrderPlacedEvent> records = consumer.poll(Duration.ofMillis(500));
    for (ConsumerRecord<String, OrderPlacedEvent> record : records) {
        processOrder(record.value());
    }
    consumer.commitSync(); // Mark offset as processed
}
```

Spring's `@KafkaListener` abstracts this — you just annotate a method and it handles the loop.

### 2. Offset

The **offset** is the position of a message within a partition. It starts at 0 and increments.

```
Partition 0:  [0: OrderA][1: OrderB][2: OrderC][3: OrderD]
                                        ↑
                             Consumer's current offset = 2
                             "I've processed up to OrderB"
```

The consumer **commits** its offset to tell Kafka: "I've successfully processed up to this point." If the consumer crashes and restarts, it resumes from the last committed offset.

### 3. Auto vs Manual Offset Commit

| Mode | Behavior | Risk |
|------|---------|------|
| `enable.auto.commit=true` | Kafka commits offset every N seconds automatically | Can lose messages if consumer crashes between auto-commit |
| `enable.auto.commit=false` + `ackMode=MANUAL` | You explicitly commit after processing | Safer, you control exactly-once processing |

For critical events like orders, **manual commit** is safer.

### 4. Deserialization

Consumers deserialize the byte array from Kafka back into Java objects. Must match the producer's serializer:

- Producer used `JsonSerializer` → Consumer uses `JsonDeserializer`
- Producer used Avro → Consumer uses AvroDeserializer

### 5. Consumer Lag

**Lag** = how many messages the consumer is *behind* the latest message.

```
Topic partition: [0][1][2][3][4][5]  ← latest offset = 5
Consumer committed offset: 2
Lag = 5 - 2 = 3 (3 messages waiting to be processed)
```

High lag means your consumer is too slow. You either need to:
- Scale up consumers (more instances in the consumer group)
- Optimize the processing logic
- Increase partitions (allows more consumers)

---

## Spring Boot Consumer: Kitchen Service

### KafkaConsumerConfig.java

```java
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    /**
     * Consumer factory — creates the actual Kafka consumer instances.
     * Kitchen Service consumes OrderPlacedEvent messages.
     */
    @Bean
    public ConsumerFactory<String, OrderPlacedEvent> orderConsumerFactory() {
        Map<String, Object> config = new HashMap<>();

        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // Consumer group ID — all instances of Kitchen Service share this group
        // Kafka distributes partitions among group members
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "kitchen-service-group");

        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        // Trust our event classes for deserialization (security: whitelist specific packages)
        config.put(JsonDeserializer.TRUSTED_PACKAGES, "com.fooddelivery.shared.events");

        // Do NOT auto-commit — we want to control exactly when we mark a message as done
        // This prevents losing an order if the service crashes mid-processing
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // When a new consumer group joins, start from the earliest available message
        // This is useful when you first deploy — you won't miss any events
        // Change to "latest" if you only want messages from when you started
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Fetch up to 50 records per poll — tune based on how fast you can process
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);

        return new DefaultKafkaConsumerFactory<>(config,
            new StringDeserializer(),
            new JsonDeserializer<>(OrderPlacedEvent.class));
    }

    /**
     * Listener container factory — manages the poll loop and thread lifecycle.
     * MANUAL_IMMEDIATE ack mode: commit offset immediately when you call ack.acknowledge()
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderPlacedEvent>
            kafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<String, OrderPlacedEvent> factory =
            new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(orderConsumerFactory());

        // MANUAL_IMMEDIATE: we call ack.acknowledge() after successfully processing
        // This gives us full control over when the offset is committed
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        // Run 3 concurrent listener threads — each handles different partitions
        factory.setConcurrency(3);

        return factory;
    }
}
```

### KitchenOrderConsumer.java

```java
@Component
@Slf4j
public class KitchenOrderConsumer {

    private final KitchenOrderService kitchenOrderService;

    public KitchenOrderConsumer(KitchenOrderService kitchenOrderService) {
        this.kitchenOrderService = kitchenOrderService;
    }

    /**
     * Listens for new orders that need to be prepared.
     *
     * Real-life analogy: The kitchen display system (KDS) — a tablet in the kitchen
     * that shows new orders as they come in. When an order appears, kitchen staff
     * acknowledge it and start cooking.
     *
     * @param event      the order that was placed
     * @param ack        used to commit the offset after successful processing
     * @param metadata   Kafka metadata (topic, partition, offset) — useful for logging
     */
    @KafkaListener(
        topics = "order.placed",
        groupId = "kitchen-service-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void onOrderPlaced(
            OrderPlacedEvent event,
            Acknowledgment ack,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        log.info("Kitchen received order: orderId={}, partition={}, offset={}",
            event.getOrderId(), partition, offset);

        try {
            // Process the order — update kitchen display, assign to cook, etc.
            kitchenOrderService.acceptAndQueueOrder(event);

            // Only commit offset AFTER successful processing
            // If we crash here before acknowledging, we'll reprocess this order on restart
            // (this is at-least-once semantics — see 06-offsets-delivery-guarantees.md)
            ack.acknowledge();

            log.info("Order acknowledged by kitchen: orderId={}", event.getOrderId());

        } catch (KitchenCapacityException e) {
            // Kitchen is full — don't ack, message stays in partition for retry
            log.warn("Kitchen at capacity, will retry order: orderId={}", event.getOrderId());
            // Spring will retry based on retry config

        } catch (Exception e) {
            // Unexpected error — log and ack to avoid infinite loops
            // In production, send to Dead Letter Topic first (see 08-advanced-patterns.md)
            log.error("Failed to process order in kitchen: orderId={}, error={}",
                event.getOrderId(), e.getMessage(), e);
            ack.acknowledge(); // Still ack to avoid blocking the whole partition
        }
    }

    /**
     * Listens for order status updates (e.g., order cancelled — stop cooking).
     * Note: different topic, different listener method, same consumer group.
     */
    @KafkaListener(
        topics = "order.status-updated",
        groupId = "kitchen-service-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void onOrderStatusUpdated(OrderStatusUpdatedEvent event, Acknowledgment ack) {
        log.info("Kitchen received status update: orderId={}, status={}",
            event.getOrderId(), event.getNewStatus());

        if (event.getNewStatus() == OrderStatus.CANCELLED) {
            // Stop preparing this order
            kitchenOrderService.cancelOrder(event.getOrderId());
        }

        ack.acknowledge();
    }
}
```

### NotificationConsumer.java (separate service)

```java
@Component
@Slf4j
public class NotificationConsumer {

    private final SmsService smsService;
    private final EmailService emailService;
    private final CustomerRepository customerRepository;

    /**
     * Notification Service subscribes to both topics independently.
     * It uses a DIFFERENT consumer group than Kitchen Service.
     *
     * This is the magic of Kafka: the same "order.placed" event is delivered
     * to BOTH Kitchen Service and Notification Service independently.
     * They don't compete — they each get their own copy of every message.
     *
     * Analogy: A TV broadcast — when the channel broadcasts, every TV receives it.
     * Kitchen TV and Notification TV both tune in.
     */
    @KafkaListener(
        topics = "order.placed",
        groupId = "notification-service-group"  // Different group = independent consumption
    )
    public void onOrderPlaced(OrderPlacedEvent event, Acknowledgment ack) {
        log.info("Notification service received order: orderId={}", event.getOrderId());

        // Fetch customer details to get contact info
        Customer customer = customerRepository.findById(event.getCustomerId())
            .orElseThrow(() -> new CustomerNotFoundException(event.getCustomerId()));

        // Send SMS: "Your order #789 has been placed! Estimated time: 35 mins"
        smsService.send(
            customer.getPhoneNumber(),
            String.format("Your order #%s has been placed! Estimated delivery: 35 mins",
                event.getOrderId().substring(0, 8))
        );

        ack.acknowledge();
    }

    @KafkaListener(
        topics = "order.status-updated",
        groupId = "notification-service-group"
    )
    public void onOrderStatusUpdated(OrderStatusUpdatedEvent event, Acknowledgment ack) {
        // Different status → different notification
        String message = switch (event.getNewStatus()) {
            case ACCEPTED   -> "Your order has been accepted by the restaurant!";
            case PREPARING  -> "The kitchen is now preparing your order.";
            case PICKED_UP  -> "Your order has been picked up by the driver.";
            case DELIVERED  -> "Your order has been delivered. Enjoy your meal!";
            case CANCELLED  -> "Your order has been cancelled. Refund processing.";
            default         -> "Your order status has been updated: " + event.getNewStatus();
        };

        // Would fetch customer details and send actual SMS/push notification
        log.info("Would send notification: orderId={}, message={}", event.getOrderId(), message);

        ack.acknowledge();
    }
}
```

---

## Consumer Lifecycle

```
1. Consumer starts
   → Joins consumer group
   → Kafka assigns partitions (rebalance)

2. Poll loop begins
   → Fetch records from assigned partitions
   → Process records
   → Commit offsets

3. Consumer stops/crashes
   → Kafka detects it (missed heartbeat after session.timeout.ms)
   → Rebalance: redistribute partitions to remaining consumers
   → Remaining consumers resume from last committed offset
```

---

## Commit Strategies

### At-Least-Once (Default safe choice)

```java
// Commit AFTER processing
processRecord(record);
ack.acknowledge(); // Only commit after successful processing
```

Risk: If service crashes between processing and ack, message is reprocessed.
Mitigation: Make processing **idempotent** — reprocessing the same order twice has the same result.

### At-Most-Once (Rarely used for orders)

```java
// Commit BEFORE processing
ack.acknowledge(); // Commit first
processRecord(record); // If this crashes, message is lost
```

OK for analytics events where occasional loss is acceptable.

---

## Common Consumer Mistakes

### 1. Blocking the poll thread

```java
// BAD — blocks the poll loop, Kafka thinks consumer is dead
@KafkaListener(topics = "order.placed", ...)
public void onOrderPlaced(OrderPlacedEvent event) {
    Thread.sleep(30_000); // Simulating slow DB call
    // Kafka sees no heartbeat → assumes consumer is dead → rebalance!
}

// GOOD — use async processing or increase max.poll.interval.ms
```

### 2. Committing before processing

```java
// BAD — if processing fails, offset already committed → message lost
ack.acknowledge();
kitchenService.processOrder(event); // crash here = lost order

// GOOD — process first, then commit
kitchenService.processOrder(event);
ack.acknowledge();
```

### 3. Wrong group ID per service

```java
// BAD — both services share a group, compete for messages
@KafkaListener(groupId = "food-delivery-app", ...)  // in Kitchen Service
@KafkaListener(groupId = "food-delivery-app", ...)  // in Notification Service

// GOOD — separate groups, each service gets all messages
@KafkaListener(groupId = "kitchen-service-group", ...)
@KafkaListener(groupId = "notification-service-group", ...)
```

---

## Summary

| Consumer Setting | Kitchen Service | Notification Service |
|-----------------|----------------|---------------------|
| Group ID | `kitchen-service-group` | `notification-service-group` |
| Ack Mode | `MANUAL_IMMEDIATE` | `MANUAL_IMMEDIATE` |
| `auto.offset.reset` | `earliest` | `earliest` |
| Concurrency | 3 | 2 |
| Topics | `order.placed`, `order.status-updated` | `order.placed`, `order.status-updated` |

**Next:** [04-topics-partitions.md](04-topics-partitions.md) — How topics and partitions enable scale
