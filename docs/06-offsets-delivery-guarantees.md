# 06 — Offsets & Delivery Guarantees

## The Core Problem

In a distributed system, things can always go wrong at the worst moment:

```
Kitchen Service receives order-789 event...
Starts processing (mark order as "Accepted" in DB)...
DB write succeeds...
Kafka consumer crashes before committing offset...
Kitchen Service restarts...
Kafka replays order-789...
Kitchen Service tries to accept the same order AGAIN
```

How do you handle this? This is where **delivery guarantees** and **idempotency** come in.

---

## Three Delivery Guarantees

### 1. At-Most-Once

Messages may be lost, but never duplicated.

```
Consumer logic:
  1. Receive message
  2. Commit offset    ← Mark as "done" in Kafka
  3. Process message  ← If this crashes, message is LOST (already committed)

Risk: Lost orders — unacceptable for a food delivery app
Use case: Metrics, logs, analytics where occasional loss is OK
```

### 2. At-Least-Once

Messages may be duplicated, but never lost.

```
Consumer logic:
  1. Receive message
  2. Process message  ← If this crashes, restart from last committed offset
  3. Commit offset    ← Only commit AFTER successful processing

Risk: Duplicate processing — must make processing idempotent
Use case: Most production systems (orders, payments)
```

### 3. Exactly-Once

Messages are processed exactly once — never lost, never duplicated.

```
Consumer logic:
  1. Receive message
  2. Begin transaction (DB + Kafka commit as atomic operation)
  3. Process message
  4. Commit DB write + Kafka offset atomically
  → If anything fails, entire transaction rolls back

Risk: Complex to implement, some performance overhead
Use case: Financial transactions, inventory deduction, payment processing
```

Kafka supports exactly-once semantics (EOS) since Kafka 0.11 via **transactions**.

---

## Offsets in Depth

### What is an Offset?

An offset is a monotonically increasing integer that uniquely identifies a message within a partition.

```
Partition 0 of order.placed:
Offset:   0         1         2         3         4
Message: [PLACED] [PLACED] [PLACED] [PLACED] [PLACED]
          order-1   order-2   order-3   order-4   order-5

Consumer committed offset: 3
→ Consumer has processed offsets 0, 1, 2, 3
→ Next poll will start from offset 4
```

### __consumer_offsets Topic

Kafka stores committed offsets in a special internal topic called `__consumer_offsets`. This is just another Kafka topic — durable and replicated.

```
Record in __consumer_offsets:
  Key:   {group: "kitchen-service-group", topic: "order.placed", partition: 0}
  Value: {offset: 42, metadata: "", timestamp: 1695432000000}
```

When a consumer restarts, it reads its committed offset from this topic and resumes from there.

---

## Implementing At-Least-Once (The Standard)

```java
@KafkaListener(topics = "order.placed", groupId = "kitchen-service-group")
public void onOrderPlaced(OrderPlacedEvent event, Acknowledgment ack) {

    try {
        // Step 1: Process (idempotently!)
        kitchenService.acceptOrder(event);

        // Step 2: Only commit after success
        // If the service crashes before this line, Kafka will redeliver the message
        ack.acknowledge();

    } catch (DuplicateOrderException e) {
        // Order already processed (duplicate delivery) — safe to ack and move on
        log.warn("Duplicate order detected, skipping: orderId={}", event.getOrderId());
        ack.acknowledge();

    } catch (Exception e) {
        // Don't ack on real errors — let Kafka redeliver
        // (handle with retry/DLQ — see 08-advanced-patterns.md)
        log.error("Processing failed: {}", e.getMessage());
        throw e; // Let error handler handle
    }
}
```

### Making Processing Idempotent

**Idempotent** = calling the same operation multiple times has the same effect as calling it once.

```java
@Service
public class KitchenService {

    /**
     * Idempotent order acceptance.
     * If called twice with the same orderId, the second call is a no-op.
     *
     * This handles the "at-least-once" case where Kafka might redeliver.
     */
    @Transactional
    public void acceptOrder(OrderPlacedEvent event) {
        // Check if already processed (idempotency check)
        if (kitchenOrderRepository.existsByOrderId(event.getOrderId())) {
            log.warn("Order already in kitchen, skipping duplicate: orderId={}",
                event.getOrderId());
            return; // Idempotent — second call does nothing
        }

        // First time processing this order
        KitchenOrder kitchenOrder = KitchenOrder.builder()
            .orderId(event.getOrderId())
            .restaurantId(event.getRestaurantId())
            .items(event.getItems())
            .status(KitchenOrderStatus.QUEUED)
            .receivedAt(Instant.now())
            .build();

        kitchenOrderRepository.save(kitchenOrder);
        log.info("Order queued in kitchen: orderId={}", event.getOrderId());
    }
}
```

---

## Implementing Exactly-Once

Kafka transactions ensure that a message offset commit and a database write happen **atomically**.

### Producer Side (Transactional Producer)

```java
@Configuration
public class TransactionalKafkaConfig {

    @Bean
    public ProducerFactory<String, Object> transactionalProducerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // Enable transactions — assigns a unique transactional ID to this producer
        // If producer restarts with same ID, Kafka can abort the in-flight transaction
        config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "order-service-tx-producer");

        return new DefaultKafkaProducerFactory<>(config);
    }
}
```

### Consumer → Process → Produce (Read-Process-Write)

```java
@Service
@Slf4j
public class PaymentProcessor {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final PaymentRepository paymentRepository;

    /**
     * Exactly-once payment processing.
     *
     * Scenario: Payment event comes in → deduct from account → publish payment.completed
     * We need: deduct exactly once AND publish exactly once, even if we crash mid-way.
     *
     * Kafka Transactions ensure:
     * - The DB write and Kafka offset commit are atomic
     * - If we publish payment.completed, we guaranteed processed the payment
     * - If we crash, neither the DB write nor the offset commit happen
     */
    @Transactional
    @KafkaListener(topics = "payment.requested", groupId = "payment-processor-group")
    public void processPayment(PaymentRequestedEvent event, Acknowledgment ack) {

        // Begin Kafka transaction
        kafkaTemplate.executeInTransaction(operations -> {
            // Step 1: Deduct from customer account (DB write)
            Payment payment = paymentRepository.processPayment(
                event.getOrderId(),
                event.getAmount(),
                event.getCustomerId()
            );

            // Step 2: Publish result to Kafka (within same transaction)
            if (payment.isSuccessful()) {
                operations.send("payment.completed",
                    event.getOrderId(),
                    new PaymentCompletedEvent(event.getOrderId(), payment.getTransactionId()));
            } else {
                operations.send("payment.failed",
                    event.getOrderId(),
                    new PaymentFailedEvent(event.getOrderId(), payment.getFailureReason()));
            }

            return true;
        });

        // Offset committed as part of the transaction above
        // No need for ack.acknowledge() here
    }
}
```

---

## Auto vs Manual Offset Commit

### Auto-Commit (Not Recommended for Orders)

```yaml
spring:
  kafka:
    consumer:
      enable-auto-commit: true
      auto-commit-interval: 5000  # Commit every 5 seconds
```

```
Timeline:
  t=0:  Receive order-789, start processing
  t=5:  AUTO-COMMIT fires (Kafka marks offset as done)
  t=6:  Service crashes while still processing
  t=∞:  Order-789 never finishes processing — LOST!
```

### Manual Commit (Recommended)

```java
// MANUAL_IMMEDIATE — commit exactly when you call ack.acknowledge()
@Bean
public ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory() {
    factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);
    return factory;
}
```

```
Timeline:
  t=0:  Receive order-789, start processing
  t=5:  Service crashes (no commit yet)
  t=10: Service restarts
  t=11: Kafka redelivers order-789 (offset not committed)
  t=15: Processing completes, ack.acknowledge() called
  t=15: Offset committed — message marked as done ✓
```

---

## Offset Reset Strategies

What happens when a consumer group starts fresh (first deployment) or offset is lost?

```yaml
spring:
  kafka:
    consumer:
      auto-offset-reset: earliest  # Start from the very first message in the topic
      # OR
      auto-offset-reset: latest    # Start from the newest message (ignore history)
      # OR
      auto-offset-reset: none      # Throw exception if no offset — force explicit reset
```

### When to Use Each

| Strategy | When to Use |
|----------|------------|
| `earliest` | First deployment — replay all events to build initial state |
| `latest` | Only care about new events from now on |
| `none` | Strict — fail if offset lost, don't silently skip events |

### Manually Seeking Offsets

```java
/**
 * Seek to a specific offset — useful for:
 * - Re-processing events (bug fix, data migration)
 * - Skipping a poisoned message
 * - Starting from a specific timestamp
 */
@Autowired
private KafkaListenerEndpointRegistry registry;

public void replayOrdersFrom(String groupId, long timestampEpochMs) {
    // Find the consumer container for kitchen-service-group
    MessageListenerContainer container = registry.getListenerContainer("kitchenOrderListener");
    container.pause();

    // Seek all partitions to messages from a specific timestamp
    // e.g., replay all orders from the last hour
    container.getAssignedPartitions().forEach(tp -> {
        // ... seek logic
    });

    container.resume();
}
```

---

## Summary

| Guarantee | Risk | Use Case | How to Implement |
|-----------|------|----------|-----------------|
| At-most-once | Message loss | Metrics, logs | Auto-commit before processing |
| At-least-once | Duplicate processing | Orders, notifications | Manual commit + idempotent logic |
| Exactly-once | Complexity, performance | Payments, inventory | Kafka transactions |

**Rule of thumb:** Use **at-least-once + idempotency** for 95% of cases. It's simpler and fast enough.

**Next:** [07-schema-serialization.md](07-schema-serialization.md) — JSON vs Avro and Schema Registry
