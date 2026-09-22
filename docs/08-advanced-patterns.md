# 08 — Advanced Patterns

## Overview

This document covers production patterns for making Kafka consumers robust:
1. **Dead Letter Topics (DLT)** — park failed messages for investigation
2. **Retry with Backoff** — retry transiently failing messages
3. **Error Handling** — differentiate permanent vs transient failures
4. **Idempotent Consumers** — safely handle duplicate deliveries
5. **Outbox Pattern** — guarantee event publication alongside DB writes
6. **Saga Pattern** — coordinate multi-step workflows via events

---

## 1. Dead Letter Topics (DLT)

A **Dead Letter Topic** is where messages go when they can't be processed after all retries. It's like the "undeliverable mail" bin at a post office.

```
Normal flow:
  order.placed → Kitchen Service (success) ✓

Failed flow:
  order.placed → Kitchen Service (fail, retry 1)
              → Kitchen Service (fail, retry 2)
              → Kitchen Service (fail, retry 3)
              → order.placed.DLT  ← archived for manual inspection
```

### Why DLT?

Without DLT:
- A single bad message **blocks** the entire partition
- All orders behind it in the partition are stuck
- Consumer lag grows indefinitely

With DLT:
- Failed message is moved out of the way
- Processing of other messages continues
- Failed message is preserved for debugging/replay

### Spring Boot DLT Configuration

```java
@Configuration
public class KitchenKafkaConfig {

    /**
     * Configure retry + DLT behavior for kitchen-service-group.
     *
     * Retry strategy:
     *   - Retry up to 3 times with exponential backoff (1s, 2s, 4s)
     *   - If still failing: route to order.placed.DLT
     */
    @Bean
    public DefaultErrorHandler kitchenErrorHandler(KafkaTemplate<String, Object> kafkaTemplate) {

        // Dead letter publisher — routes to "<topic>.DLT" by default
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
            (record, exception) -> {
                // Custom DLT topic routing — can also route based on exception type
                log.error("Routing to DLT: topic={}, key={}, exception={}",
                    record.topic(), record.key(), exception.getMessage());

                // Route to "<original-topic>.DLT"
                return new TopicPartition(record.topic() + ".DLT", -1); // -1 = any partition
            }
        );

        // Exponential backoff: 1s, 2s, 4s — max 3 retries
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(3);
        backOff.setInitialInterval(1_000L);   // 1 second initial wait
        backOff.setMultiplier(2.0);           // Double wait each time
        backOff.setMaxInterval(10_000L);      // Cap at 10 seconds

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);

        // Don't retry on these exceptions — they're permanent failures, go straight to DLT
        errorHandler.addNotRetryableExceptions(
            InvalidOrderException.class,        // Bad data — retrying won't fix it
            RestaurantClosedException.class,    // Restaurant closed — retry won't help
            JsonProcessingException.class       // Malformed JSON — can't deserialize, skip
        );

        // Always retry on these transient exceptions
        errorHandler.addRetryableExceptions(
            DatabaseTimeoutException.class,     // DB might recover
            ServiceUnavailableException.class,  // Downstream service might recover
            OptimisticLockException.class       // Concurrent modification — retry is safe
        );

        return errorHandler;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderPlacedEvent>
            kitchenListenerContainerFactory(
                ConsumerFactory<String, OrderPlacedEvent> consumerFactory,
                DefaultErrorHandler kitchenErrorHandler) {

        ConcurrentKafkaListenerContainerFactory<String, OrderPlacedEvent> factory =
            new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(kitchenErrorHandler); // Attach error handler
        factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);

        return factory;
    }
}
```

### DLT Consumer (Monitor & Replay)

```java
@Component
@Slf4j
public class DltConsumer {

    private final AlertingService alertingService;
    private final DltMessageRepository dltRepository;

    /**
     * Monitors the Dead Letter Topic.
     * Does NOT auto-retry — that's the engineer's decision.
     * Instead: alert, store for investigation, optionally replay after fix.
     */
    @KafkaListener(
        topics = "order.placed.DLT",
        groupId = "dlt-monitor-group"
    )
    public void onDeadLetterMessage(
            ConsumerRecord<String, String> record,  // Raw string — might be malformed
            @Header(KafkaHeaders.EXCEPTION_MESSAGE) String exceptionMessage,
            @Header(KafkaHeaders.EXCEPTION_FQCN) String exceptionClass,
            @Header(KafkaHeaders.ORIGINAL_TOPIC) String originalTopic,
            @Header(KafkaHeaders.ORIGINAL_PARTITION) int originalPartition,
            @Header(KafkaHeaders.ORIGINAL_OFFSET) long originalOffset,
            Acknowledgment ack) {

        log.error("DLT message: topic={}, partition={}, offset={}, exception={}: {}",
            originalTopic, originalPartition, originalOffset,
            exceptionClass, exceptionMessage);

        // Store in DB for inspection and potential replay
        dltRepository.save(DltMessage.builder()
            .originalTopic(originalTopic)
            .originalPartition(originalPartition)
            .originalOffset(originalOffset)
            .messageKey(record.key())
            .messageValue(record.value())
            .exceptionClass(exceptionClass)
            .exceptionMessage(exceptionMessage)
            .receivedAt(Instant.now())
            .status(DltMessageStatus.PENDING_REVIEW)
            .build());

        // Alert the on-call engineer
        alertingService.sendAlert(String.format(
            "Order processing failure: orderId=%s, error=%s",
            record.key(), exceptionMessage));

        ack.acknowledge();
    }
}
```

---

## 2. Retry with Backoff

For transient failures (DB timeout, downstream service slow), retry with **exponential backoff** instead of retrying immediately (which just hammers an already-slow system).

```
First failure:
  t=0:  Processing fails (DB timeout)
  t=1:  Retry 1 (wait 1 second)
  t=3:  Retry 2 (wait 2 seconds) — still failing
  t=7:  Retry 3 (wait 4 seconds) — still failing
  t=7:  Route to DLT

DB recovers at t=5:
  t=3:  Retry 2 still fails (DB recovering)
  t=7:  Retry 3 succeeds! ✓ (DB recovered)
```

This is already handled by `DefaultErrorHandler` with `ExponentialBackOffWithMaxRetries` (see above).

---

## 3. The Outbox Pattern

### The Problem

```java
// WRONG — race condition / inconsistent state
@Transactional
public Order placeOrder(PlaceOrderRequest request) {
    Order order = orderRepository.save(buildOrder(request));  // DB commit

    // What if Kafka publish FAILS here?
    // DB has the order, but no event was published.
    // Kitchen never knows. Order hangs forever.
    kafkaTemplate.send("order.placed", order.getId(), buildEvent(order));

    return order;
}
```

### The Solution: Transactional Outbox

```
Instead of publishing directly to Kafka:
  1. Within the same DB transaction, write event to an "outbox" table
  2. A separate process (Debezium/polling) reads the outbox and publishes to Kafka
  3. On success: mark outbox entry as published
  4. On failure: retry from outbox

DB transaction:
  BEGIN
    INSERT INTO orders (...)
    INSERT INTO outbox_events (topic, key, payload, status='PENDING')
  COMMIT

Outbox publisher (async):
  SELECT * FROM outbox_events WHERE status='PENDING'
  FOR EACH event: publish to Kafka → UPDATE status='PUBLISHED'
```

```java
// OutboxEvent.java
@Entity
@Table(name = "outbox_events")
public class OutboxEvent {
    @Id
    private String eventId;
    private String topic;
    private String messageKey;

    @Column(columnDefinition = "TEXT")
    private String payload;       // JSON string

    private String eventType;
    private OutboxStatus status;  // PENDING, PUBLISHED, FAILED

    private Instant createdAt;
    private Instant publishedAt;
    private int retryCount;
}

// OrderService.java — outbox pattern
@Service
public class OrderService {

    @Transactional
    public Order placeOrder(PlaceOrderRequest request) {
        // Step 1: Save order to DB
        Order order = orderRepository.save(buildOrder(request));

        // Step 2: Save event to outbox IN THE SAME TRANSACTION
        // If DB transaction rolls back, the event is also rolled back — consistent!
        OutboxEvent outboxEvent = OutboxEvent.builder()
            .eventId(UUID.randomUUID().toString())
            .topic("order.placed")
            .messageKey(order.getOrderId())
            .payload(objectMapper.writeValueAsString(buildOrderPlacedEvent(order)))
            .eventType("OrderPlacedEvent")
            .status(OutboxStatus.PENDING)
            .createdAt(Instant.now())
            .build();

        outboxRepository.save(outboxEvent);

        return order;
        // DB transaction commits here — both order AND outbox event saved atomically
    }
}

// OutboxPublisher.java — scheduled publisher
@Component
@Slf4j
public class OutboxPublisher {

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    /**
     * Polls the outbox table every second and publishes pending events to Kafka.
     * In production, replace this with Debezium (CDC) for lower latency.
     */
    @Scheduled(fixedDelay = 1000)
    @Transactional
    public void publishPendingEvents() {
        List<OutboxEvent> pending = outboxRepository
            .findTop100ByStatusOrderByCreatedAtAsc(OutboxStatus.PENDING);

        for (OutboxEvent event : pending) {
            try {
                kafkaTemplate.send(event.getTopic(), event.getMessageKey(), event.getPayload())
                    .get(5, TimeUnit.SECONDS); // Synchronous — ensure delivery before marking done

                event.setStatus(OutboxStatus.PUBLISHED);
                event.setPublishedAt(Instant.now());
                outboxRepository.save(event);

            } catch (Exception e) {
                log.error("Failed to publish outbox event: eventId={}, attempt={}",
                    event.getEventId(), event.getRetryCount() + 1);

                event.setRetryCount(event.getRetryCount() + 1);
                if (event.getRetryCount() >= 5) {
                    event.setStatus(OutboxStatus.FAILED);  // Alert on-call
                }
                outboxRepository.save(event);
            }
        }
    }
}
```

---

## 4. Saga Pattern (Event-Driven Transactions)

A **Saga** breaks a complex business transaction into steps, each handled by a different service via events. If one step fails, compensating events undo previous steps.

### Order Placement Saga

```
Order Placed
    │
    ▼
[Order Service] → order.placed event
    │
    ▼
[Payment Service] → listens for order.placed
                  → processes payment
                  → publishes payment.completed OR payment.failed
    │
    ▼
[Inventory Service] → listens for payment.completed
                    → reserves ingredients
                    → publishes inventory.reserved OR inventory.failed
    │
    ▼
[Kitchen Service] → listens for inventory.reserved
                  → starts cooking
                  → publishes order.preparing

Failure path (payment.failed):
[Order Service] → listens for payment.failed
              → cancels order
              → publishes order.cancelled
[Notification Service] → sends "payment failed" SMS
```

```java
// Choreography-based Saga — each service reacts to events from the previous step
// No central coordinator needed

// Payment Service
@KafkaListener(topics = "order.placed", groupId = "payment-service-group")
public void onOrderPlaced(OrderPlacedEvent event, Acknowledgment ack) {
    try {
        // Process payment
        PaymentResult result = paymentGateway.charge(
            event.getCustomerId(), event.getTotalAmount());

        if (result.isSuccessful()) {
            // Trigger next saga step
            eventPublisher.publish("payment.completed",
                new PaymentCompletedEvent(event.getOrderId(), result.getTransactionId()));
        } else {
            // Trigger compensating step
            eventPublisher.publish("payment.failed",
                new PaymentFailedEvent(event.getOrderId(), result.getDeclineReason()));
        }

        ack.acknowledge();

    } catch (Exception e) {
        log.error("Payment processing error: {}", e.getMessage());
        throw e; // Will retry or go to DLT
    }
}

// Order Service — handles saga compensation
@KafkaListener(topics = "payment.failed", groupId = "order-service-group")
public void onPaymentFailed(PaymentFailedEvent event, Acknowledgment ack) {
    // Compensating action: cancel the order
    orderRepository.findByOrderId(event.getOrderId()).ifPresent(order -> {
        order.setStatus(OrderStatus.CANCELLED);
        order.setCancellationReason("Payment failed: " + event.getDeclineReason());
        orderRepository.save(order);
    });

    // Notify next steps via event
    eventPublisher.publish("order.status-updated",
        new OrderStatusUpdatedEvent(event.getOrderId(), null, OrderStatus.CANCELLED,
            "system", event.getDeclineReason(), Instant.now()));

    ack.acknowledge();
}
```

---

## 5. Conditional Listener (Pause/Resume)

Dynamically pause consumers (e.g., during maintenance or backpressure):

```java
@Component
public class KitchenCapacityController {

    private final KafkaListenerEndpointRegistry registry;

    /**
     * Pause consuming new orders when kitchen is at 100% capacity.
     * This creates backpressure — orders wait in Kafka instead of overloading the kitchen.
     * Consumer lag will grow, but messages won't be lost.
     */
    public void pauseOrderConsumption() {
        MessageListenerContainer container =
            registry.getListenerContainer("kitchenOrderListener");

        if (container != null && !container.isContainerPaused()) {
            container.pause();
            log.info("Kitchen order consumption PAUSED — at capacity");
        }
    }

    public void resumeOrderConsumption() {
        MessageListenerContainer container =
            registry.getListenerContainer("kitchenOrderListener");

        if (container != null && container.isContainerPaused()) {
            container.resume();
            log.info("Kitchen order consumption RESUMED");
        }
    }
}
```

---

## Summary of Patterns

| Pattern | Problem Solved | Complexity |
|---------|---------------|-----------|
| Dead Letter Topic | Prevent bad messages blocking partitions | Low |
| Retry with Backoff | Transient failures (DB timeouts) | Low |
| Outbox Pattern | Guaranteed event publication with DB | Medium |
| Saga | Distributed transactions across services | High |
| Pause/Resume | Backpressure control | Low |
| Idempotent Consumer | At-least-once duplicate handling | Medium |

---

## Production Checklist

- [ ] DLT configured for every critical consumer
- [ ] Retry with exponential backoff configured
- [ ] Non-retryable exceptions listed (bad data, business rule violations)
- [ ] DLT consumer + alerting set up
- [ ] Idempotency key checked before processing
- [ ] Consumer offset committed manually (not auto)
- [ ] Outbox pattern for any event that must be published with a DB write
- [ ] Consumer lag monitored and alerts configured
- [ ] Rebalance listeners clean up state correctly
- [ ] Schema evolution rules followed (no breaking changes)
