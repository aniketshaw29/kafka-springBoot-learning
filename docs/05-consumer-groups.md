# 05 — Consumer Groups

## What is a Consumer Group?

A **consumer group** is a set of consumers that cooperate to consume a topic. Kafka distributes the topic's partitions among the group members, so each partition is consumed by exactly one consumer at a time.

This is how Kafka enables **horizontal scaling** of consumers — add more consumers to a group, process faster.

---

## The Core Rule

> **One partition is assigned to at most one consumer per group.**

```
Topic: order.placed (3 partitions)

Kitchen Service Group (3 instances):
  Consumer A → Partition 0
  Consumer B → Partition 1
  Consumer C → Partition 2

  Result: All 3 consumers work in parallel ✓

Notification Service Group (2 instances):
  Consumer D → Partitions 0 and 2
  Consumer E → Partition 1

  Result: Consumer D handles twice the load — suboptimal ⚠
```

The maximum useful parallelism = number of partitions.

---

## Visualizing Consumer Groups

```
                    Topic: order.placed
                 ┌──────────────────────┐
                 │  Partition 0         │
                 │  Partition 1         │
                 │  Partition 2         │
                 └──────────────────────┘
                            │
              ┌─────────────┴─────────────┐
              │                           │
    ┌─────────────────────┐    ┌─────────────────────────┐
    │  kitchen-service-   │    │  notification-service-  │
    │  group              │    │  group                  │
    │  ┌─────┐ ┌─────┐    │    │  ┌─────────────────┐    │
    │  │ K-1 │ │ K-2 │    │    │  │      N-1        │    │
    │  │ P0  │ │ P1,2│    │    │  │   P0, P1, P2    │    │
    │  └─────┘ └─────┘    │    │  └─────────────────┘    │
    └─────────────────────┘    └─────────────────────────┘
         kitchen-service-group has 2 instances
         notification-service-group has 1 instance
```

Both groups independently consume **all messages**. They don't compete — Kafka delivers the same messages to both groups.

---

## Rebalancing

A **rebalance** happens when the partition assignments change:
- A new consumer joins the group
- A consumer leaves or crashes
- The topic gets more partitions

During a rebalance, all consumers in the group **pause** briefly while Kafka redistributes partitions.

### Example: Kitchen Service Scale-Up

```
Before scale-up (2 consumers, 4 partitions):
  Kitchen-1: Partition 0, Partition 1
  Kitchen-2: Partition 2, Partition 3

New Kitchen-3 joins → REBALANCE

After rebalance (3 consumers, 4 partitions):
  Kitchen-1: Partition 0, Partition 1
  Kitchen-2: Partition 2
  Kitchen-3: Partition 3
             ↑
  Rebalanced — Kitchen-3 takes Partition 3 from Kitchen-2
```

### Example: Consumer Crash

```
Before crash (3 consumers, 3 partitions):
  Kitchen-1: Partition 0
  Kitchen-2: Partition 1
  Kitchen-3: Partition 2  ← crashes!

Kafka detects crash (after session.timeout.ms = 30s) → REBALANCE

After rebalance (2 consumers, 3 partitions):
  Kitchen-1: Partition 0, Partition 2  ← picks up Kitchen-3's work
  Kitchen-2: Partition 1
```

No messages are lost — Kitchen-1 resumes from the last committed offset of Partition 2.

---

## Group Coordinator & Session Timeout

```
Consumer → heartbeat every heartbeat.interval.ms → Group Coordinator (broker)

If coordinator doesn't receive heartbeat within session.timeout.ms:
  → Consumer declared dead
  → Rebalance triggered
```

Typical settings:
```yaml
session.timeout.ms: 30000     # 30s — how long before a consumer is declared dead
heartbeat.interval.ms: 10000  # 10s — how often consumer says "I'm alive"
max.poll.interval.ms: 300000  # 5 min — max time between polls before considered dead
```

**Problem:** If your processing takes longer than `max.poll.interval.ms`, Kafka kicks you out of the group (rebalance). Common mistake with slow DB calls or heavy computation.

---

## Spring Boot: Multiple Consumers

### Running Multiple Instances (Scale Out)

Simply start multiple instances of the same service:

```bash
# Terminal 1
SERVER_PORT=8082 mvn spring-boot:run

# Terminal 2
SERVER_PORT=8083 mvn spring-boot:run

# Terminal 3
SERVER_PORT=8084 mvn spring-boot:run
```

All three have the same `groupId = "kitchen-service-group"`, so Kafka distributes partitions among them automatically.

### Concurrency Within One Instance

```java
@Bean
public ConcurrentKafkaListenerContainerFactory<String, OrderPlacedEvent>
        kafkaListenerContainerFactory() {

    ConcurrentKafkaListenerContainerFactory<String, OrderPlacedEvent> factory =
        new ConcurrentKafkaListenerContainerFactory<>();

    factory.setConsumerFactory(orderConsumerFactory());

    // 3 concurrent threads in THIS JVM — each gets a partition
    // Works well if you have 3+ partitions and want to maximize CPU on one machine
    factory.setConcurrency(3);

    return factory;
}
```

### Assigning Partition per Consumer (Static Assignment)

By default, Kafka dynamically rebalances partitions when consumers join/leave. This causes brief pauses.

For high-frequency data (like GPS), you can use **static membership** to avoid unnecessary rebalances:

```java
// application.yaml
spring:
  kafka:
    consumer:
      # Stable member ID — avoids rebalance if consumer restarts quickly (within session.timeout)
      group-instance-id: kitchen-service-instance-1
```

Or manually assign partitions:

```java
/**
 * Direct partition assignment — no coordinator, no rebalance.
 * Useful when:
 * - You have a fixed number of consumers (Kubernetes StatefulSet)
 * - Rebalances cause unacceptable latency spikes
 */
@KafkaListener(
    topicPartitions = @TopicPartition(
        topic = "driver.location-updated",
        partitionOffsets = @PartitionOffset(partition = "0", initialOffset = "0")
    ),
    groupId = "maps-service-group"
)
public void onDriverLocationUpdated(DriverLocationEvent event, Acknowledgment ack) {
    mapsService.updateDriverPosition(event.getDriverId(), event.getLatitude(), event.getLongitude());
    ack.acknowledge();
}
```

---

## Separate Groups for Different Purposes

A powerful pattern: multiple consumer groups read the same topic independently for different purposes.

```
Topic: order.placed
           │
           ├── kitchen-service-group     → Start preparing food
           ├── notification-service-group → Send SMS to customer
           ├── analytics-service-group   → Update order metrics dashboard
           ├── loyalty-service-group     → Award loyalty points
           └── fraud-detection-group     → Check for suspicious orders
```

Each group:
- Gets every message independently
- Progresses at its own speed (no coupling)
- Can be paused and replayed independently

This is **fan-out** — one event triggers many reactions.

---

## Consumer Group Lag Monitoring

**Lag** = latest offset in partition − consumer's committed offset = messages yet to process.

```
Partition 0:  latest offset = 1000
Kitchen-1 committed offset = 990
Lag = 10 (10 messages queued up)

If lag keeps growing → consumer is too slow → scale out!
```

Monitor lag in Kafka UI at http://localhost:8080 → Consumer Groups → kitchen-service-group.

Or via command line:
```bash
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group kitchen-service-group
```

Output:
```
GROUP                 TOPIC          PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
kitchen-service-group order.placed   0          990             1000            10
kitchen-service-group order.placed   1          500             500             0
kitchen-service-group order.placed   2          750             755             5
```

---

## Rebalance Listeners (Advanced)

You can hook into rebalance events to clean up state:

```java
@Component
public class KitchenRebalanceListener implements ConsumerAwareRebalanceListener {

    private final KitchenInProgressCache inProgressCache;

    /**
     * Called BEFORE Kafka reassigns partitions.
     * Use to save any in-memory state that belongs to partitions you're losing.
     */
    @Override
    public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer,
                                                 Collection<TopicPartition> partitions) {
        log.info("Partitions revoked — flushing in-progress orders: {}", partitions);
        // Save any in-progress cooking state to Redis before losing these partitions
        inProgressCache.flushToRedis(partitions);
    }

    /**
     * Called AFTER Kafka assigns new partitions.
     * Use to load state for newly assigned partitions.
     */
    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer,
                                      Collection<TopicPartition> partitions) {
        log.info("Partitions assigned — loading state: {}", partitions);
        // Load any saved state from Redis for the newly assigned partitions
        inProgressCache.loadFromRedis(partitions);
    }
}
```

---

## Summary

| Scenario | Recommendation |
|---------|---------------|
| Each service should get all messages | Use separate group IDs per service |
| Need to process faster | Add more consumer instances (same group) |
| Max consumers per group | = Number of partitions |
| Consumers > partitions | Extra consumers idle — add more partitions |
| Static workload, avoid rebalances | Use `group.instance.id` (static membership) |

**Next:** [06-offsets-delivery-guarantees.md](06-offsets-delivery-guarantees.md) — At-least-once vs exactly-once processing
