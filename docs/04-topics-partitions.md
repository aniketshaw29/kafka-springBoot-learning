# 04 — Topics & Partitions

## Topics

A **topic** is a named, ordered feed of records. Think of it as a Kafka equivalent of a database table — but append-only and readable by any number of consumers independently.

Topics are the main way services communicate in our food delivery platform:

| Topic | Producer | Consumers | Purpose |
|-------|---------|----------|---------|
| `order.placed` | Order Service | Kitchen, Notification, Analytics | New order broadcast |
| `order.status-updated` | Kitchen, Driver Service | Notification, Customer App, Analytics | Status change events |
| `driver.location-updated` | Driver App | Maps Service, Customer App | Real-time GPS |
| `payment.completed` | Payment Service | Order Service, Notification | Payment confirmation |
| `notification.send` | Any service | Notification Service | Centralized notification queue |

---

## Partitions — The Unit of Parallelism

Every topic is split into **partitions**. This is Kafka's secret to handling millions of events/second.

```
Topic: order.placed (3 partitions)
                                       ← newer
  Partition 0:  [evt0][evt1][evt2][evt3]
  Partition 1:  [evt0][evt1][evt2]
  Partition 2:  [evt0][evt1][evt2][evt3][evt4]
```

Each partition is:
- An **ordered, immutable log** — messages are appended, never changed
- Stored on **one broker** (with copies on replica brokers)
- Read by **one consumer per group** at a time

### Why Partitions Enable Scale

```
1 partition, 1 consumer:
  Broker: [order1][order2][order3][order4][order5]
  Consumer reads at 100 orders/sec

3 partitions, 3 consumers (parallel):
  Partition 0: [order1][order4]          Consumer A: 100 orders/sec
  Partition 1: [order2][order5]   →      Consumer B: 100 orders/sec
  Partition 2: [order3][order6]          Consumer C: 100 orders/sec
                                  Total: 300 orders/sec
```

The number of partitions determines the **maximum parallelism** — you can't have more consumers than partitions in a group.

---

## How Messages Are Assigned to Partitions

### With a Key (Consistent Hashing)

When a message has a key (like `orderId`), Kafka hashes the key to determine the partition:

```
partition = hash(key) % numPartitions

hash("order-789") % 3 = partition 1
hash("order-790") % 3 = partition 2
hash("order-791") % 3 = partition 0
```

**All events for the same order always go to the same partition.**

This guarantees ordering: `PLACED → ACCEPTED → PREPARING → DELIVERED` are always in sequence for a given order.

```
Partition 1: [order-789:PLACED][order-789:ACCEPTED][order-789:PREPARING][order-789:DELIVERED]
             ← time ───────────────────────────────────────────────────────────────────────→
```

### Without a Key (Round-Robin)

No key = Kafka distributes messages evenly across partitions. Good for maximizing throughput when order doesn't matter (e.g., driver location updates).

```
Update 1 → Partition 0
Update 2 → Partition 1
Update 3 → Partition 2
Update 4 → Partition 0
...
```

### Custom Partitioner

You can write your own partitioner. Example: route VIP orders to a dedicated partition:

```java
public class VipOrderPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes, Cluster cluster) {

        OrderPlacedEvent event = (OrderPlacedEvent) value;
        int numPartitions = cluster.partitionCountForTopic(topic);

        // VIP customers always go to partition 0 — processed with higher priority
        if (event.getCustomerTier() == CustomerTier.VIP) {
            return 0;
        }

        // Regular customers spread across remaining partitions
        return (Math.abs(key.hashCode()) % (numPartitions - 1)) + 1;
    }
}
```

---

## Replication — Fault Tolerance

Each partition has one **leader** and N-1 **replicas** (followers) on other brokers.

```
Topic: order.placed, Partition 0
  ┌──────────────────────────────────────────────────────┐
  │ Broker 1 (Leader)     Broker 2 (Replica)  Broker 3 (Replica) │
  │ [evt0][evt1][evt2]    [evt0][evt1][evt2]   [evt0][evt1][evt2] │
  └──────────────────────────────────────────────────────┘
         ↑                       ↑                    ↑
    Producer writes          Async sync            Async sync
    reads here
```

- Producers and consumers always talk to the **leader**
- Followers replicate asynchronously
- If the leader crashes, a follower is promoted to leader
- Data is safe as long as at least one replica is alive

### Replication Factor

```yaml
# Typical production settings:
replication.factor: 3          # 3 copies of every partition
min.insync.replicas: 2         # At least 2 must ack before confirming write
```

With `acks=all` and `min.insync.replicas=2`: even if one broker dies, your write succeeds.

---

## Topic Configuration

### Creating Topics in Spring Boot

```java
@Configuration
public class KafkaTopicConfig {

    /**
     * Spring Boot auto-creates topics if spring.kafka.admin.auto-create is true,
     * but it's better to define topics explicitly with the right partition count
     * and replication factor.
     */
    @Bean
    public NewTopic orderPlacedTopic() {
        return TopicBuilder.name("order.placed")
            .partitions(3)        // 3 partitions → 3 consumers can work in parallel
            .replicas(3)          // 3 copies for fault tolerance
            .config(TopicConfig.RETENTION_MS_CONFIG, "604800000") // Keep for 7 days
            .build();
    }

    @Bean
    public NewTopic orderStatusTopic() {
        return TopicBuilder.name("order.status-updated")
            .partitions(3)
            .replicas(3)
            .build();
    }

    @Bean
    public NewTopic driverLocationTopic() {
        // High-frequency GPS updates — more partitions for more parallelism
        // Also use log compaction: only keep the LATEST location per driverId
        return TopicBuilder.name("driver.location-updated")
            .partitions(12)       // 12 parallel consumers → handles many drivers
            .replicas(2)          // GPS updates are less critical, 2 replicas OK
            .config(TopicConfig.CLEANUP_POLICY_CONFIG, "compact")  // Keep latest per key
            .config(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.01")
            .build();
    }

    @Bean
    public NewTopic notificationTopic() {
        // Compacted + delete: keep last state but also expire old entries
        return TopicBuilder.name("notification.send")
            .partitions(6)
            .replicas(3)
            .config(TopicConfig.RETENTION_MS_CONFIG, "86400000") // Expire after 1 day
            .build();
    }
}
```

---

## Choosing the Right Number of Partitions

This is one of the most important decisions when designing a Kafka topic. **Partitions cannot be reduced later** (only increased), so start conservatively.

### Formula

```
partitions = max(throughput_needed / throughput_per_consumer,
                 throughput_needed / throughput_per_producer)
```

### Rules of Thumb

| Scenario | Recommended Partitions |
|----------|----------------------|
| Small app, 1-2 consumers | 1-3 |
| Medium app, team of services | 3-12 |
| High-throughput analytics | 12-100+ |
| Real-time GPS (many drivers) | 12-48 |
| Order events | 3-6 (ordering within an order matters) |

### Why NOT to Use Too Many Partitions

```
Too many partitions → more overhead:
- Each partition is a file handle on the broker
- Leader election is O(partitions)
- Rebalances are slower
- More memory used for offset tracking
```

**Rule:** Start with fewer partitions. You can increase them, but not decrease.

---

## Log Compaction

For some topics, you don't want to keep all history — you only want the **latest value per key**. Example: current driver location, current order status.

```
Normal topic (time-based retention):
  [driver-1: lat=12.9, lon=77.5]  ← eventually deleted after 7 days
  [driver-1: lat=12.9, lon=77.6]
  [driver-1: lat=12.9, lon=77.7]  ← current position

Compacted topic (key-based retention):
  [driver-1: lat=12.9, lon=77.7]  ← only latest value kept forever
```

Useful for:
- Current driver locations
- Current order status (what if a new service joins? replay from beginning = current state)
- User preferences
- Feature flags

---

## Topic Naming Conventions

Good topic names communicate both the domain and the event type:

```
# Pattern: <domain>.<entity>-<eventType>
order.placed
order.status-updated
order.cancelled
payment.completed
payment.failed
driver.location-updated
driver.accepted-order
notification.send
restaurant.menu-updated
```

**Avoid generic names like:**
- `orders` (which orders? placed? updated? all?)
- `events` (too broad)
- `service-a-to-service-b` (couples services to topic naming)

---

## Summary

| Concept | Key Takeaway |
|---------|-------------|
| Topics | Named channels — use one per event type |
| Partitions | Unit of parallelism — more = higher throughput |
| Keys | Ensures ordering — same key always same partition |
| Replication | Fault tolerance — replicas on other brokers |
| Retention | How long messages stay (time or log compaction) |

**Next:** [05-consumer-groups.md](05-consumer-groups.md) — How multiple consumers share work
