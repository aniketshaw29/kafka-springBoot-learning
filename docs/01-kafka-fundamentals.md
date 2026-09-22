# 01 — Kafka Fundamentals

## What Is Apache Kafka?

Apache Kafka is a **distributed event streaming platform**. Think of it as a highly scalable, fault-tolerant, ordered **log** that multiple services can write to and read from.

It was originally built at LinkedIn to handle their activity stream (clicks, likes, messages). Today it processes **trillions of events per day** at companies like Uber, Airbnb, Netflix, and LinkedIn.

---

## The Core Problem Kafka Solves

### Without Kafka — Tight Coupling

Imagine our food delivery app at 1,000 orders/minute:

```
Order Service ──HTTP──▶ Kitchen Service       (fails? order lost)
             ──HTTP──▶ Notification Service   (slow? order delayed)
             ──HTTP──▶ Analytics Service      (scaling issues?)
             ──HTTP──▶ Driver Assignment      (new service? must update Order Service)
```

**Problems:**
- Order Service must know every downstream service
- If Notification Service crashes, either Order Service fails too, or we write complex retry logic
- Adding a new service means changing Order Service
- Spikes in traffic hit all services simultaneously

### With Kafka — Decoupling via Events

```
Order Service ──publish──▶ [Kafka] order.placed topic
                                        │
                           ┌────────────┼────────────┐
                           ▼            ▼            ▼
                   Kitchen Service  Notification  Analytics
                   (subscribes)     Service       Service
                                    (subscribes)  (subscribes)
```

**Solutions:**
- Order Service only writes to Kafka — it doesn't know who's listening
- Downstream services can crash, restart, and **replay missed events** from exactly where they left off
- Add a new service? Just subscribe to the topic — zero changes to Order Service
- Kafka acts as a buffer during traffic spikes

---

## Kafka Architecture — The Building Blocks

### 1. Broker

A **broker** is a single Kafka server process. It:
- Receives messages from producers
- Stores messages on disk
- Serves messages to consumers

```
                  ┌─────────────────────┐
                  │    Kafka Broker      │
                  │                     │
                  │  Topic: order.placed │
                  │  ┌─────────────────┐ │
                  │  │ Partition 0     │ │
                  │  │ [msg0][msg1]... │ │
                  │  ├─────────────────┤ │
                  │  │ Partition 1     │ │
                  │  │ [msg0][msg1]... │ │
                  │  └─────────────────┘ │
                  └─────────────────────┘
```

### 2. Cluster

A **cluster** is a group of brokers working together. This provides:
- **Fault tolerance** — if one broker fails, others take over
- **Scalability** — distribute load across machines

In production, you typically run **3+ brokers**.

```
                  ┌──────────┐   ┌──────────┐   ┌──────────┐
                  │ Broker 1 │   │ Broker 2 │   │ Broker 3 │
                  │(Leader)  │   │(Replica) │   │(Replica) │
                  └──────────┘   └──────────┘   └──────────┘
                        │               │               │
                        └───────────────┴───────────────┘
                                        │
                              ZooKeeper (coordinator)
```

### 3. ZooKeeper (Legacy) / KRaft (Modern)

**ZooKeeper** was Kafka's coordination service — it tracked:
- Which broker is the leader for each partition
- Which brokers are alive
- Topic configuration

As of Kafka 3.x, Kafka switched to **KRaft** (Kafka Raft) — a built-in consensus mechanism that removes the ZooKeeper dependency. However, ZooKeeper is still widely used in production and in tutorials.

For this learning project, we use **ZooKeeper** (simpler to set up with Docker).

### 4. Topic

A **topic** is a named channel/feed of messages. It's like a table in a database but append-only and ordered.

Examples in our food delivery app:
- `order.placed` — someone placed an order
- `order.status-updated` — order status changed (accepted, preparing, picked up, delivered)
- `notification.send` — a notification needs to be sent
- `driver.location-updated` — real-time driver GPS updates

### 5. Partition

Each topic is split into **partitions** — ordered, immutable sequences of messages stored on disk. Partitions are how Kafka achieves parallel processing.

```
Topic: order.placed
                                    offset
                  Partition 0:  [0][1][2][3][4][5]  ← newest
                  Partition 1:  [0][1][2][3]
                  Partition 2:  [0][1][2][3][4]
```

- Messages within a partition are strictly ordered
- Messages across partitions have no guaranteed order
- Each partition lives on one broker (with replicas on others)

### 6. Message (Record)

A Kafka message contains:
- **Key** (optional) — used to determine which partition the message goes to
- **Value** — the actual data (often JSON or Avro)
- **Timestamp** — when the message was created
- **Headers** (optional) — metadata (trace ID, source service, etc.)

```json
{
  "key": "order-789",
  "value": {
    "orderId": "order-789",
    "customerId": "cust-123",
    "restaurantId": "rest-456",
    "status": "PLACED",
    "items": [{"name": "Pizza", "qty": 2}]
  },
  "timestamp": 1695432000000,
  "headers": {
    "correlationId": "abc-xyz",
    "sourceService": "order-service"
  }
}
```

### 7. Producer

Any application that **writes messages** to Kafka. In our app, the Order Service is a producer — it publishes `OrderPlacedEvent` to `order.placed`.

### 8. Consumer

Any application that **reads messages** from Kafka. Kitchen Service and Notification Service are consumers — they subscribe to topics and process events.

---

## How Kafka Stores Data

Kafka stores messages on disk as **log segments** (files). This is different from traditional message queues like RabbitMQ which delete messages after delivery.

```
/kafka-data/
  order.placed-0/          ← partition 0 of topic "order.placed"
    00000000000000000000.log   ← segment file (messages)
    00000000000000000000.index ← offset index (for fast lookup)
    00000000000000000000.timeindex
```

Key properties:
- **Retention period** — messages are kept for N days (default 7 days), regardless of whether anyone read them
- **Log compaction** — for "state" topics, Kafka only keeps the latest message per key (useful for "current driver location")
- **Immutable** — once written, messages are never modified

This means:
- Consumers can **replay** history (re-process past orders for a bug fix)
- Multiple consumers can read the same message independently
- Slow consumers don't cause messages to pile up and crash

---

## Real Numbers (Why This Matters)

| System | Throughput | Latency | Durability |
|--------|-----------|---------|-----------|
| Kafka | Millions msg/sec | ~5ms | Yes (replicated) |
| RabbitMQ | Hundreds of thousands | ~1ms | Yes |
| HTTP REST | Tens of thousands | ~10-50ms | No |

Kafka isn't always the right choice — for a simple app, HTTP or RabbitMQ is simpler. But when you need:
- High throughput (millions of events/sec)
- Message replay
- Multiple independent consumers
- Long-term event storage
- Event sourcing

...Kafka is the standard choice.

---

## Summary

| Concept | What it is | Food delivery analogy |
|---------|-----------|----------------------|
| Broker | Kafka server process | Post office building |
| Cluster | Group of brokers | Post office network (multiple buildings) |
| Topic | Named message channel | A mailbox category ("Orders", "Notifications") |
| Partition | Ordered log within a topic | Individual stack of mail in a mailbox |
| Message | A record/event | A single piece of mail |
| Producer | Writer of messages | Person putting mail in the box |
| Consumer | Reader of messages | Person collecting mail from the box |
| Offset | Position in a partition | Page number in a book |

**Next:** [02-producers.md](02-producers.md) — How Order Service sends events to Kafka
