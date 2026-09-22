# Kafka + Spring Boot Learning Project

## Real-World Scenario: Food Delivery Platform

This project teaches Apache Kafka concepts using a **food delivery platform** (think Zomato, Uber Eats) as the running example. Events like "Order Placed", "Restaurant Accepted", "Driver Assigned", "Order Delivered" flow through Kafka between microservices.

---

## Why Kafka? The Problem It Solves

Without Kafka, a food delivery app might look like this:

```
Customer App → [HTTP] → Order Service → [HTTP] → Kitchen Service
                                      → [HTTP] → Notification Service
                                      → [HTTP] → Analytics Service
```

Problems:
- If Notification Service is down, the order fails or you lose the notification
- Order Service must know about every downstream service
- Heavy traffic causes cascading failures

With Kafka:

```
Customer App → Order Service → [Kafka Topic: order.placed] → Kitchen Service
                                                            → Notification Service
                                                            → Analytics Service
```

Benefits:
- Services are decoupled — Order Service doesn't know who is listening
- Downstream services can be down and catch up later (message replay)
- Scales independently per service

---

## Project Structure

```
kafka-springBoot-learning/
│
├── docs/                          # Concept explanation docs (read these first!)
│   ├── 01-kafka-fundamentals.md   # What is Kafka, brokers, clusters
│   ├── 02-producers.md            # How messages are sent
│   ├── 03-consumers.md            # How messages are received
│   ├── 04-topics-partitions.md    # How Kafka organizes data
│   ├── 05-consumer-groups.md      # Parallel processing & load balancing
│   ├── 06-offsets-delivery-guarantees.md  # At-least-once, exactly-once
│   ├── 07-schema-serialization.md # JSON vs Avro, Schema Registry
│   └── 08-advanced-patterns.md    # Dead letter queues, retries, idempotency
│
├── order-service/                 # Spring Boot app — PRODUCER
│   └── src/main/java/...          # Publishes order events to Kafka
│
├── kitchen-service/               # Spring Boot app — CONSUMER
│   └── src/main/java/...          # Listens for orders, updates status
│
├── notification-service/          # Spring Boot app — CONSUMER
│   └── src/main/java/...          # Sends SMS/email on order events
│
├── shared-models/                 # Shared DTOs and event classes
│
├── docker-compose.yml             # Kafka + Zookeeper + Kafka UI setup
└── README.md                      # This file
```

---

## Learning Path (Follow This Order)

| Step | File | Concept |
|------|------|---------|
| 1 | [01-kafka-fundamentals.md](docs/01-kafka-fundamentals.md) | Brokers, topics, clusters — the big picture |
| 2 | [02-producers.md](docs/02-producers.md) | How Order Service sends events |
| 3 | [03-consumers.md](docs/03-consumers.md) | How Kitchen/Notification Service receives events |
| 4 | [04-topics-partitions.md](docs/04-topics-partitions.md) | Partitioning for scale |
| 5 | [05-consumer-groups.md](docs/05-consumer-groups.md) | Parallel processing, rebalancing |
| 6 | [06-offsets-delivery-guarantees.md](docs/06-offsets-delivery-guarantees.md) | Reliability guarantees |
| 7 | [07-schema-serialization.md](docs/07-schema-serialization.md) | JSON vs Avro |
| 8 | [08-advanced-patterns.md](docs/08-advanced-patterns.md) | DLQ, retries, idempotency |

---

## Running the Project

### 1. Start Kafka with Docker

```bash
docker-compose up -d
```

This starts:
- **Zookeeper** on port 2181 (Kafka's coordination service)
- **Kafka Broker** on port 9092
- **Kafka UI** on http://localhost:8080 (visual dashboard)

### 2. Start the Services

```bash
# Terminal 1 — Order Service (producer)
cd order-service && mvn spring-boot:run

# Terminal 2 — Kitchen Service (consumer)
cd kitchen-service && mvn spring-boot:run

# Terminal 3 — Notification Service (consumer)
cd notification-service && mvn spring-boot:run
```

### 3. Place a Test Order

```bash
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "cust-123",
    "restaurantId": "rest-456",
    "items": [{"name": "Pizza Margherita", "quantity": 2, "price": 12.99}],
    "deliveryAddress": "123 Main St, Bengaluru"
  }'
```

Watch the Kitchen Service and Notification Service terminals — they will log their processing of the event.

### 4. Explore the Kafka UI

Open http://localhost:8080 and browse:
- **Topics** — see `order.placed`, `order.status-updated`, `notification.send`
- **Messages** — inspect the actual JSON payloads
- **Consumer Groups** — see lag and partition assignments

---

## Key Kafka Terms Cheatsheet

| Term | Food Delivery Analogy |
|------|-----------------------|
| **Broker** | The Kafka server — like a post office |
| **Topic** | A category of messages — like a mailbox labeled "orders" |
| **Partition** | A physical file on disk — like a shelf in the mailbox |
| **Producer** | Order Service — the one putting mail in the box |
| **Consumer** | Kitchen/Notification Service — the one taking mail out |
| **Consumer Group** | A team sharing work — multiple kitchen staff sharing a queue |
| **Offset** | Position in a partition — like a bookmark |
| **Message** | An event/record — the actual order slip |

---

## Prerequisites

- Java 17+
- Maven 3.8+
- Docker & Docker Compose
- Basic Spring Boot knowledge
