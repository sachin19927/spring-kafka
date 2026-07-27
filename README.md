# Spring Kafka Microservices - Event-Driven Order Processing

A production-ready Spring Boot microservices application demonstrating Apache Kafka integration with Schema Registry for event-driven architecture. This project implements a complete order processing pipeline with three microservices communicating through Kafka topics using Avro serialization.

**Updated**: Now includes Schema Registry for schema management, Confluent Avro serialization, and KRaft mode Kafka cluster.

## 🏗️ Architecture Overview

This project implements an event-driven architecture with three microservices:

```
┌──────────────────────┐
│    order-init        │
│  (Producer)          │
│   Port: 8084         │
└──────────┬───────────┘
           │ Sends Order Events
           │
┌──────────▼──────────────────────────────┐
│         Apache Kafka Cluster            │
│    (KRaft Mode - 3 Brokers)             │
│  Bootstrap: localhost:9092,9093,9094   │
│    Topic: order-events (3 partitions)   │
└──────────┬───────────────────────────────┘
           │
    ┌──────┴──────────────┬──────────────────────┐
    │                     │                      │
┌───▼──────────────┐  ┌──▼─────────────────┐  ┌─▼──────────────────┐
│ order-process    │  │ alert-notification │  │ Schema Registry    │
│ (Consumer)       │  │ (Consumer)         │  │ (Port: 7074)       │
│ Port: 8082       │  │ Port: 8083         │  │ (Schema mgmt)      │
└──────────────────┘  └────────────────────┘  └────────────────────┘
```

## 📦 Modules

### 1. order-init (Port: 8084)
**Role**: Order Entry Service (Producer)
- REST API endpoint for creating orders
- Produces order events to Kafka topic "order-events"
- Uses orderId as message key for partition consistency
- **Serialization**: Confluent Avro with auto schema registration

**Key Components**:
- `OrderController`: REST endpoints (`POST /api/orders`)
- `OrderInitService`: Kafka message producer with synchronous send
- `OrderRequest`: DTO for order creation
- `Order`: Avro-generated event model

**Port**: `8084`

### 2. order-process (Port: 8082)
**Role**: Order Processing Service (Consumer)
- Consumes orders from Kafka topic "order-events"
- Processes business logic (inventory validation, payment processing, status updates)
- Consumer group: `order-processing-group`
- **Serialization**: Confluent Avro with schema registry deserialization

**Key Components**:
- `OrderProcessingService`: Main business logic processor with `@KafkaListener`
- `KafkaConsumerConfig`: Consumer configuration with Avro deserializer
- Validates inventory, processes payments, updates order status
- **Concurrency**: 3 parallel threads (matches partition count)

**Port**: `8082`

### 3. alert-notification (Port: 8083)
**Role**: Alert & Notification Service (Consumer)
- Consumes orders from same Kafka topic for alerting purposes
- Handles high-value order alerts (> $1000) and cancellation notifications
- Consumer group: `alert-notification-group` (independent processing)
- **Serialization**: Confluent Avro with schema registry deserialization

**Key Components**:
- `AlertNotificationService`: Alert processing with `@KafkaListener`
- `KafkaConsumerConfig`: Consumer configuration with Avro deserializer
- Sends alerts for high-value orders and cancellations
- **Concurrency**: 3 parallel threads (matches partition count)

**Port**: `8083`

### 4. shared-events (Shared Library)
**Role**: Common Event Models and Schema Definitions
- Contains Avro schema definitions (`src/main/avro/`)
- Auto-generated Java classes from Avro schemas
- Shared DTOs and event models for all services

**Key Components**:
- `Order`: Main event model (Avro-generated)
- `OrderStatus`: Enum for order statuses (CREATED, COMPLETED, FAILED, etc.)
- Maven plugin automatically compiles `.avsc` files to Java classes

## 🚀 Technology Stack

| Component | Version | Purpose |
|-----------|---------|---------|
| **Java** | 23 | Programming language |
| **Spring Boot** | 4.0.6 | Application framework |
| **Spring Kafka** | 4.0.5 | Kafka integration |
| **Apache Kafka** | Latest (KRaft) | Message broker |
| **Confluent Schema Registry** | 7.7.1 | Schema management |
| **Confluent Avro Serializer** | 7.7.1 | Schema-based serialization |
| **Apache Avro** | 1.12.0 | Data serialization format |
| **Maven** | 3.6+ | Build & dependency management |
| **Lombok** | 1.18.36 | Boilerplate reduction |
| **Jackson** | 2.21.2 | JSON processing |
| **Docker** | Latest | Container runtime |
| **Docker Compose** | 3.8+ | Infrastructure orchestration |

### Key Features Implemented
- ✅ **Schema Registry Integration**: Centralized schema management with Confluent
- ✅ **Avro Serialization**: Efficient, schema-based message serialization
- ✅ **KRaft Mode**: Kafka without Zookeeper (3-broker cluster)
- ✅ **Synchronous Producer**: OrderInitService waits for send confirmation
- ✅ **Proper Property Placeholders**: Fixed `${}` syntax in `@KafkaListener`
- ✅ **Consumer Groups**: Independent processing via separate consumer groups
- ✅ **Parallel Processing**: Configurable concurrency matching partition count
- ✅ **Error Handling**: Proper exception propagation and logging

## 📋 Prerequisites

- Java 23 or higher
- Maven 3.6+
- Docker and Docker Compose
- Git
- curl (for testing)

## 🛠️ Complete Setup & Installation Guide

### Step 1: Clone the Repository
```bash
git clone <repository-url>
cd spring-kafka
```

### Step 2: Set Environment Variables

**Windows (PowerShell)**:
```powershell
$env:KAFKA_EXTERNAL_HOST = "localhost"
$env:SCHEMA_REGISTRY_URL = "http://localhost:7074"
```

**Windows (CMD)**:
```cmd
set KAFKA_EXTERNAL_HOST=localhost
set SCHEMA_REGISTRY_URL=http://localhost:7074
```

**Linux/macOS (Bash)**:
```bash
export KAFKA_EXTERNAL_HOST=localhost
export SCHEMA_REGISTRY_URL=http://localhost:7074
```

### Step 3: Start Kafka Infrastructure with KRaft Mode

Navigate to docker-compose files directory:
```bash
cd docker-compose-files
```

**Start Kafka Cluster (3 Brokers with KRaft)**:
```bash
docker-compose -f docker-compose-kafka-kraft.yaml \
  --env-file .env.local up -d
```

Wait 15-20 seconds for Kafka cluster to stabilize:
```bash
docker logs kafka1  # Check for leader election
```

**Start Schema Registry & Kafka REST API**:
```bash
docker-compose -f docker-compose.schema-rest.yaml \
  --env-file .env.local up -d
```

**Verify Services**:
```bash
# Check running containers
docker ps

# Test Kafka connectivity
telnet localhost 9092

# Test Schema Registry
curl http://localhost:7074/subjects
# Should return: []

# Check logs
docker logs schema-registry
```

### Step 4: Build All Modules

From project root:
```bash
# Build all modules
mvn clean package -DskipTests

# Or with tests (requires Kafka running)
mvn clean package
```

Build output:
```
✓ shared-events-1.0.0.jar (Shared models & schemas)
✓ order-init-1.0.0.jar (Producer service)
✓ order-process-1.0.0.jar (Consumer service)
✓ alert-notification-1.0.0.jar (Consumer service)
```

### Step 5: Run the Services

You can run services in multiple ways:

#### Option A: Maven (Best for Development)

**Terminal 1 - Order Init (Producer)**:
```bash
cd spring-kafka-parent/order-init
mvn spring-boot:run
```

Expected output:
```
Started OrderInitApplication in 7.9 seconds
Tomcat initialized with port 8084 (http)
```

**Terminal 2 - Order Process (Consumer)**:
```bash
cd spring-kafka-parent/order-process
mvn spring-boot:run
```

Expected output:
```
Started OrderProcessApplication in 9.5 seconds
Subscribed to topic(s): {order-events}
Successfully synced group in generation
```

**Terminal 3 - Alert Notification (Consumer)**:
```bash
cd spring-kafka-parent/alert-notification
mvn spring-boot:run
```

Expected output:
```
Started AlertNotificationApplication in 9.3 seconds
Subscribed to topic(s): {order-events}
Successfully synced group in generation
```

#### Option B: Docker (Production-like)

```bash
# Build Docker images for each service
docker build -t order-init:latest spring-kafka-parent/order-init/
docker build -t order-process:latest spring-kafka-parent/order-process/
docker build -t alert-notification:latest spring-kafka-parent/alert-notification/

# Run services
docker run -e KAFKA_EXTERNAL_HOST=host.docker.internal \
           -e SCHEMA_REGISTRY_URL=http://host.docker.internal:7074 \
           -p 8084:8084 order-init:latest

docker run -e KAFKA_EXTERNAL_HOST=host.docker.internal \
           -e SCHEMA_REGISTRY_URL=http://host.docker.internal:7074 \
           -p 8082:8082 order-process:latest

docker run -e KAFKA_EXTERNAL_HOST=host.docker.internal \
           -e SCHEMA_REGISTRY_URL=http://host.docker.internal:7074 \
           -p 8083:8083 alert-notification:latest
```

#### Option C: JAR Files

```bash
cd spring-kafka-parent

java -jar order-init/target/order-init-1.0.0.jar
java -jar order-process/target/order-process-1.0.0.jar
java -jar alert-notification/target/alert-notification-1.0.0.jar
```

### Step 6: Verify All Services Are Running

```bash
# Check if services are responsive
curl http://localhost:8084/actuator/health  # order-init
curl http://localhost:8082/actuator/health  # order-process
curl http://localhost:8083/actuator/health  # alert-notification

# Should return: {"status":"UP"}
```

## 🎯 Testing & API Usage

### Test 1: Create an Order
```bash
curl -X POST http://localhost:8084/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-001",
    "product": "Laptop",
    "quantity": 1,
    "amount": 1200.50
  }'
```

**Expected Response** (HTTP 202):
```json
{
  "orderId": "550e8400-e29b-41d4-a716-446655440000",
  "status": "CREATED",
  "message": "Order accepted"
}
```

### Test 2: Create a High-Value Order (Triggers Alert)
```bash
curl -X POST http://localhost:8084/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-002",
    "product": "Gaming Laptop",
    "quantity": 1,
    "amount": 1500.00
  }'
```

The alert-notification service will log:
```
HIGH VALUE ORDER ALERT: Order ... amount: $1500.00
```

### Test 3: Create Multiple Orders (Test Throughput)
```bash
for i in {1..10}; do
  curl -X POST http://localhost:8084/api/orders \
    -H "Content-Type: application/json" \
    -d "{
      \"customerId\": \"CUST-00$i\",
      \"product\": \"Product $i\",
      \"quantity\": $i,
      \"amount\": $((i * 100))
    }"
  echo "\n"
done
```

### Monitor Message Flow

**View Logs**:
```bash
# order-init logs (show produced messages)
# order-process logs (show processing)
# alert-notification logs (show alerts)
```

**Check Kafka Topic**:
```bash
docker exec kafka1 kafka-topics --bootstrap-server localhost:29092 \
  --describe --topic order-events
```

**View Schema Registry Schemas**:
```bash
curl http://localhost:7074/subjects

# Get specific schema
curl http://localhost:7074/subjects/order-events-value/versions
```

## 📊 Data Model

### Order (Avro Schema)
Defined in: `shared-events/src/main/avro/Order.avsc`

```json
{
  "type": "record",
  "name": "Order",
  "namespace": "com.spring.poc.kafka.events",
  "fields": [
    {"name": "orderId", "type": "string"},
    {"name": "customerId", "type": "string"},
    {"name": "product", "type": "string"},
    {"name": "quantity", "type": "int"},
    {"name": "amount", "type": "double"},
    {"name": "status", "type": {"type": "enum", "name": "OrderStatus", 
                                "symbols": ["CREATED", "COMPLETED", "FAILED"]}},
    {"name": "message", "type": "string"},
    {"name": "createdTimestamp", "type": "long"},
    {"name": "updatedTimestamp", "type": "long"}
  ]
}
```

### OrderStatus Enum
```
CREATED    → Order received by producer
COMPLETED  → Order successfully processed
FAILED     → Order processing failed
```

## ⚙️ Configuration Details

### Environment Variables
```bash
# Kafka Configuration
KAFKA_EXTERNAL_HOST=localhost          # Kafka broker host
SCHEMA_REGISTRY_URL=http://localhost:7074  # Schema Registry URL (MUST include http://)

# Optional
KAFKA_PORT1=9092
KAFKA_PORT2=9093
KAFKA_PORT3=9094
SCHEMA_REGISTRY_PORT=7074
KAFKA_REST_PORT=7075
```

### Producer Configuration (order-init)

**File**: `order-init/src/main/resources/application.yml`

```yaml
spring.kafka:
  bootstrap-servers: localhost:9092,localhost:9093,localhost:9094
  producer:
    key-serializer: StringSerializer
    value-serializer: KafkaAvroSerializer
    acks: all                    # Wait for all replicas
    retries: 3                   # Auto-retry on failure
    properties:
      schema.registry.url: http://localhost:7074
      auto.register.schemas: true  # Auto-create schemas in registry
      enable.idempotence: true   # Prevent duplicate messages
```

**Key Settings**:
- **Acks = all**: Ensures message durability (slower but safer)
- **Retries = 3**: Automatic retry with exponential backoff
- **Idempotence**: Prevents duplicates even on network failures
- **Synchronous Send**: OrderInitService calls `.get()` on send future

### Consumer Configuration (order-process & alert-notification)

**File**: `order-process/src/main/resources/application.yml`

```yaml
spring.kafka:
  bootstrap-servers: localhost:9092,localhost:9093,localhost:9094
  consumer:
    group-id: order-processing-group    # Unique per service
    auto-offset-reset: earliest         # Start from beginning if no offset
    key-deserializer: StringDeserializer
    value-deserializer: KafkaAvroDeserializer
    properties:
      schema.registry.url: http://localhost:7074
      specific.avro.reader: true        # Use Avro-generated classes
  listener:
    concurrency: 3                      # Match partition count
```

**Key Settings**:
- **group-id**: Different for each service (independent processing)
- **auto-offset-reset: earliest**: Consume all messages
- **concurrency: 3**: 3 threads per container (matches 3 partitions)
- **specific.avro.reader**: Deserialize to generated Java classes

### Kafka Topic Configuration

**Topic**: `order-events`
- **Partitions**: 3 (allows parallel processing)
- **Replication Factor**: 3 (high availability)
- **Min In-Sync Replicas**: 2 (durability guarantee)

## 🔄 Message Flow Diagram

```
1. CLIENT REQUEST
   └─> POST /api/orders (HTTP 202)

2. PRODUCER (order-init:8084)
   └─> OrderInitService.createOrder()
       ├─> Build Order event (Avro)
       ├─> Register schema with Schema Registry
       ├─> Send to Kafka (SYNCHRONOUS - waits for confirmation)
       └─> Return OrderResponse to client

3. KAFKA CLUSTER (KRaft mode)
   └─> Partition based on orderId (key)
       ├─> Replica 1 (sync)
       ├─> Replica 2 (sync)
       └─> Replica 3 (async)

4. PARALLEL CONSUMER PROCESSING
   ├─> order-process consumer group:1 (order-processing-group)
   │   └─> OrderProcessingService.processOrder()
   │       ├─> Validate inventory
   │       ├─> Process payment
   │       ├─> Update order status to COMPLETED
   │       └─> Log success
   │
   └─> alert-notification consumer group:2 (alert-notification-group)
       └─> AlertNotificationService.handleOrderForAlert()
           ├─> Check if amount > $1000
           ├─> Send high-value alert if true
           ├─> Check if status == FAILED
           ├─> Send cancellation alert if true
           └─> Log alert
```

## 🔧 Recent Changes & Fixes (v1.1)

### Critical Fixes Applied
1. ✅ **Fixed Schema Registry Port**: Changed from 8081 → 7074 (correct mapped port)
2. ✅ **Fixed Environment Variables**: Now requires `SCHEMA_REGISTRY_URL=http://localhost:7074` (with protocol)
3. ✅ **Fixed Consumer Topic Resolution**: Changed `{app.kafka.topics.order-events}` → `${app.kafka.topics.order-events}`
4. ✅ **Fixed Consumer GroupId Resolution**: Changed `{spring.kafka.consumer.group-id}` → `${spring.kafka.consumer.group-id}`
5. ✅ **Made Producer Synchronous**: OrderInitService now waits for send to complete before returning response
6. ✅ **Added Proper Error Handling**: Exceptions thrown synchronously to client

### Files Modified
```
✓ order-init/src/main/resources/application.yml
✓ order-init/src/main/java/.../OrderInitService.java
✓ order-process/src/main/resources/application.yml
✓ order-process/src/main/java/.../OrderProcessingService.java
✓ alert-notification/src/main/java/.../AlertNotificationService.java
```

## 🔍 Monitoring & Observability

### Kafka Monitoring
```bash
# Connect to Kafka container
docker exec -it kafka1 bash

# List topics
kafka-topics --bootstrap-server localhost:29092 --list

# Describe order-events topic
kafka-topics --bootstrap-server localhost:29092 \
  --describe --topic order-events

# View message count
kafka-log-dirs --bootstrap-server localhost:29092 \
  --describe --topic-list order-events

# Consumer group status
kafka-consumer-groups --bootstrap-server localhost:29092 \
  --group order-processing-group --describe

kafka-consumer-groups --bootstrap-server localhost:29092 \
  --group alert-notification-group --describe
```

### Schema Registry Monitoring
```bash
# List all schemas
curl -s http://localhost:7074/subjects | jq

# Get order-events schema versions
curl -s http://localhost:7074/subjects/order-events-value/versions | jq

# Get latest schema
curl -s http://localhost:7074/subjects/order-events-value/versions/latest | jq

# Check compatibility mode
curl -s http://localhost:7074/config | jq
```

### Application Logs

All services output detailed logs including:
- Order creation/processing timestamps
- Kafka partition and offset information
- Schema registration details
- Consumer group assignments
- Business logic execution

**Example Log Output**:
```
2026-07-28T03:02:57.000+05:30 INFO OrderProcessingService: PROCESSING SERVICE | Published order ... (partition=2, offset=45)
2026-07-28T03:02:58.000+05:30 INFO OrderProcessingService: Processing Order: Order{...status=CREATED...}
2026-07-28T03:02:59.000+05:30 INFO AlertNotificationService: HIGH VALUE ORDER ALERT: Order ... amount: $1500.00
```

## 🚨 Troubleshooting

### Problem: "Connection refused" or "ConnectException"
**Symptoms**: Error at `localhost:7074` or `localhost:9092`

**Solution**:
```bash
# Check if containers are running
docker ps

# Start missing services
docker-compose -f docker-compose-kafka-kraft.yaml --env-file .env.local up -d
docker-compose -f docker-compose.schema-rest.yaml --env-file .env.local up -d

# Check logs
docker logs schema-registry
docker logs kafka1
```

### Problem: "Unknown protocol: localhost"
**Symptoms**: `MalformedURLException: unknown protocol: localhost`

**Cause**: Missing `http://` in `SCHEMA_REGISTRY_URL` environment variable

**Solution**:
```bash
# WRONG
export SCHEMA_REGISTRY_URL=localhost:7074

# CORRECT
export SCHEMA_REGISTRY_URL=http://localhost:7074
```

### Problem: "Invalid topics: [{app.kafka.topics.order-events}]"
**Symptoms**: Consumer can't find topic, literal placeholder name in error

**Cause**: Wrong placeholder syntax in `@KafkaListener` annotation

**Solution**: Use `${}` not `{}` for property placeholders
```java
// WRONG
@KafkaListener(topics = "{app.kafka.topics.order-events}")

// CORRECT
@KafkaListener(topics = "${app.kafka.topics.order-events}")
```

### Problem: Consumer lag or no messages being consumed
**Symptoms**: Consumer starts but doesn't process messages

**Solution**:
```bash
# Check consumer group status
docker exec kafka1 kafka-consumer-groups \
  --bootstrap-server localhost:29092 \
  --group order-processing-group --describe

# Reset consumer offset to beginning
docker exec kafka1 kafka-consumer-groups \
  --bootstrap-server localhost:29092 \
  --group order-processing-group --reset-offsets \
  --to-earliest --execute --topic order-events
```

### Problem: Schema Registry connection timeout
**Symptoms**: Logs show "Failed to send HTTP request to endpoint"

**Solution**:
```bash
# Verify Schema Registry is running
docker ps | grep schema-registry

# Check Schema Registry logs
docker logs schema-registry

# Restart if needed
docker restart schema-registry

# Wait 10 seconds then test
curl http://localhost:7074/subjects
```

## 🧹 Cleanup & Reset

### Stop All Services
```bash
# Stop from project root
docker-compose -f docker-compose-files/docker-compose.schema-rest.yaml down
docker-compose -f docker-compose-files/docker-compose-kafka-kraft.yaml down
```

### Full Reset (Remove Data)
```bash
# Stop and remove all containers and volumes
docker-compose -f docker-compose-files/docker-compose.schema-rest.yaml down -v
docker-compose -f docker-compose-files/docker-compose-kafka-kraft.yaml down -v

# Remove all Kafka data
docker volume rm \
  docker-compose-files_kafka1-data \
  docker-compose-files_kafka2-data \
  docker-compose-files_kafka3-data

# Verify cleanup
docker ps -a
docker volume ls
```

### Clean Build
```bash
# Remove all compiled code
mvn clean

# Rebuild everything
mvn clean package

# Fresh start
cd spring-kafka-parent
mvn clean package -DskipTests
```

## 🔧 Development & Architecture

### Key Design Patterns
- **Event-Driven Architecture**: Loose coupling via Kafka messaging
- **Partition Key Strategy**: orderId ensures message ordering per order
- **Consumer Groups**: Independent processing via separate consumer groups
- **Idempotent Producer**: Prevents duplicate messages on retry
- **Schema Registry**: Centralized schema management and evolution
- **Avro Serialization**: Efficient, versioned data format

### Code Changes Summary (v1.1)

#### 1. OrderInitService - Synchronous Producer
```java
// BEFORE: Fire-and-forget async send
kafkaTemplate.send(topic, orderId, order)
    .whenComplete((result, ex) -> { ... });  // Async callback
return new OrderResponse(...);              // Returns immediately!

// AFTER: Synchronous send with proper error handling
try {
    var sendResult = kafkaTemplate.send(topic, orderId, order).get();  // Blocks!
    log.info("Published order...");
} catch (Exception ex) {
    log.error("Failed to publish...");
    throw new RuntimeException("Failed to create order", ex);
}
return new OrderResponse(...);  // Returns only after send succeeds
```

#### 2. Consumer Topic Resolution Fix
```java
// BEFORE: Literal placeholders (doesn't resolve)
@KafkaListener(
    topics = "{app.kafka.topics.order-events}",    // Treated as literal string
    groupId = "{spring.kafka.consumer.group-id}"
)

// AFTER: Proper Spring placeholders
@KafkaListener(
    topics = "${app.kafka.topics.order-events}",   // Resolves to "order-events"
    groupId = "${spring.kafka.consumer.group-id}"
)
```

### Performance Tuning

**For Higher Throughput**:
```yaml
# Producer (order-init)
spring.kafka.producer:
  batch-size: 32768           # Larger batches
  linger-ms: 10               # Wait up to 10ms to batch
  compression-type: snappy    # Compress messages

# Consumer (order-process, alert-notification)
spring.kafka.listener:
  concurrency: 10             # More threads
spring.kafka.consumer:
    properties:
      fetch.max.bytes: 52428800  # 50MB
```

**For Lower Latency**:
```yaml
# Producer
spring.kafka.producer:
  batch-size: 0               # No batching
  linger-ms: 0                # Send immediately

# Consumer
spring.kafka.listener:
  concurrency: 1              # Sequential processing
spring.kafka.consumer:
    max-poll-records: 10      # Small batches
```

### Scalability Considerations

1. **Horizontal Scaling**: Deploy multiple instances per service
2. **Partition Scaling**: Increase partitions for higher throughput
   ```bash
   docker exec kafka1 kafka-topics --bootstrap-server localhost:29092 \
     --topic order-events --alter --partitions 6
   ```
3. **Consumer Concurrency**: Set to number of partitions
4. **Schema Versioning**: Update schemas with proper compatibility mode

## 📚 Project Structure

```
spring-kafka/
├── README.md                      # This file
├── docker-compose-files/          # Infrastructure as Code
│   ├── docker-compose-kafka-kraft.yaml
│   ├── docker-compose.schema-rest.yaml
│   ├── .env.local                # Local environment variables
│   └── .env.aws                  # AWS environment variables
│
├── spring-kafka-parent/           # Parent POM
│   ├── pom.xml                   # Maven configuration
│   │
│   ├── shared-events/            # Shared library
│   │   ├── src/main/avro/        # Avro schemas
│   │   │   ├── Order.avsc
│   │   │   └── OrderStatus.avsc
│   │   └── target/generated-sources/  # Generated Java classes
│   │
│   ├── order-init/               # Producer service
│   │   ├── src/main/resources/
│   │   │   └── application.yml   # Service configuration
│   │   └── src/main/java/com/spring/poc/kafka/
│   │       ├── OrderInitApplication.java
│   │       ├── controller/
│   │       │   └── OrderController.java
│   │       └── service/
│   │           └── OrderInitService.java
│   │
│   ├── order-process/            # Consumer service 1
│   │   ├── src/main/resources/
│   │   │   └── application.yml
│   │   └── src/main/java/com/spring/poc/kafka/
│   │       ├── OrderProcessApplication.java
│   │       ├── config/
│   │       │   └── KafkaConsumerConfig.java
│   │       └── service/
│   │           └── OrderProcessingService.java
│   │
│   └── alert-notification/       # Consumer service 2
│       ├── src/main/resources/
│       │   └── application.yml
│       └── src/main/java/com/spring/poc/kafka/
│           ├── AlertNotificationApplication.java
│           ├── config/
│           │   └── KafkaConsumerConfig.java
│           └── service/
│               └── AlertNotificationService.java
```

## 🚀 Quick Reference

### Essential Commands

```bash
# Setup
export KAFKA_EXTERNAL_HOST=localhost
export SCHEMA_REGISTRY_URL=http://localhost:7074
cd docker-compose-files
docker-compose -f docker-compose-kafka-kraft.yaml --env-file .env.local up -d
docker-compose -f docker-compose.schema-rest.yaml --env-file .env.local up -d

# Build & Run
cd spring-kafka-parent
mvn clean package
cd order-init && mvn spring-boot:run     # Terminal 1
cd order-process && mvn spring-boot:run  # Terminal 2
cd alert-notification && mvn spring-boot:run  # Terminal 3

# Test
curl -X POST http://localhost:8084/api/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"CUST-001","product":"Laptop","quantity":1,"amount":1200.50}'

# Monitor
curl http://localhost:7074/subjects
docker logs schema-registry
docker exec kafka1 kafka-consumer-groups --bootstrap-server localhost:29092 \
  --group order-processing-group --describe

# Cleanup
docker-compose down -v
```

## ❓ FAQ

**Q: Why is my application failing with "Connection refused"?**
A: Ensure Docker containers are running and environment variables are set correctly.
```bash
docker ps
echo $SCHEMA_REGISTRY_URL  # Must be http://localhost:7074
```

**Q: How do I verify the schema is registered?**
A: 
```bash
curl http://localhost:7074/subjects/order-events-value/versions/latest
```

**Q: Can I run the services without Docker?**
A: No, Kafka and Schema Registry must be running. Docker Compose is the recommended way.

**Q: How do I change the order-events topic partitions?**
A:
```bash
docker exec kafka1 kafka-topics --bootstrap-server localhost:29092 \
  --topic order-events --alter --partitions 5
```

**Q: What if consumer is not receiving messages?**
A: Check consumer group status and reset offset:
```bash
docker exec kafka1 kafka-consumer-groups --bootstrap-server localhost:29092 \
  --group order-processing-group --reset-offsets --to-earliest --execute \
  --topic order-events
```

**Q: How do I monitor Kafka topics in real-time?**
A:
```bash
docker exec kafka1 kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic order-events --from-beginning
```

**Q: Can I use a different serialization format?**
A: Yes, replace Avro with JSON or Protobuf. Update producer/consumer deserializers.

## 📚 Additional Resources

- [Spring Kafka Documentation](https://spring.io/projects/spring-kafka)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html)
- [Apache Avro](https://avro.apache.org/docs/)
- [Spring Boot Documentation](https://spring.io/projects/spring-boot)
- [KRaft Mode Guide](https://kafka.apache.org/documentation/#kraft)

## 🤝 Contributing

1. Create a feature branch: `git checkout -b feature/my-feature`
2. Make changes and ensure tests pass: `mvn test`
3. Commit with clear message: `git commit -m "Add my feature"`
4. Push to branch: `git push origin feature/my-feature`
5. Create a Pull Request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

---

**Last Updated**: July 28, 2026
**Version**: 1.1.0 (with Schema Registry & KRaft mode)
**Status**: Production Ready ✅
