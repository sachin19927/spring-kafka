# Spring Kafka Microservices Project

A production-ready Spring Boot microservices application demonstrating Apache Kafka integration for event-driven architecture. This project showcases a complete order processing pipeline with three microservices communicating through Kafka topics.

## 🏗️ Architecture Overview

This project implements an event-driven architecture with three microservices:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   order-init    │    │  order-process  │    │alert-notification│
│   (Port: 8081)  │    │   (Port: 8082)  │    │   (Port: 8083)  │
└─────────┬───────┘    └─────────┬───────┘    └─────────┬───────┘
          │                      │                      │
          └──────────────────────┼──────────────────────┘
                                 │
                    ┌─────────────▼─────────────┐
                    │      Apache Kafka         │
                    │   Topic: "orders"         │
                    │  Bootstrap: 9092,9093,9094 │
                    └───────────────────────────┘
```

## 📦 Modules

### 1. order-init (Port: 8081)
**Role**: Order Entry Service
- REST API endpoint for creating orders
- Produces order events to Kafka topic "orders"
- Uses customerId as partition key for ordering consistency

**Key Components**:
- `OrderController`: REST endpoints (`/api/orders`)
- `OrderProducerService`: Kafka message producer
- `OrderRequest`: DTO for order creation
- `KafkaProducerConfig`: Producer configuration

### 2. order-process (Port: 8082)
**Role**: Order Processing Service
- Consumes orders from Kafka
- Processes business logic (inventory, payment, status updates)
- Consumer group: `order-processing-group`

**Key Components**:
- `OrderProcessingService`: Main business logic processor
- `KafkaConsumerConfig`: Consumer configuration
- Processes orders with validation, payment, and status updates

### 3. alert-notification (Port: 8083)
**Role**: Alert & Notification Service
- Consumes orders for alerting purposes
- Handles high-value order alerts and cancellation notifications
- Consumer group: `alert-notification-group`

**Key Components**:
- `AlertNotificationService`: Alert processing logic
- `KafkaConsumerConfig`: Consumer configuration
- Sends alerts for high-value orders (> $1000) and cancellations

## 🚀 Technology Stack

- **Java 21** with Spring Boot 4.0.6
- **Apache Kafka** for event streaming
- **Spring Kafka** for Kafka integration
- **Maven** for dependency management
- **Lombok** for boilerplate reduction
- **Jackson** for JSON serialization
- **Docker** for Kafka infrastructure

## 📋 Prerequisites

- Java 21 or higher
- Maven 3.6+
- Docker and Docker Compose
- Git

## 🛠️ Setup & Installation

### 1. Clone the Repository
```bash
git clone <repository-url>
cd spring-kafka
```

### 2. Start Kafka Infrastructure

#### Option A: Single Node Kafka (Simple Setup)
```bash
docker-compose -f docker-kafka.yaml up -d
```

#### Option B: Kafka with KRaft Mode (Production-like)
```bash
docker-compose -f docker-kafka-kraft.yaml up -d
```

**Access Kafka UI**: http://localhost:9000 (Kafdrop)

### 3. Build All Modules
```bash
# Build all modules from root
mvn clean install

# Or build individual modules
cd order-init && mvn clean install
cd ../order-process && mvn clean install
cd ../alert-notification && mvn clean install
```

### 4. Start All Services

#### Terminal 1 - Order Init Service
```bash
cd order-init
mvn spring-boot:run
```

#### Terminal 2 - Order Process Service
```bash
cd order-process
mvn spring-boot:run
```

#### Terminal 3 - Alert Notification Service
```bash
cd alert-notification
mvn spring-boot:run
```

## 🎯 Usage Examples

### Create an Order
```bash
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-001",
    "productId": "Wireless Keyboard",
    "quantity": 1,
    "price": 99.99
  }'
```

### Create a High-Value Order (Triggers Alert)
```bash
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "CUST-002",
    "productId": "Gaming Laptop",
    "quantity": 1,
    "price": 1500.00
  }'
```

### Test Order Creation
```bash
curl http://localhost:8081/api/orders/test
```

## 📊 Order Model

```java
public record Order(
    UUID orderId,
    String customerId,
    String productId,
    Integer quantity,
    BigDecimal price,
    OrderStatus status,    // PENDING, CONFIRMED, SHIPPED, DELIVERED, CANCELLED
    LocalDateTime timestamp
)
```

## ⚙️ Configuration Details

### Kafka Configuration
- **Bootstrap Servers**: `localhost:9092,localhost:9093,localhost:9094`
- **Topic**: `orders`
- **Serialization**: JSON with Jackson

### Producer Settings (order-init)
- **Acks**: `all` (wait for all replicas)
- **Retries**: 3
- **Idempotence**: Enabled
- **Batch Size**: 16384
- **Buffer Memory**: 32MB

### Consumer Settings (order-process & alert-notification)
- **Auto Offset Reset**: `earliest`
- **Concurrency**: 3 threads
- **JSON Trusted Packages**: `com.spring.poc.kafka.model`

## 🔍 Monitoring & Observability

### Kafka Topics Monitoring
- **Kafdrop UI**: http://localhost:9000
- Monitor topic partitions, consumer groups, and message flow

### Application Logs
Each service logs detailed information about:
- Order processing steps
- Kafka message metadata (partition, offset, key)
- Business logic execution
- Error handling and retries

## 🧪 Testing

### Unit Tests
```bash
# Run tests for all modules
mvn test

# Run tests for specific module
cd order-init && mvn test
```

### Integration Testing
1. Start Kafka infrastructure
2. Start all services
3. Use curl commands to test order flow
4. Monitor logs and Kafka UI for message flow

## 🔄 Message Flow

1. **Order Creation**: Client calls `/api/orders` endpoint
2. **Event Publishing**: `order-init` publishes order to Kafka topic "orders"
3. **Parallel Processing**: 
   - `order-process` consumes and processes the order
   - `alert-notification` consumes for alerting
4. **Business Logic**: Processing service validates inventory, processes payment
5. **Alerts**: Notification service checks for high-value orders and cancellations

## 🚨 Error Handling

- **Producer Retries**: 3 automatic retries with exponential backoff
- **Consumer Error Handling**: Failed messages trigger retry/DLQ mechanisms
- **Logging**: Comprehensive error logging with context
- **Dead Letter Queue**: Configurable for failed message handling

## 🔧 Development Notes

### Key Design Patterns
- **Event-Driven Architecture**: Loose coupling via Kafka
- **Partition Key Strategy**: Customer ID for ordering consistency
- **Consumer Groups**: Different groups for parallel processing
- **Idempotent Producer**: Prevents duplicate messages

### Scalability Considerations
- **Horizontal Scaling**: Multiple instances per service
- **Partition Scaling**: Add partitions for higher throughput
- **Consumer Concurrency**: Configurable thread pools
- **Load Balancing**: Kafka's natural load distribution

## 📚 Additional Resources

- [Spring Kafka Documentation](https://spring.io/projects/spring-kafka)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Spring Boot Documentation](https://spring.io/projects/spring-boot)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.
