# Bytewax Pulsar Connector Examples

This directory contains practical examples demonstrating how to use the Bytewax Pulsar connector for different use cases.

## 🚀 Quick Start

```bash
# 1. Start Pulsar
docker run -d --name pulsar -p 6650:6650 -p 8080:8080 \
  apachepulsar/pulsar:latest bin/pulsar standalone

# 2. Install dependencies
pip install bytewax bytewax-pulsar-connector

# 3. Run your first example
python -m bytewax.run examples.getting_started:main
```

## 📚 Examples Overview

### 1. **getting_started.py** - Hello World
The simplest example to understand the basics.
- **What**: Adds timestamps to messages
- **Learn**: Basic source → process → sink pattern
- **Run**: `python -m bytewax.run examples.getting_started:flow`

### 2. **high_performance.py** - IoT Data Processing
Optimized for processing high volumes of messages.
- **What**: High-throughput IoT sensor data processing
- **Learn**: Performance optimization, batching, compression
- **Use Case**: IoT, logs, metrics, real-time analytics
- **Run**: `python -m bytewax.run examples.high_performance:flow -w 4`

### 3. **production_ready.py** - E-commerce Order Processing
Production-grade example with error handling and monitoring.
- **What**: Order processing with validation and error handling
- **Learn**: Error streams, dead letter queues, monitoring
- **Use Case**: E-commerce, financial services, APIs
- **Run**: `python -m bytewax.run examples.production_ready:flow`
- **Test Data**: Use `interactive_producer.py` with **type 4 (transaction)** for realistic e-commerce data

### 4. **multi_tenant.py** - SaaS Platform Routing
Dynamic routing for multi-tenant architectures.
- **What**: Route messages to tenant-specific topics
- **Learn**: Dynamic sinks, content-based routing
- **Use Case**: Multi-tenant SaaS, geographic routing
- **Run**: `python -m bytewax.run examples.multi_tenant:flow`

### 5. **message_tracking.py** - Delivery Tracking & Monitoring
Track message delivery with callbacks and metrics.
- **What**: Real-time message tracking and statistics
- **Learn**: Callbacks, metrics collection, monitoring
- **Use Case**: Audit trails, SLA monitoring, debugging
- **Run**: `python -m bytewax.run examples.message_tracking:flow`

## 🛠️ Utility Scripts

### **interactive_producer.py** - Test Data Generator
Interactive tool to generate test messages for your pipelines.
```bash
python examples/interactive_producer.py
```

**Message Types Available:**
- **Type 1 (simple)**: Basic test messages - good for `getting_started.py`
- **Type 2 (sensor)**: IoT sensor data - perfect for `high_performance.py`
- **Type 3 (event)**: User events - good for `message_tracking.py`
- **Type 4 (transaction)**: E-commerce transactions - ideal for `production_ready.py`

## 🚀 Running the Examples

All examples use standardized topics for simplicity:
- **Input**: `input-topic`
- **Output**: `output-topic` (or dynamically routed topics)

### Step 1: Start Pulsar (if not already running)
```bash
docker run -d --name pulsar -p 6650:6650 -p 8080:8080 \
  apachepulsar/pulsar:latest bin/pulsar standalone
```

### Step 2: Run an Example
```bash
# Basic example
python -m bytewax.run examples.getting_started:flow

# High performance with 4 workers
python -m bytewax.run examples.high_performance:flow -w 4

# Production ready with error handling
python -m bytewax.run examples.production_ready:flow

# Multi-tenant routing
python -m bytewax.run examples.multi_tenant:flow

# Message tracking with metrics
python -m bytewax.run examples.message_tracking:flow
```

### Step 3: Send Test Messages

**Option A: Use Interactive Producer (Recommended)**
```bash
# Generate realistic test data with interactive prompts
python examples/interactive_producer.py

# Then select:
# - Type 1 for getting_started.py
# - Type 2 for high_performance.py  
# - Type 3 for message_tracking.py
# - Type 4 for production_ready.py
```

**Option B: Use Pulsar CLI**
```bash
# Simple message
docker exec -it pulsar bin/pulsar-client produce input-topic \
  --messages '{"message": "Hello Bytewax!"}'

# IoT sensor data
docker exec -it pulsar bin/pulsar-client produce input-topic \
  --messages '{"sensor_id": "temp-001", "temperature": 23.5, "humidity": 65}'

# E-commerce order
docker exec -it pulsar bin/pulsar-client produce input-topic \
  --messages '{"order_id": "ORD-12345", "customer_id": "CUST-001", "items": [{"sku": "ITEM-1", "price": 29.99, "quantity": 2}], "total": 59.98}'

# Multi-tenant message
docker exec -it pulsar bin/pulsar-client produce input-topic \
  --messages '{"tenant_id": "ent_acme", "order_id": "123", "amount": 999.99, "region": "us-east"}'

# Message with priority for tracking
docker exec -it pulsar bin/pulsar-client produce input-topic \
  --messages '{"id": 1, "type": "standard", "priority": "high"}'
```

### Step 4: Monitor Output
```bash
# Check output topic
docker exec -it pulsar bin/pulsar-client consume output-topic \
  --subscription-name test -n 10

# For dynamic routing examples, check specific topics
docker exec -it pulsar bin/pulsar-client consume tenant-ent_acme-us-east-orders \
  --subscription-name test -n 10
```

## 🎯 Choosing the Right Example

| If you need... | Use this example | Key Features |
|----------------|------------------|--------------|
| To learn basics | `getting_started.py` | Simple, clear, minimal |
| Maximum throughput | `high_performance.py` | Batching, compression, parallel processing |
| Error handling | `production_ready.py` | Validation, DLQ, monitoring |
| Dynamic routing | `multi_tenant.py` | Content-based routing, multi-tenancy |
| Message tracking | `message_tracking.py` | Callbacks, metrics, audit trails |

## 💡 Common Patterns

### High Throughput Configuration
```python
from bytewax_pulsar_connector import ProducerConfig, ConsumerConfig, PulsarCompressionType

# Consumer for high throughput
consumer_config = ConsumerConfig(
    consumer_type="shared",
    receiver_queue_size=10000,
    max_total_receiver_queue_size_across_partitions=50000
)

# Producer for high throughput
producer_config = ProducerConfig().for_high_throughput()
# Or customize:
producer_config = ProducerConfig(
    compression_type=PulsarCompressionType.LZ4,
    batching_enabled=True,
    batching_max_messages=1000,
    max_pending_messages=50000
)
```

### Error Handling Pattern
```python
# Separate success and error processing
def process_with_validation(msg):
    try:
        data = json.loads(msg.value.decode('utf-8'))
        # Validation and processing
        return create_success_message(data)
    except Exception as e:
        return create_error_message(msg, str(e))
```

### Multi-Tenant Pattern
```python
# Simple dynamic routing - just set the topic in the message
def route_by_tenant(msg):
    data = json.loads(msg.value.decode('utf-8'))
    tenant_id = data.get('tenant_id', 'default')
    
    return PulsarSinkMessage(
        value=msg.value,
        topic=f"tenant-{tenant_id}-events",  # Dynamic topic per message
        properties={'tenant': tenant_id}
    )

# Use with PulsarDynamicSink - no custom class needed!
dynamic_sink = PulsarDynamicSink("pulsar://localhost:6650")
```

## 🧪 Testing Your Pipeline

### Send Test Messages
```bash
# Using Pulsar CLI
docker exec -it pulsar bin/pulsar-client produce input-topic \
  --messages '{"sensor_id": "temp-001", "value": 23.5}'

# Using interactive producer
python examples/interactive_producer.py
```

### Monitor Output
```bash
# Consume from output topic
docker exec -it pulsar bin/pulsar-client consume output-topic \
  --subscription-name test -n 10

# Check error topic
docker exec -it pulsar bin/pulsar-client consume error-topic \
  --subscription-name test -n 10
```

## 📊 Performance Tips

1. **Use multiple workers**: `-w 4` for parallel processing
2. **Enable batching**: Better throughput for high-volume data
3. **Choose right compression**: LZ4 for speed, ZSTD for size
4. **Tune queue sizes**: Based on your memory constraints
5. **Monitor metrics**: Use callbacks for tracking

## 🔧 Troubleshooting

### Connection Issues
```bash
# Check Pulsar is running
docker ps | grep pulsar

# Test connection
telnet localhost 6650
```

### Performance Issues
- Increase receiver queue size
- Enable producer batching
- Use compression
- Add more workers

### Memory Issues
- Reduce queue sizes
- Enable backpressure (`block_if_queue_full=True`)
- Use streaming windows instead of collecting

## 📖 Next Steps

1. Start with `getting_started.py`
2. Choose example matching your use case
3. Customize configuration for your needs
4. Deploy with Docker/Kubernetes
5. Monitor with Pulsar Manager

Need help? Check the [main documentation](../README.md) or open an issue! 