"""Connectors for [Apache Pulsar](https://pulsar.apache.org).

This module provides Bytewax connectors for Apache Pulsar with three main approaches:

## 1. Direct Approach (Simple & Fast)
Use `PulsarSource` and `PulsarSink` directly for simple, high-performance scenarios:

**When to Use Direct Approach**:
- Simple message processing without complex error handling
- High-throughput scenarios where performance matters
- Prototyping and development
- Single-topic processing
- When you handle serialization manually

**Production Use Cases**:
- High-frequency trading systems
- IoT sensor data ingestion
- Log aggregation pipelines
- Simple ETL processes

```python
from bytewax_pulsar_connector import PulsarSource, PulsarSink, PulsarSinkMessage
from bytewax import operators as op
from bytewax.dataflow import Dataflow

# Simple, direct approach - maximum performance
flow = Dataflow("direct-example")
source = PulsarSource("pulsar://localhost:6650", ["input-topic"], "my-subscription")
sink = PulsarSink("pulsar://localhost:6650", "output-topic")

input_stream = op.input("input", flow, source)
processed = op.map("process", input_stream, my_processor)
op.output("output", processed, sink)
```

## 2. Operators Approach (Advanced & Robust)
Use the operators for production systems requiring error handling and SerDe:

**When to Use Operators Approach**:
- Production systems requiring error handling
- Complex serialization/deserialization needs
- Multi-topic processing with different SerDe per topic
- When you need error streams for monitoring/alerting
- Dead letter queue patterns

**Production Use Cases**:
- Microservices communication
- Event-driven architectures
- Data transformation pipelines
- Systems requiring monitoring and alerting

```python
from bytewax_pulsar_connector import operators as pop
from bytewax.dataflow import Dataflow

# Advanced approach with error handling and SerDe
flow = Dataflow("operators-example")
pulsar_input = pop.input("input", flow, service_url="...", topics=["..."], subscription_name="...")

# Handle errors separately
op.inspect("log_errors", pulsar_input.errs)

# Process with JSON SerDe
json_stream = pop.deserialize_value("deser", pulsar_input.oks, pop.DEFAULT_JSON_SERDE)
processed = op.map("process", json_stream.oks, my_processor)
serialized = pop.serialize_value("ser", processed, pop.DEFAULT_JSON_PRETTY_SERDE)
pop.output("output", serialized, service_url="...", topic="...")
```

## 3. Dynamic Sink Approach (Topic-per-Message Routing)
Use `PulsarDynamicSink` when you need to route messages to different topics dynamically:

**When to Use Dynamic Sink**:
- Multi-tenant systems (route by tenant ID)
- Event routing based on message content
- A/B testing scenarios
- Geographic routing
- Error routing to different topics

**Production Use Cases**:
- Multi-tenant SaaS platforms
- Event sourcing systems
- Microservices with dynamic routing
- Data lake partitioning strategies

```python
from bytewax_pulsar_connector import PulsarDynamicSink, PulsarSinkMessage

# Simple dynamic routing - just set the topic in the message
flow = Dataflow("dynamic-routing")

def route_by_tenant(data):
    # Route messages to tenant-specific topics
    tenant_id = data.get('tenant_id', 'default')
    return PulsarSinkMessage(
        value=json.dumps(data).encode('utf-8'),
        topic=f"tenant-{tenant_id}-events",  # Dynamic topic per message
        properties={'tenant': tenant_id}
    )

routed_messages = op.map("route", processed_stream, route_by_tenant)

# The sink automatically routes to the topic specified in each message
dynamic_sink = PulsarDynamicSink("pulsar://localhost:6650")
op.output("dynamic-output", routed_messages, dynamic_sink)
```

"""

from collections.abc import Callable
from typing import List, Union

import pulsar
from bytewax.inputs import DynamicSource, StatelessSourcePartition
from bytewax.outputs import DynamicSink, StatelessSinkPartition

from .models import (
    PulsarError,
    PulsarSourceMessage,
    PulsarSinkMessage,
    ConsumerConfig,
    ProducerConfig,
)
from .pulsar_manager import PulsarManager


class _PulsarSourcePartition(StatelessSourcePartition):
    """Internal partition implementation for PulsarSource.

    **Internal Class**: This handles the actual message consumption for each worker.
    Users should use PulsarSource instead of this class directly.
    """

    def __init__(self, consumer: pulsar.Consumer, raise_on_errors: bool = True):
        self.consumer = consumer
        self.raise_on_errors = raise_on_errors

    def next_batch(self) -> List[Union[PulsarSourceMessage, PulsarError]]:
        """Attempt to get the next batch of input items.

        :returns: Items immediately ready. May be empty if no new items.
        """
        try:
            messages: List[pulsar.Message] = self.consumer.batch_receive()
            batch = []

            for message in messages:
                try:
                    # Convert pulsar.Message to PulsarSourceMessage
                    source_msg = PulsarSourceMessage(
                        key=(
                            message.partition_key().encode()
                            if message.partition_key()
                            else None
                        ),
                        value=message.data(),
                        topic=message.topic_name(),
                        properties=message.properties(),
                        message_id=str(message.message_id()),
                        publish_timestamp=message.publish_timestamp(),
                        event_timestamp=message.event_timestamp(),
                        partition_key=message.partition_key(),
                        ordering_key=message.ordering_key(),
                    )
                    batch.append(source_msg)
                    self.consumer.acknowledge(message)

                except Exception as e: # pylint: disable=broad-exception-caught
                    error_msg = PulsarError(
                        error=f"Error processing message: {e}",
                        msg=PulsarSourceMessage(
                            key=None,
                            value=b"",
                            topic=(
                                message.topic_name()
                                if hasattr(message, "topic_name")
                                else None
                            ),
                        ),
                    )
                    if self.raise_on_errors:
                        raise e
                    batch.append(error_msg)

            return batch

        except Exception as e: # pylint: disable=broad-exception-caught
            error_msg = PulsarError(
                error=f"Error receiving batch: {e}",
                msg=PulsarSourceMessage(key=None, value=b""),
            )
            if self.raise_on_errors:
                raise e
            return [error_msg]

    def close(self) -> None:
        self.consumer.close()


# pylint: disable=too-few-public-methods
class PulsarSource(DynamicSource):
    """Direct Pulsar source for high-performance message consumption.

    **What**: Creates Pulsar consumers that read messages from specified topics
    **Why**: Provides simple, high-performance message consumption
    **When**: Use for simple processing without complex error handling needs

    **Performance Characteristics**:
    - Minimal overhead for maximum throughput
    - Direct message processing without intermediate transformations
    - Suitable for high-frequency, low-latency scenarios

    **Production Use Cases**:
    - High-throughput data ingestion (IoT sensors, logs)
    - Simple ETL processes
    - Real-time analytics pipelines
    - Event streaming for microservices

    Args:
        service_url: Pulsar service URL (e.g., 'pulsar://localhost:6650')
        topics: List of topics to consume from
        subscription_name: Subscription name for consumer group
        consumer_config: Consumer configuration (ConsumerConfig object or dict)
        raise_on_errors: Whether to raise exceptions on errors (default: True)

    Example:
        ```python
        # High-performance IoT data processing
        from bytewax_pulsar_connector import ConsumerConfig

        flow = Dataflow("iot-processing")

        # Using Pydantic model for type safety
        config = ConsumerConfig(
            consumer_type=pulsar.ConsumerType.Shared,
            receiver_queue_size=10000,
            max_total_receiver_queue_size_across_partitions=50000
        )

        source = PulsarSource(
            service_url="pulsar://localhost:6650",
            topics=["sensor-data", "device-metrics"],
            subscription_name="iot-processor",
            consumer_config=config
        )

        input_stream = op.input("pulsar-input", flow, source)

        def process_sensor_data(msg: PulsarSourceMessage):
            # Fast processing of sensor data
            data = json.loads(msg.value.decode('utf-8'))
            return calculate_metrics(data)

        processed = op.map("process", input_stream, process_sensor_data)

        sink = PulsarSink("pulsar://localhost:6650", "processed-metrics")
        op.output("pulsar-output", processed, sink)
        ```
    """

    # pylint: disable=too-many-arguments, too-many-positional-arguments
    def __init__(
        self,
        service_url: str,
        topics: List[str],
        subscription_name: str,
        consumer_config: ConsumerConfig = None,
        raise_on_errors: bool = True,
    ) -> None:
        # Validate that topics is a list, not a string
        if isinstance(topics, str):
            raise TypeError("topics must be an iterable and not a string")

        self.service_url = service_url
        self.topics = topics
        self.subscription_name = subscription_name
        self.consumer_config: ConsumerConfig = consumer_config or ConsumerConfig()

        self.raise_on_errors = raise_on_errors

    def build(self, step_id: str, worker_index: int, worker_count: int):
        """Build an input source for a worker."""
        pulsar_consumer = PulsarManager.create_consumer(
            service_url=self.service_url,
            topics=self.topics,
            subscription_name=self.subscription_name,
            worker_index=worker_index,
            consumer_config=self.consumer_config,
        )
        return _PulsarSourcePartition(pulsar_consumer, self.raise_on_errors)


# pylint: disable=too-few-public-methods
class _PulsarSinkPartition(StatelessSinkPartition):
    """Internal partition implementation for PulsarSink.

    **Internal Class**: This handles the actual message production for each worker.
    Users should use PulsarSink instead of this class directly.
    """

    def __init__(
        self,
        producer: pulsar.Producer,
        message_callback: Callable[[pulsar.Result, pulsar.MessageId], None],
        topic: str,
    ):
        self.producer = producer
        self.message_callback = message_callback
        self.topic = topic

    def write_batch(self, items: List[PulsarSinkMessage]):
        """Write a batch of output items."""
        message: PulsarSinkMessage
        for message in items:
            if message.topic and message.topic != self.topic:
                raise ValueError(
                    "Topic mismatch, message topic is different from sink topic may be you should use PulsarDynamicSink"
                )

            self.producer.send_async(
                content=message.value,
                **message.model_dump(exclude={"key", "value", "topic"}),
                callback=self.message_callback,
            )

        self.producer.flush()

    def close(self):
        self.producer.close()


class _PulsarDynamicSinkPartition(StatelessSinkPartition):
    """Sink partition that routes messages based on their topic field.

    **What**: Handles dynamic topic routing based on message.topic field
    **Why**: Enables flexible message routing without custom implementations
    **When**: Automatically used by PulsarDynamicSink

    **How it works**:
    - Each message must have a 'topic' field set
    - Producers are created/reused per topic automatically
    - Messages are routed to their specified topics
    - No custom implementation needed - just set message.topic

    """

    def __init__(
        self,
        worker_index: int,
        service_url: str,
        producer_config: ProducerConfig = None,
    ):
        self.worker_index = worker_index
        self.service_url = service_url
        self.producer_config = producer_config or ProducerConfig()

    def write_batch(self, items: List[PulsarSinkMessage]):
        """Write a batch of output items to their specified topics.

        Each message's 'topic' field determines where it will be sent.
        Producers are automatically created and reused per topic.

        :arg items: Messages with 'topic' field set. Non-deterministically batched.
        """
        message: PulsarSinkMessage
        for message in items:
            if not message.topic:
                raise ValueError("Topic is required for Pulsar Dynamic Sink")

            # Get or create producer for this topic
            producer = PulsarManager.get_create_producer(
                service_url=self.service_url,
                topic_name=message.topic,
                worker_index=self.worker_index,
                producer_config=self.producer_config,
            )

            producer.send_async(
                content=message.value,
                **message.model_dump(exclude={"value", "topic", "key"}),
                callback=self.producer_config.message_callback,
            )
            producer.flush()

    def close(self) -> None:
        PulsarManager.close_worker_producers(self.worker_index)


class PulsarSink(DynamicSink):
    """Direct Pulsar sink for high-performance message production.

    **What**: Creates Pulsar producers that write messages to specified topics
    **Why**: Provides simple, high-performance message production
    **When**: Use for straightforward output to single topics

    **Performance Characteristics**:
    - Minimal overhead for maximum throughput
    - Direct message production without intermediate transformations
    - Suitable for high-frequency, low-latency scenarios

    **Production Use Cases**:
    - High-throughput data output (processed events, aggregated metrics)
    - Simple data pipeline outputs
    - Real-time analytics results
    - Microservices event publishing

    Args:
        service_url: Pulsar service URL (e.g., 'pulsar://localhost:6650')
        topic: Topic to produce messages to
        producer_config: Producer configuration (ProducerConfig object or dict)
        message_callback: Optional callback for message delivery confirmation

    Example:
        ```python
        # High-performance metrics publishing
        from bytewax_pulsar_connector import ProducerConfig, CompressionType

        # Using Pydantic model for validated configuration
        config = ProducerConfig(
            compression_type=CompressionType.LZ4,
            batching_enabled=True,
            batching_max_messages=1000,
            batching_max_publish_delay_ms=100,
            block_if_queue_full=True
        )

        # Or use preset for common scenarios
        config = ProducerConfig().for_high_throughput()

        sink = PulsarSink(
            service_url="pulsar://localhost:6650",
            topic="real-time-metrics",
            producer_config=config,
            message_callback=lambda msg_id: logger.info(f"Sent: {msg_id}")
        )

        def create_metric_message(metric_data):
            return PulsarSinkMessage(
                key=metric_data['metric_name'].encode('utf-8'),
                value=json.dumps(metric_data).encode('utf-8'),
                properties={
                    'metric_type': metric_data['type'],
                    'source': 'analytics-pipeline'
                },
                event_timestamp=metric_data['timestamp']
            )

        metrics_stream = op.map("create_messages", processed_data, create_metric_message)
        op.output("metrics-output", metrics_stream, sink)
        ```
    """

    def __init__(
        self,
        service_url: str,
        topic: str,
        producer_config: ProducerConfig = None,
    ) -> None:
        super().__init__()
        if not topic:
            raise ValueError("Topic is required for PulsarSink")
        self.service_url = service_url
        self.topic = topic
        self.producer_config = producer_config or ProducerConfig()

    def build(self, step_id: str, worker_index: int, worker_count: int):
        """Build an output partition for a worker."""
        producer = PulsarManager.get_create_producer(
            service_url=self.service_url,
            topic_name=self.topic,
            worker_index=worker_index,
            producer_config=self.producer_config,
        )
        return _PulsarSinkPartition(
            producer=producer,
            message_callback=self.producer_config.message_callback,
            topic=self.topic,
        )


class PulsarDynamicSink(DynamicSink):
    """Dynamic sink that routes messages based on their topic field.

    **What**: Sink that automatically routes messages to topics specified in message.topic
    **Why**: Enables flexible topic routing without custom implementations
    **When**: Use when messages need to go to different topics dynamically

    **Key Difference from PulsarSink**:
    - PulsarSink: All messages go to one fixed topic
    - PulsarDynamicSink: Each message specifies its own topic

    **Architecture Benefits**:
    - Simple: Just set message.topic - no custom classes needed
    - Scalable: Each worker handles routing independently
    - Efficient: Reuses producers for the same topics
    - Flexible: Route based on any logic in your processing function

    **Example Use Cases**:
    ```python
    # Route by customer type
    def route_by_customer(data):
        customer_type = data.get('type', 'standard')
        return PulsarSinkMessage(
            value=json.dumps(data).encode('utf-8'),
            topic=f"customers-{customer_type}"
        )

    # Route errors to dead letter queue
    def route_with_error_handling(result):
        if result.get('error'):
            topic = "dead-letter-queue"
        else:
            topic = "processed-events"

        return PulsarSinkMessage(
            value=json.dumps(result).encode('utf-8'),
            topic=topic
        )

    # Geographic routing
    def route_by_region(event):
        region = event.get('region', 'us-east')
        return PulsarSinkMessage(
            value=json.dumps(event).encode('utf-8'),
            topic=f"events-{region}",
            partition_key=event.get('user_id')  # Ensure user events stay together
        )
    ```

    Args:
        service_url: Pulsar service URL (e.g., 'pulsar://localhost:6650')
        producer_config: Producer configuration (ProducerConfig object or dict)
    """

    def __init__(
        self,
        service_url: str,
        producer_config: ProducerConfig = None,
    ):
        """Initialize the PulsarDynamicSink.

        Args:
            service_url: Pulsar service URL (e.g., 'pulsar://localhost:6650')
            producer_config: Producer configuration (ProducerConfig object or dict).
        """
        self.service_url = service_url
        self.producer_config: ProducerConfig = producer_config or ProducerConfig()

    def build(self, step_id: str, worker_index: int, worker_count: int):
        return _PulsarDynamicSinkPartition(
            worker_index=worker_index,
            service_url=self.service_url,
            producer_config=self.producer_config,
        )
