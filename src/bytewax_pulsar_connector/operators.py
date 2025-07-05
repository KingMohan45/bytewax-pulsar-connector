"""Operators for the Pulsar source and sink.

This module provides two approaches for Pulsar integration:

1. **Direct Approach**: Simple source/sink for basic use cases
2. **Operators Approach**: Advanced operators with error handling and SerDe

It's suggested to import operators like this:

```python
from bytewax_pulsar_connector import operators as pop
```

## Use Cases

### When to Use Direct Approach
- Simple message processing without complex error handling
- Prototyping and development
- Single-topic processing
- When you handle serialization manually

### When to Use Operators Approach  
- Production systems requiring error handling
- Complex serialization/deserialization needs
- Multi-topic processing with different SerDe per topic
- When you need error streams for monitoring/alerting

## Examples

### Basic Usage
```python
from bytewax.dataflow import Dataflow

flow = Dataflow("pulsar-in-out")
pulsar_input = pop.input("pulsar_inp", flow, service_url="...", topics=["..."], subscription_name="...")
pop.output("pulsar-out", pulsar_input.oks, service_url="...", topic="...")
```

### With SerDe
```python
# JSON processing with error handling
json_stream = pop.deserialize_value("deser", pulsar_input.oks, pop.DEFAULT_JSON_SERDE)
processed = op.map("process", json_stream.oks, my_processor)
pop.serialize_value("ser", processed, pop.DEFAULT_JSON_PRETTY_SERDE)
```
"""

import json
import pickle
from typing import Any, Dict, List, Optional, Union, cast

import bytewax.operators as op
from bytewax.dataflow import Dataflow, Stream, operator

# Import official Pulsar SerDe classes - always available
from pulsar import SerDe
from pydantic import BaseModel

from .models import PulsarError, PulsarSinkMessage, PulsarSourceMessage
from .connector import PulsarSink, PulsarSource


class PulsarOpOut(BaseModel):
    """Result streams from Pulsar operators.

    This class encapsulates the two output streams from Pulsar operators:
    - `oks`: Successfully processed messages
    - `errs`: Error messages that failed processing

    **Production Use Case**: Allows you to handle errors separately from successful
    messages, enabling robust error handling, monitoring, and dead letter queues.

    Example:
        ```python
        pulsar_input = pop.input("input", flow, ...)

        # Handle successful messages
        processed = op.map("process", pulsar_input.oks, my_processor)

        # Handle errors - send to monitoring/dead letter queue
        op.inspect("log_errors", pulsar_input.errs)
        error_sink = pop.output("errors", pulsar_input.errs, topic="error-topic")
        ```
    """

    oks: Stream[PulsarSourceMessage]
    """Successfully processed items."""

    errs: Stream[PulsarError]
    """Errors that occurred during processing."""


# Official Pulsar SerDe implementations following the documentation
# Reference: https://pulsar.apache.org/docs/next/functions-develop-serde/


class IdentitySerDe(SerDe):
    """
    Default SerDe that leaves data unchanged (identity operation).

    **What**: Passes data through without modification
    **Why**: Default SerDe used by Pulsar Python functions for raw byte processing
    **When**: Use when you want to handle serialization manually or work with raw bytes

    **Production Use Case**:
    - Binary data processing (images, files, protobuf)
    - When downstream systems expect raw bytes
    - High-performance scenarios where serialization overhead matters

    Reference: https://pulsar.apache.org/docs/next/functions-develop-serde/

    Example:
        ```python
        # Process raw binary data
        binary_stream = pop.deserialize_value("deser", input_stream, IdentitySerDe())
        processed = op.map("process_binary", binary_stream.oks, my_binary_processor)
        ```
    """

    def serialize(self, input: Any) -> bytes: # pylint: disable=redefined-outer-name
        """Serialize input to bytes with minimal conversion."""
        if isinstance(input, bytes):
            return input
        if isinstance(input, str):
            return input.encode("utf-8")
        return str(input).encode("utf-8")

    def deserialize(self, input_bytes: bytes) -> bytes:
        """Deserialize bytes (identity operation - no change)."""
        return input_bytes


class StringSerDe(SerDe):
    """
    String SerDe for UTF-8 string serialization/deserialization.

    **What**: Converts between bytes and UTF-8 strings
    **Why**: Most common text processing use case in streaming
    **When**: Processing text messages, logs, simple data formats

    **Production Use Case**:
    - Log processing and analysis
    - Text-based message processing
    - Simple data transformation pipelines
    - When working with systems that produce/consume text

    Reference: https://pulsar.apache.org/docs/next/functions-develop-serde/

    Example:
        ```python
        # Process text messages
        text_stream = pop.deserialize_value("deser", input_stream, StringSerDe())

        def process_text(msg):
            text = getattr(msg, '_deserialized_value', '')
            return text.upper()  # Convert to uppercase

        processed = op.map("process", text_stream.oks, process_text)
        pop.serialize_value("ser", processed, StringSerDe())
        ```
    """

    def serialize(self, input: Any) -> bytes: # pylint: disable=redefined-outer-name
        """Serialize input to UTF-8 bytes."""
        if input is None:
            return b""
        return str(input).encode("utf-8")

    def deserialize(self, input_bytes: bytes) -> str:
        """Deserialize bytes to UTF-8 string."""
        return input_bytes.decode("utf-8")


class JsonSerDe(SerDe):
    """
    JSON SerDe for JSON serialization/deserialization.

    **What**: Converts between bytes and Python objects via JSON
    **Why**: JSON is the most common structured data format in modern systems
    **When**: Processing structured data, APIs, microservices communication

    **Production Use Case**:
    - REST API message processing
    - Microservices communication
    - Event-driven architectures
    - Data transformation pipelines
    - Integration with web services

    Reference: https://pulsar.apache.org/docs/next/functions-develop-serde/

    Args:
        indent: Optional indentation for pretty-printing (useful for debugging)

    Example:
        ```python
        # Process JSON messages with pretty printing for debugging
        json_serde = JsonSerDe(indent=2)
        json_stream = pop.deserialize_value("deser", input_stream, json_serde)

        def enrich_json(msg):
            data = getattr(msg, '_deserialized_value', {})
            data['processed_at'] = datetime.now(tz=timezone.utc).isoformat()
            data['processor'] = 'my-service'
            return data

        enriched = op.map("enrich", json_stream.oks, enrich_json)
        pop.serialize_value("ser", enriched, json_serde)
        ```
    """

    def __init__(self, indent: Optional[int] = None):
        """Initialize JsonSerDe with optional indentation for pretty printing."""
        self.indent = indent

    def serialize(self, input: Any) -> bytes: # pylint: disable=redefined-outer-name
        """Serialize input to JSON bytes."""
        if input is None:
            return b"{}"
        try:
            return json.dumps(input, indent=self.indent).encode("utf-8")
        except (TypeError, ValueError) as e: # pylint: disable=broad-exception-caught
            raise ValueError(f"Failed to serialize to JSON: {e}") from e

    def deserialize(self, input_bytes: bytes) -> Any:
        """Deserialize JSON bytes to Python object."""
        if not input_bytes:
            return None
        try:
            return json.loads(input_bytes.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise ValueError(f"Failed to deserialize JSON: {e}") from e


class PickleSerDe(SerDe):
    """
    Pickle SerDe using Python's pickle module.

    **What**: Serializes Python objects using pickle protocol
    **Why**: Allows complex Python objects to be sent through Pulsar
    **When**: Internal Python-to-Python communication, complex object graphs

    **Production Use Case**:
    - Internal microservices (Python-only)
    - Complex data structures (classes, nested objects)
    - Machine learning model parameters
    - When JSON serialization is insufficient

    **⚠️ Security Warning**: Only use with trusted data sources. Pickle can execute
    arbitrary code during deserialization.

    Reference: https://pulsar.apache.org/docs/next/functions-develop-serde/

    Example:
        ```python
        # Process complex Python objects (internal systems only)
        pickle_stream = pop.deserialize_value("deser", input_stream, PickleSerDe())

        def process_complex_object(msg):
            obj = getattr(msg, '_deserialized_value')
            # Process complex Python object
            obj.process_timestamp = time.time()
            return obj

        processed = op.map("process", pickle_stream.oks, process_complex_object)
        ```
    """

    def serialize(self, input: Any) -> bytes: # pylint: disable=redefined-outer-name
        """Serialize input using pickle."""
        return pickle.dumps(input)

    def deserialize(self, input_bytes: bytes) -> Any:
        """Deserialize bytes using pickle."""
        return pickle.loads(input_bytes)


# Default SerDe instances for common use cases
DEFAULT_IDENTITY_SERDE = IdentitySerDe()
"""Default identity SerDe - passes data through unchanged."""

DEFAULT_STRING_SERDE = StringSerDe()
"""Default string SerDe - UTF-8 string processing."""

DEFAULT_JSON_SERDE = JsonSerDe()
"""Default JSON SerDe - compact JSON processing."""

DEFAULT_JSON_PRETTY_SERDE = JsonSerDe(indent=2)
"""Pretty-printed JSON SerDe - useful for debugging and human-readable output."""

DEFAULT_PICKLE_SERDE = PickleSerDe()
"""Default pickle SerDe - Python object serialization (use with caution)."""


@operator
def _pulsar_error_split(
    step_id: str, up: Stream[Union[PulsarSourceMessage, PulsarError]]
) -> PulsarOpOut:
    """Split the stream from PulsarSource between successful messages and errors.

    **Internal Operator**: This is used internally by other operators to separate
    successful messages from errors, enabling proper error handling.

    Args:
        step_id: Unique identifier for this operator step
        up: Stream containing both successful messages and errors

    Returns:
        PulsarOpOut with separated oks and errs streams
    """
    branch = op.branch(
        step_id.replace(".", "__"),
        up,
        lambda msg: isinstance(msg, PulsarSourceMessage),
    )
    # Cast the streams to the proper expected types.
    oks = cast(Stream[PulsarSourceMessage], branch.trues)
    errs = cast(Stream[PulsarError], branch.falses)
    return PulsarOpOut(oks, errs) # pylint: disable=too-many-function-args


@operator
def _to_sink(
    step_id: str, up: Stream[Union[PulsarSourceMessage, PulsarSinkMessage]]
) -> Stream[PulsarSinkMessage]:
    """Convert PulsarSourceMessage to PulsarSinkMessage automatically.

    **Internal Operator**: Automatically converts source messages to sink messages
    to enable seamless processing pipelines.

    **What**: Converts message types for output compatibility
    **Why**: Allows processing pipelines to work with both source and sink messages
    **When**: Used internally by output operators
    """

    def shim_mapper(
        msg: Union[PulsarSourceMessage, PulsarSinkMessage],
    ) -> PulsarSinkMessage:
        if isinstance(msg, PulsarSourceMessage):
            return msg.to_sink()
        return msg

    return op.map(
        step_id.replace(".", "__"),
        up,
        shim_mapper,
    )


@operator
def input( # pylint: disable=redefined-builtin, too-many-arguments
    step_id: str,
    flow: Dataflow,
    *,
    service_url: str,
    topics: List[str],
    subscription_name: str,
    consumer_config: Optional[Dict[str, Any]] = None,
) -> PulsarOpOut:
    """Consume from Pulsar as an input source with error handling.

    **What**: Creates a Pulsar consumer that reads from specified topics
    **Why**: Provides error-separated streams for robust message processing
    **When**: Use when you need error handling and monitoring capabilities

    **Production Benefits**:
    - Error stream separation for monitoring and alerting
    - Automatic error handling without crashing the pipeline
    - Support for multiple topics with single subscription
    - Configurable consumer properties

    Args:
        step_id: Unique identifier for this input step
        flow: Bytewax dataflow to add this input to
        service_url: Pulsar service URL (e.g., 'pulsar://localhost:6650')
        topics: List of topics to consume from
        subscription_name: Subscription name for the consumer group
        consumer_config: Additional consumer configuration properties

    Returns:
        PulsarOpOut with separated success and error streams

    Example:
        ```python
        # Basic usage with error handling
        pulsar_input = pop.input(
            "consumer",
            flow,
            service_url="pulsar://localhost:6650",
            topics=["orders", "payments"],
            subscription_name="order-processor"
        )

        # Process successful messages
        op.map("process", pulsar_input.oks, process_order)

        # Handle errors - send to dead letter queue
        op.inspect("log_errors", pulsar_input.errs)
        pop.output("dlq", pulsar_input.errs, topic="dead-letters")
        ```
    """
    return op.input(
        step_id.replace(".", "__"),
        flow,
        PulsarSource(
            service_url=service_url,
            topics=topics,
            subscription_name=subscription_name,
            consumer_config=consumer_config,
            # Don't raise on errors, since we split
            # the stream and let users handle that
            raise_on_errors=False,
        ),
    ).then(_pulsar_error_split, "split_err")


@operator
def output(
    step_id: str,
    up: Stream[Union[PulsarSourceMessage, PulsarSinkMessage]],
    *,
    service_url: str,
    topic: str,
    producer_config: Optional[Dict[str, Any]] = None,
) -> None:
    """Produce to Pulsar as an output sink.

    **What**: Creates a Pulsar producer that writes to specified topic
    **Why**: Provides a simple way to output processed messages to Pulsar
    **When**: Use for standard output scenarios with single topic

    **Production Benefits**:
    - Automatic message type conversion
    - Configurable producer properties
    - Built-in error handling
    - Support for message metadata preservation

    Args:
        step_id: Unique identifier for this output step
        up: Stream of messages to output
        service_url: Pulsar service URL (e.g., 'pulsar://localhost:6650')
        topic: Topic to produce messages to
        producer_config: Additional producer configuration properties

    Example:
        ```python
        # Basic output
        pop.output(
            "producer",
            processed_stream,
            service_url="pulsar://localhost:6650",
            topic="processed-orders"
        )

        # With producer configuration
        pop.output(
            "producer",
            processed_stream,
            service_url="pulsar://localhost:6650",
            topic="processed-orders",
            producer_config={
                "compression_type": pulsar.CompressionType.LZ4,
                "batching_enabled": True,
                "batching_max_messages": 1000
            }
        )
        ```
    """
    return _to_sink(
        step_id.replace(".", "__"),
        up,
    ).then(
        op.output,
        "pulsar_output",
        PulsarSink(service_url, topic, producer_config),
    )


@operator
def deserialize_value(
    step_id: str,
    up: Stream[PulsarSourceMessage],
    serde: Optional[SerDe] = None,
) -> PulsarOpOut:
    """Deserialize Pulsar message values using official Pulsar SerDe.

    **What**: Converts message bytes to Python objects using SerDe
    **Why**: Enables structured data processing with proper error handling
    **When**: Use when you need to process structured data (JSON, strings, etc.)

    **Production Benefits**:
    - Type-safe deserialization with proper error handling
    - Compatible with official Pulsar SerDe standards
    - Error stream separation for monitoring failed deserializations
    - Preserves all message metadata

    Args:
        step_id: Unique identifier for this deserialization step
        up: Stream of raw Pulsar messages
        serde: SerDe instance to use (defaults to StringSerDe)

    Returns:
        PulsarOpOut with deserialized messages and deserialization errors

    Example:
        ```python
        # JSON deserialization with error handling
        json_stream = pop.deserialize_value(
            "json_deser",
            input_stream,
            DEFAULT_JSON_SERDE
        )

        # Process successfully deserialized JSON
        def process_json_data(msg):
            data = getattr(msg, '_deserialized_value', {})
            # Process the Python dict/list
            return transform_data(data)

        processed = op.map("process", json_stream.oks, process_json_data)

        # Handle deserialization errors
        op.inspect("deser_errors", json_stream.errs)
        ```
    """
    if serde is None:
        serde = DEFAULT_STRING_SERDE

    def shim_mapper(
        msg: PulsarSourceMessage,
    ) -> Union[PulsarSourceMessage, PulsarError]:
        try:
            # Deserialize the value using the provided SerDe
            deserialized_value = serde.deserialize(msg.value)

            # Create a new message with the deserialized value stored in properties
            # and re-serialize for consistency with the bytes interface
            new_msg = PulsarSourceMessage(
                key=msg.key,
                value=(
                    serde.serialize(deserialized_value)
                    if hasattr(serde, "serialize")
                    else msg.value
                ),
                topic=msg.topic,
                properties={**msg.properties, "_serde_type": serde.__class__.__name__},
                message_id=msg.message_id,
                publish_timestamp=msg.publish_timestamp,
                event_timestamp=msg.event_timestamp,
                partition_key=msg.partition_key,
                ordering_key=msg.ordering_key,
            )
            # Store the actual deserialized value as an attribute for processing
            setattr(new_msg, "_deserialized_value", deserialized_value)
            return new_msg
        except Exception as e: # pylint: disable=broad-exception-caught
            return PulsarError(error=f"Deserialization error: {e}", msg=msg) # pylint: disable=too-many-function-args

    return op.map(
        step_id.replace(".", "__"),
        up,
        shim_mapper,
    ).then(_pulsar_error_split, "split")


@operator
def serialize_value(
    step_id: str,
    up: Stream[Union[PulsarSourceMessage, PulsarSinkMessage]],
    serde: Optional[SerDe] = None,
) -> Stream[PulsarSinkMessage]:
    """Serialize message values using official Pulsar SerDe.

    **What**: Converts Python objects back to bytes for Pulsar output
    **Why**: Ensures proper data format for downstream consumers
    **When**: Use before outputting processed data to Pulsar topics

    **Production Benefits**:
    - Consistent serialization format
    - Automatic handling of different data types
    - Preserves message metadata
    - Compatible with Pulsar ecosystem

    Args:
        step_id: Unique identifier for this serialization step
        up: Stream of messages with processed data
        serde: SerDe instance to use (defaults to StringSerDe)

    Returns:
        Stream of messages with serialized values ready for output

    Example:
        ```python
        # Serialize processed data back to JSON
        serialized = pop.serialize_value(
            "json_ser",
            processed_stream,
            DEFAULT_JSON_PRETTY_SERDE  # Pretty print for debugging
        )

        # Output serialized messages
        pop.output("output", serialized, topic="processed-data")
        ```
    """
    if serde is None:
        serde = DEFAULT_STRING_SERDE

    def shim_mapper(msg: PulsarSinkMessage) -> PulsarSinkMessage:
        # Get the value to serialize
        if hasattr(msg, "_deserialized_value"):
            # Use the deserialized value if available
            value = getattr(msg, "_deserialized_value")
        else:
            # Try to deserialize the current value first
            try:
                if hasattr(serde, "deserialize"):
                    value = serde.deserialize(msg.value)
                else:
                    value = (
                        msg.value.decode("utf-8")
                        if isinstance(msg.value, bytes)
                        else msg.value
                    )
            except Exception: # pylint: disable=broad-exception-caught
                # Fallback to treating as string
                value = (
                    msg.value.decode("utf-8", errors="ignore")
                    if isinstance(msg.value, bytes)
                    else str(msg.value)
                )

        # Serialize the value
        serialized_value = serde.serialize(value)

        return PulsarSinkMessage(
            key=msg.key,
            value=serialized_value,
            topic=msg.topic,
            properties={**msg.properties, "_serde_type": serde.__class__.__name__},
            partition_key=msg.partition_key,
            ordering_key=msg.ordering_key,
            event_timestamp=msg.event_timestamp,
            sequence_id=msg.sequence_id,
        )

    return _to_sink(
        step_id.replace(".", "__"),
        up,
    ).then(op.map, "map", shim_mapper)
