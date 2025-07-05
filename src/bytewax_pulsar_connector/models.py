# pylint: disable=C0103

"""Pydantic models for Pulsar connector configuration.

This module provides validated configuration models for Pulsar producers and consumers,
ensuring type safety and providing excellent IDE support and documentation.
Note: As default documentation does not include the enum values, we need to use the values from the enum.
for ConsumerType, CompressionType and Result
"""

from enum import Enum
from typing import Optional, Dict, Any, Callable
import pulsar
from pydantic import BaseModel, ConfigDict, Field, field_validator


class PulsarCompressionType(Enum):
    """
    Pulsar compression type.
    Ref: https://pulsar.apache.org/api/python/3.7.x/_pulsar.CompressionType.html
    """

    NONE = pulsar.CompressionType.NONE
    Zlib = pulsar.CompressionType.ZLib
    LZ4 = pulsar.CompressionType.LZ4
    ZSTD = pulsar.CompressionType.ZSTD
    SNAPPY = pulsar.CompressionType.SNAPPY


class PulsarConsumerType(Enum):
    """
    Pulsar consumer type.
    Ref: https://pulsar.apache.org/api/python/3.7.x/_pulsar.ConsumerType.html
    """

    Exclusive = pulsar.ConsumerType.Exclusive
    Shared = pulsar.ConsumerType.Shared
    Failover = pulsar.ConsumerType.Failover
    KeyShared = pulsar.ConsumerType.KeyShared


class PulsarResult(Enum):
    """
    Result of a Pulsar operation.

    Ref: https://pulsar.apache.org/api/python/3.7.x/_pulsar.Result.html
    """

    Ok = pulsar.Result.Ok
    UnknownError = pulsar.Result.UnknownError
    InvalidConfiguration = pulsar.Result.InvalidConfiguration
    Timeout = pulsar.Result.Timeout
    LookupError = pulsar.Result.LookupError
    ConnectError = pulsar.Result.ConnectError
    ReadError = pulsar.Result.ReadError
    AuthenticationError = pulsar.Result.AuthenticationError
    AuthorizationError = pulsar.Result.AuthorizationError
    ErrorGettingAuthenticationData = pulsar.Result.ErrorGettingAuthenticationData
    BrokerMetadataError = pulsar.Result.BrokerMetadataError
    BrokerPersistenceError = pulsar.Result.BrokerPersistenceError
    ChecksumError = pulsar.Result.ChecksumError
    ConsumerBusy = pulsar.Result.ConsumerBusy
    NotConnected = pulsar.Result.NotConnected
    AlreadyClosed = pulsar.Result.AlreadyClosed
    InvalidMessage = pulsar.Result.InvalidMessage
    ConsumerNotInitialized = pulsar.Result.ConsumerNotInitialized
    ProducerNotInitialized = pulsar.Result.ProducerNotInitialized
    ProducerBusy = pulsar.Result.ProducerBusy
    TooManyLookupRequestException = pulsar.Result.TooManyLookupRequestException
    InvalidTopicName = pulsar.Result.InvalidTopicName
    InvalidUrl = pulsar.Result.InvalidUrl
    ServiceUnitNotReady = pulsar.Result.ServiceUnitNotReady
    OperationNotSupported = pulsar.Result.OperationNotSupported
    ProducerBlockedQuotaExceededError = pulsar.Result.ProducerBlockedQuotaExceededError
    ProducerBlockedQuotaExceededException = (
        pulsar.Result.ProducerBlockedQuotaExceededException
    )
    ProducerQueueIsFull = pulsar.Result.ProducerQueueIsFull
    MessageTooBig = pulsar.Result.MessageTooBig
    TopicNotFound = pulsar.Result.TopicNotFound
    SubscriptionNotFound = pulsar.Result.SubscriptionNotFound
    ConsumerNotFound = pulsar.Result.ConsumerNotFound
    UnsupportedVersionError = pulsar.Result.UnsupportedVersionError
    TopicTerminated = pulsar.Result.TopicTerminated
    CryptoError = pulsar.Result.CryptoError
    IncompatibleSchema = pulsar.Result.IncompatibleSchema
    ConsumerAssignError = pulsar.Result.ConsumerAssignError
    CumulativeAcknowledgementNotAllowedError = (
        pulsar.Result.CumulativeAcknowledgementNotAllowedError
    )
    TransactionCoordinatorNotFoundError = (
        pulsar.Result.TransactionCoordinatorNotFoundError
    )
    InvalidTxnStatusError = pulsar.Result.InvalidTxnStatusError
    NotAllowedError = pulsar.Result.NotAllowedError
    TransactionConflict = pulsar.Result.TransactionConflict
    TransactionNotFound = pulsar.Result.TransactionNotFound
    ProducerFenced = pulsar.Result.ProducerFenced
    MemoryBufferIsFull = pulsar.Result.MemoryBufferIsFull
    Interrupted = pulsar.Result.Interrupted


class PulsarSourceMessage(BaseModel):
    """Message read from Pulsar with complete metadata.

    **What**: Represents a message consumed from a Pulsar topic
    **Why**: Provides access to all Pulsar message metadata for processing decisions
    **When**: Used as input to your processing functions

    **Production Benefits**:
    - Complete message metadata for routing and processing decisions
    - Timestamps for time-based processing and late data handling
    - Partition and ordering keys for maintaining message order
    - Properties for custom metadata and tracing

    Attributes:
        key: Message key (optional, used for partitioning)
        value: Message payload as bytes
        topic: Source topic name
        properties: Custom message properties (dict)
        message_id: Unique message identifier
        publish_timestamp: When message was published (milliseconds)
        event_timestamp: Application-provided event time (milliseconds)
        partition_key: Key used for topic partitioning
        ordering_key: Key used for message ordering within partition

    Example:
        ```python
        def process_message(msg: PulsarSourceMessage):
            print(f"Processing message from {msg.topic}")
            print(f"Event time: {msg.event_timestamp}")
            print(f"Properties: {msg.properties}")

            # Access message data
            data = json.loads(msg.value.decode('utf-8'))

            # Use metadata for processing decisions
            if msg.event_timestamp and is_late_data(msg.event_timestamp):
                handle_late_arrival(data)

            return process_data(data)
        ```
    """

    key: Optional[bytes]
    value: bytes

    topic: Optional[str] = None
    properties: Dict[str, str] = Field(default_factory=dict)
    message_id: Optional[str] = None
    publish_timestamp: Optional[int] = None
    event_timestamp: Optional[int] = None
    partition_key: Optional[str] = None
    ordering_key: Optional[str] = None

    def to_sink(self) -> "PulsarSinkMessage":
        """Convert a source message to be used with a sink.

        **What**: Transforms this source message into a sink message
        **Why**: Enables easy pass-through processing while preserving metadata
        **When**: Use when you want to forward messages with minimal changes

        Returns:
            PulsarSinkMessage with preserved metadata

        Example:
            ```python
            def forward_with_enrichment(msg: PulsarSourceMessage):
                # Add processing metadata
                enriched_properties = {
                    **msg.properties,
                    'processed_by': 'my-service',
                    'processed_at': str(int(time.time()))
                }

                sink_msg = msg.to_sink()
                sink_msg.properties.update(enriched_properties)
                return sink_msg
            ```
        """
        return PulsarSinkMessage(
            key=self.key,
            value=self.value,
            properties=self.properties,
            partition_key=self.partition_key,
            ordering_key=self.ordering_key,
            event_timestamp=self.event_timestamp,
        )


class PulsarError(BaseModel):
    """Error from a Pulsar source with context.

    **What**: Represents an error that occurred during message processing
    **Why**: Provides error context for debugging and monitoring
    **When**: Generated when message consumption or processing fails

    **Production Benefits**:
    - Detailed error information for debugging
    - Original message context for error analysis
    - Enables dead letter queue patterns
    - Supports error monitoring and alerting

    Attributes:
        error: Human-readable error message
        msg: Original message that caused the error

    Example:
        ```python
        def handle_errors(error: PulsarError):
            # Log error with context
            logger.error(f"Processing failed: {error.error}")
            logger.error(f"Original message: {error.msg.message_id}")

            # Send to dead letter queue
            dead_letter_msg = PulsarSinkMessage(
                key=error.msg.key,
                value=error.msg.value,
                properties={
                    **error.msg.properties,
                    'error': error.error,
                    'failed_at': str(int(time.time()))
                }
            )
            return dead_letter_msg
        ```
    """

    error: str
    """Error message."""

    msg: PulsarSourceMessage
    """Message attached to that error."""


class PulsarSinkMessage(BaseModel):
    """Message to be written to Pulsar with full control.

    **What**: Represents a message to be produced to a Pulsar topic
    **Why**: Provides full control over message properties and routing
    **When**: Used as output from your processing functions

    **Production Benefits**:
    - Complete control over message metadata
    - Support for custom routing and partitioning
    - Event time preservation for time-based processing
    - Custom properties for tracing and monitoring

    Attributes:
        key: Message key (optional, used for partitioning)
        value: Message payload as bytes
        topic: Target topic (optional, can override sink topic)
        properties: Custom message properties
        partition_key: Key for topic partitioning
        ordering_key: Key for message ordering
        event_timestamp: Event time (milliseconds)
        sequence_id: Message sequence for exactly-once semantics

    Example:
        ```python
        def create_output_message(processed_data):
            return PulsarSinkMessage(
                key=processed_data.get('user_id', '').encode('utf-8'),
                value=json.dumps(processed_data).encode('utf-8'),
                properties={
                    'content_type': 'application/json',
                    'processing_version': '1.2.3',
                    'trace_id': generate_trace_id()
                },
                partition_key=processed_data.get('tenant_id'),
                event_timestamp=int(time.time() * 1000)
            )
        ```
    """

    key: Optional[bytes] = None
    value: bytes

    topic: Optional[str] = None
    properties: Dict[str, str] = Field(default_factory=dict)
    partition_key: Optional[str] = None
    ordering_key: Optional[str] = None
    event_timestamp: Optional[int] = None
    sequence_id: Optional[int] = None


class ProducerConfig(BaseModel):
    """Validated configuration for Pulsar producers.

    This model provides type-safe configuration with sensible defaults
    optimized for high-throughput scenarios (millions of messages/day).

    Example:
        ```python
        from bytewax_pulsar_connector.models import ProducerConfig, CompressionType

        # High-throughput configuration
        config = ProducerConfig(
            compression_type=CompressionType.LZ4,
            batching_enabled=True,
            batching_max_messages=1000,
            batching_max_publish_delay_ms=10,
            max_pending_messages=50000,
            message_callback=lambda msg_id: logger.info(f"Sent: {msg_id}")
        )

        sink = PulsarSink(
            service_url="pulsar://localhost:6650",
            topic="high-volume-topic",
            producer_config=config.to_pulsar_config()
        )
        ```
    """

    # Basic configuration
    producer_name: Optional[str] = Field(
        None, description="Producer name for identification in Pulsar admin"
    )

    # Compression settings
    compression_type: PulsarCompressionType = Field(
        PulsarCompressionType.NONE,
        description="Compression algorithm for messages (LZ4 recommended for high throughput)",
    )

    # Batching configuration (critical for high throughput)
    batching_enabled: bool = Field(
        True, description="Enable message batching for better throughput"
    )
    batching_max_messages: int = Field(
        1000, ge=1, le=5000, description="Maximum messages per batch"
    )
    batching_max_publish_delay_ms: int = Field(
        10,
        ge=1,
        le=1000,
        description="Maximum delay before sending incomplete batch (ms)",
    )

    # Queue management
    max_pending_messages: int = Field(
        50000, ge=1000, le=100000, description="Maximum messages in producer queue"
    )
    max_pending_messages_across_partitions: int = Field(
        100000, ge=1000, le=500000, description="Maximum messages across all partitions"
    )
    block_if_queue_full: bool = Field(
        True, description="Block when queue is full (enables backpressure)"
    )

    # Timeout settings
    send_timeout_millis: int = Field(
        30000,  # 30 seconds
        ge=1000,
        le=300000,  # Max 5 minutes
        description="Timeout for send operations (ms)",
    )
    # Encryption (optional)
    encryption_key: Optional[str] = Field(
        None, description="Public key for message encryption"
    )
    crypto_key_reader: Optional[Any] = Field(
        None, description="Crypto key reader for encryption"
    )

    message_callback: Optional[Callable[[pulsar.Result, pulsar.MessageId], None]] = (
        Field(
            None,
            description="Callback function called when message is successfully sent",
        )
    )

    model_config = ConfigDict(arbitrary_types_allowed="ignore")

    @field_validator("batching_max_publish_delay_ms", mode="after")
    @classmethod
    def validate_batching_delay(cls, v, values):
        """Ensure batching delay makes sense with batching enabled."""
        if values.data.get("batching_enabled") and v > 100:
            print(f"Warning: High batching delay ({v}ms) may impact latency")
        return v

    @field_validator("max_pending_messages_across_partitions", mode="after")
    @classmethod
    def validate_cross_partition_limit(cls, v, values):
        """Ensure cross-partition limit is higher than single partition limit."""
        max_pending = values.data.get("max_pending_messages", 50000)
        if v < max_pending:
            raise ValueError(
                f"max_pending_messages_across_partitions ({v}) must be >= "
                f"max_pending_messages ({max_pending})"
            )
        return v

    def model_dump(self, *args, **kwargs):
        """
        Override model_dump to exclude message_callback from the output.
        """
        exclude: set[str] = kwargs.pop("exclude", set())
        exclude.add("compression_type")
        kwargs["exclude"] = exclude
        return {  # pylint: disable=E1101
            **super().model_dump(*args, **kwargs),
            "compression_type": self.compression_type.value,
        }

    def to_pulsar_config(self) -> Dict[str, Any]:
        """Convert to Pulsar producer configuration dictionary.

        Returns:
            Dictionary compatible with Pulsar producer configuration
        """
        config = {
            "batching_enabled": self.batching_enabled,
            "batching_max_messages": self.batching_max_messages,
            "batching_max_publish_delay_ms": self.batching_max_publish_delay_ms,
            "max_pending_messages": self.max_pending_messages,
            "max_pending_messages_across_partitions": self.max_pending_messages_across_partitions,
            "block_if_queue_full": self.block_if_queue_full,
            "send_timeout_millis": self.send_timeout_millis,
            "compression_type": self.compression_type.value, # pylint: disable=E1101
        }

        # Add optional fields
        if self.producer_name:
            config["producer_name"] = self.producer_name
        if self.encryption_key:
            config["encryption_key"] = self.encryption_key
        if self.crypto_key_reader:
            config["crypto_key_reader"] = self.crypto_key_reader

        return config

    def for_high_throughput(self) -> "ProducerConfig":
        """Return a copy optimized for high throughput scenarios.

        Returns:
            ProducerConfig with settings optimized for millions of messages/day
        """
        return self.copy(
            update={
                "compression_type": PulsarCompressionType.LZ4,
                "batching_enabled": True,
                "batching_max_messages": 1000,
                "batching_max_publish_delay_ms": 10,
                "max_pending_messages": 50000,
                "max_pending_messages_across_partitions": 200000,
                "block_if_queue_full": True,
            }
        )

    def for_low_latency(self) -> "ProducerConfig":
        """Return a copy optimized for low latency scenarios.

        Returns:
            ProducerConfig with settings optimized for minimal latency
        """
        return self.copy(
            update={
                "compression_type": PulsarCompressionType.NONE,
                "batching_enabled": False,
                "max_pending_messages": 5000,
                "send_timeout_millis": 5000,
            }
        )


class ConsumerConfig(BaseModel):
    """Validated configuration for Pulsar consumers.

    This model provides type-safe configuration for consumers with
    defaults optimized for reliable message processing.

    Example:
        ```python
        from bytewax_pulsar_connector.models import ConsumerConfig

        config = ConsumerConfig(
            consumer_type="shared",
            receiver_queue_size=10000,
            max_total_receiver_queue_size_across_partitions=50000,
            ack_timeout_millis=60000
        )

        source = PulsarSource(
            service_url="pulsar://localhost:6650",
            topics=["input-topic"],
            subscription_name="my-subscription",
            consumer_config=config.to_pulsar_config()
        )
        ```
    """

    consumer_name: Optional[str] = Field(
        None, description="Consumer name for identification"
    )

    consumer_type: PulsarConsumerType = Field(
        PulsarConsumerType.KeyShared,
        description="Consumer type: 'exclusive', 'shared', 'failover', or 'key_shared'",
    )

    receiver_queue_size: int = Field(
        1000, ge=1, le=100000, description="Size of consumer receive queue"
    )

    max_total_receiver_queue_size_across_partitions: int = Field(
        50000,
        ge=1000,
        le=500000,
        description="Maximum queue size across all partitions",
    )

    negative_ack_redelivery_delay_ms: int = Field(
        1000,  # 1 second
        ge=100,
        description="Delay before redelivering negatively acknowledged messages (ms)",
    )

    replicate_subscription_state_enabled: bool = Field(
        False, description="Enable subscription state replication"
    )

    auto_ack_oldest_chunked_message_on_queue_full: bool = Field(
        False, description="Auto-ack oldest chunked message on queue full"
    )

    def model_dump(self, *args, **kwargs):
        """
        Override model_dump to exclude consumer_type from the output.
        """
        exclude: set[str] = kwargs.pop("exclude", set())
        exclude.add("consumer_type")
        kwargs["exclude"] = exclude
        return {
            **super().model_dump(*args, **kwargs),
            "consumer_type": self.consumer_type.value, # pylint: disable=E1101
        }
