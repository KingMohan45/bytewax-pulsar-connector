"""Bytewax connectors for Apache Pulsar.

This module provides high-performance Pulsar connectors for Bytewax dataflows.
See connector.py for detailed usage examples and patterns.

"""

from .models import (
    ConsumerConfig,
    ProducerConfig,
    PulsarError,
    PulsarSourceMessage,
    PulsarSinkMessage,
    PulsarConsumerType,
    PulsarCompressionType,
)
from .operators import (
    PulsarOpOut,
    DEFAULT_IDENTITY_SERDE,
    DEFAULT_STRING_SERDE,
    DEFAULT_JSON_SERDE,
    DEFAULT_JSON_PRETTY_SERDE,
    DEFAULT_PICKLE_SERDE,
)
from .connector import PulsarSource, PulsarSink, PulsarDynamicSink

__all__ = [
    "PulsarSource",
    "PulsarSink",
    "PulsarDynamicSink",
    "PulsarSourceMessage",
    "PulsarSinkMessage",
    "PulsarError",
    "ProducerConfig",
    "ConsumerConfig",
    "PulsarCompressionType",
    "PulsarConsumerType",
    "PulsarOpOut",
    # Serializers and deserializers
    "DEFAULT_IDENTITY_SERDE",
    "DEFAULT_STRING_SERDE",
    "DEFAULT_JSON_SERDE",
    "DEFAULT_JSON_PRETTY_SERDE",
    "DEFAULT_PICKLE_SERDE",
]
