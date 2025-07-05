#!/usr/bin/env python3
"""
High-Performance IoT Data Processing
===================================

Optimized for processing high volumes of messages with minimal latency.

Features:
- Batching for throughput
- LZ4 compression for network efficiency
- Parallel processing with multiple workers
- Efficient error handling

Use case: IoT sensors, metrics, logs, real-time analytics

Run:
    python -m bytewax.run examples.high_performance:flow -w 4
"""

import json
import time
from typing import Optional

from bytewax import operators as op
from bytewax.dataflow import Dataflow
from bytewax_pulsar_connector import (
    PulsarSource,
    PulsarSink,
    PulsarSinkMessage,
    ProducerConfig,
    ConsumerConfig,
    PulsarCompressionType,
)
from bytewax_pulsar_connector.models import PulsarConsumerType


def process_sensor_data(msg) -> Optional[PulsarSinkMessage]:
    """Process IoT sensor data with minimal overhead."""
    try:
        # Fast JSON parsing
        data = json.loads(msg.value.decode('utf-8'))
        
        # Extract sensor data
        sensor_id = data.get('sensor_id', 'unknown')
        temperature = data.get('temperature', 0)
        humidity = data.get('humidity', 0)
        
        # Simple calculations
        temperature_f = (temperature * 9/5) + 32
        heat_index = calculate_heat_index(temperature_f, humidity)
        
        # Create enriched output
        output = {
            'sensor_id': sensor_id,
            'temperature_c': temperature,
            'temperature_f': temperature_f,
            'humidity': humidity,
            'heat_index': heat_index,
            'timestamp': data.get('timestamp', int(time.time() * 1000)),
            'processed_at': int(time.time() * 1000),
            'alert': heat_index > 80  # Flag high heat index
        }
        
        # Return optimized message
        return PulsarSinkMessage(
            value=json.dumps(output).encode('utf-8'),
            properties={
                'sensor_type': data.get('type', 'temperature'),
                'location': data.get('location', 'unknown')
            },
            partition_key=sensor_id,  # Ensure same sensor goes to same partition
            event_timestamp=output['timestamp']
        )
        
    except Exception as e:
        # Drop bad messages in high-performance scenarios
        # In production, you might want to route these to an error topic
        print(f"Error processing message: {msg.value.decode('utf-8', errors='ignore')}")
        print(f"Error: {e}")
        return None


def calculate_heat_index(temp_f: float, humidity: float) -> float:
    """Calculate heat index (feels-like temperature)."""
    if temp_f < 80:
        return temp_f
    
    # Simplified heat index formula
    hi = (
        -42.379 + 2.04901523 * temp_f + 10.14333127 * humidity
        - 0.22475541 * temp_f * humidity - 0.00683783 * temp_f * temp_f
        - 0.05481717 * humidity * humidity + 0.00122874 * temp_f * temp_f * humidity
        + 0.00085282 * temp_f * humidity * humidity - 0.00000199 * temp_f * temp_f * humidity * humidity
    )
    return round(hi, 2)


def main():
    # Create dataflow
    flow = Dataflow("high-performance-iot")
    
    # Consumer config optimized for throughput
    consumer_config = ConsumerConfig(
        consumer_type=PulsarConsumerType.Shared,  # Allow parallel processing
        receiver_queue_size=10000,  # Large buffer
        max_total_receiver_queue_size_across_partitions=50000,
        consumer_name="iot-consumer"
    )
    
    # Create source
    source = PulsarSource(
        service_url="pulsar://localhost:6650",
        topics=["input-topic"],
        subscription_name="iot-processor",
        consumer_config=consumer_config,
        raise_on_errors=False  # Don't crash on bad messages
    )
    
    # Producer config optimized for throughput
    producer_config = ProducerConfig(
        compression_type=PulsarCompressionType.LZ4,  # Fast compression
        batching_enabled=True,
        batching_max_messages=1000,  # Large batches
        batching_max_publish_delay_ms=10,  # Low latency
        max_pending_messages=50000,  # Large send queue
        max_pending_messages_across_partitions=200000,
        block_if_queue_full=True,  # Backpressure
        producer_name="iot-producer"
    )
    
    # Create sink
    sink = PulsarSink(
        service_url="pulsar://localhost:6650",
        topic="output-topic",
        producer_config=producer_config
    )
    
    # Build pipeline with filtering
    messages = op.input("read-sensors", flow, source)
    processed = op.map("process", messages, process_sensor_data)
    filtered = op.filter("filter-none", processed, lambda x: x is not None)
    op.output("write-processed", filtered, sink)
    
    return flow


# Create flow at module level for bytewax.run
flow = main()
