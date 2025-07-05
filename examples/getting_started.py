#!/usr/bin/env python3
"""
Getting Started with Bytewax Pulsar Connector
============================================

The simplest example to understand the basics of stream processing with Pulsar.

What this does:
1. Reads messages from 'input-topic'
2. Adds a timestamp to each message
3. Writes to 'output-topic'

Run:
    python -m bytewax.run examples.getting_started:flow
"""

import json
import time
from datetime import datetime

from bytewax import operators as op
from bytewax.dataflow import Dataflow
from bytewax_pulsar_connector import (
    PulsarSource,
    PulsarSink,
    PulsarSinkMessage,
)
from bytewax_pulsar_connector import PulsarSourceMessage


def add_timestamp(msg: PulsarSourceMessage):
    """Add a timestamp to the message."""
    try:
        # Parse the message
        data = json.loads(msg.value.decode('utf-8'))
        
        # Add timestamp
        data['processed_at'] = datetime.now().isoformat()
        data['processor'] = 'getting-started'
        msg.value = json.dumps(data).encode('utf-8')
        
        # Return as sink message
        return msg.to_sink()
    
    except Exception as e:
        print(f"Error processing message: {e}")
        # Return original message on error
        return msg.to_sink()


def main():
    # Create the dataflow
    flow = Dataflow("getting-started")
    
    # Configure source (where to read from)
    source = PulsarSource(
        service_url="pulsar://localhost:6650",
        topics=["input-topic"],
        subscription_name="getting-started-subscription"
    )
    
    # Configure sink (where to write to)
    sink = PulsarSink(
        service_url="pulsar://localhost:6650",
        topic="output-topic"
    )
    
    # Build the pipeline: source → process → sink
    messages = op.input("read", flow, source)
    processed = op.map("add-timestamp", messages, add_timestamp)
    op.output("write", processed, sink)
    
    return flow


# Create flow at module level for bytewax.run
flow = main()
