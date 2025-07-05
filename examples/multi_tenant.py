#!/usr/bin/env python3
"""
Multi-Tenant SaaS Platform Message Routing
=========================================

Dynamic routing for multi-tenant architectures where messages are
routed to tenant-specific topics based on content.

Features:
- Tenant-based topic routing
- Geographic compliance (GDPR, etc.)
- Tier-based processing (free/premium/enterprise)
- Automatic producer management per topic

Use case: Multi-tenant SaaS, geographic routing, A/B testing

Run:
    python -m bytewax.run examples.multi_tenant:flow
"""

import json
import time
from typing import Dict, Any

from bytewax import operators as op
from bytewax.dataflow import Dataflow
from bytewax_pulsar_connector import (
    PulsarSource,
    PulsarSinkMessage,
    PulsarDynamicSink,
    ProducerConfig,
    ConsumerConfig,
    PulsarCompressionType,
)
from bytewax_pulsar_connector.models import PulsarConsumerType


def route_by_tenant(msg) -> PulsarSinkMessage:
    """Extract tenant information and route to appropriate topic."""
    try:
        data = json.loads(msg.value.decode('utf-8'))
        
        # Extract or determine tenant information
        tenant_id = data.get('tenant_id') or msg.properties.get('tenant_id', 'default')
        
        # Simulate tenant tier lookup (in production, query from database)
        tenant_tier = determine_tenant_tier(tenant_id)
        
        # Determine region for compliance
        region = determine_region(data, msg.properties)
        
        # Classify event type
        event_type = classify_event(data)
        
        # Determine topic based on tenant tier and region
        if tenant_tier == 'enterprise':
            # Enterprise tenants get dedicated topics per region
            topic = f"tenant-{tenant_id}-{region}-{event_type}"
        elif tenant_tier == 'premium':
            # Premium tenants get dedicated topics
            topic = f"tenant-{tenant_id}-{event_type}"
        else:
            # Free tier shares topics by region
            topic = f"shared-{region}-{event_type}"
        
        # Add processing metadata
        data['routing_metadata'] = {
            'tenant_id': tenant_id,
            'tenant_tier': tenant_tier,
            'region': region,
            'event_type': event_type,
            'processed_at': int(time.time() * 1000),
            'routed_to': topic
        }
        
        # Create routable message with topic set
        return PulsarSinkMessage(
            value=json.dumps(data).encode('utf-8'),
            topic=topic,  # Dynamic topic per message
            properties={
                **msg.properties,
                'tenant_id': tenant_id,
                'tenant_tier': tenant_tier,
                'region': region,
                'event_type': event_type,
                'original_topic': msg.topic or 'unknown'
            },
            partition_key=tenant_id,  # Ensure tenant data stays together
            event_timestamp=msg.event_timestamp or int(time.time() * 1000)
        )
        
    except Exception as e:
        # Route errors to shared error topic
        return PulsarSinkMessage(
            value=msg.value,
            topic="shared-global-errors",  # Error topic
            properties={
                **msg.properties,
                'error': str(e),
                'original_topic': msg.topic or 'unknown'
            }
        )


def determine_tenant_tier(tenant_id: str) -> str:
    """Determine tenant tier (in production, query from database)."""
    # Simulate tier lookup
    if tenant_id.startswith('ent_'):
        return 'enterprise'
    elif tenant_id.startswith('prem_'):
        return 'premium'
    else:
        return 'free'


def determine_region(data: Dict[str, Any], properties: Dict[str, str]) -> str:
    """Determine region for data compliance."""
    # Check explicit region
    region = data.get('region') or properties.get('region')
    if region:
        return region
    
    # Infer from IP or other data (simplified)
    ip = data.get('ip_address', '')
    if ip.startswith('10.1.'):
        return 'us-east'
    elif ip.startswith('10.2.'):
        return 'eu-west'
    elif ip.startswith('10.3.'):
        return 'asia-pacific'
    else:
        return 'us-east'  # default


def classify_event(data: Dict[str, Any]) -> str:
    """Classify event type for routing."""
    # Check explicit type
    if 'event_type' in data:
        return data['event_type']
    
    # Infer from data structure
    if 'order_id' in data:
        return 'orders'
    elif 'user_id' in data and 'action' in data:
        return 'user-events'
    elif 'metric_name' in data:
        return 'metrics'
    elif 'log_level' in data:
        return 'logs'
    else:
        return 'general'


def main():
    # Create dataflow
    flow = Dataflow("multi-tenant-routing")
    
    # Consumer configuration
    consumer_config = ConsumerConfig(
        consumer_type=PulsarConsumerType.Shared,
        receiver_queue_size=5000,
        consumer_name="tenant-router"
    )
    
    # Source for multi-tenant events
    source = PulsarSource(
        service_url="pulsar://localhost:6650",
        topics=["input-topic"],
        subscription_name="tenant-router",
        consumer_config=consumer_config
    )
    
    # Producer configuration optimized for multi-tenant use
    producer_config = ProducerConfig(
        batching_enabled=True,
        batching_max_messages=500,
        batching_max_publish_delay_ms=100,
        compression_type=PulsarCompressionType.LZ4,
        block_if_queue_full=True,
        max_pending_messages=10000,
        producer_name="multi-tenant-router"
    )
    
    # Create dynamic sink - automatically routes based on message.topic
    dynamic_sink = PulsarDynamicSink(
        service_url="pulsar://localhost:6650",
        producer_config=producer_config
    )
    
    # Build pipeline
    messages = op.input("read-events", flow, source)
    routed_messages = op.map("route-by-tenant", messages, route_by_tenant)
    op.output("write-to-tenant-topics", routed_messages, dynamic_sink)
    
    return flow


# Create flow at module level for bytewax.run
flow = main()
