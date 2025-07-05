#!/usr/bin/env python3
"""
Production-Ready E-commerce Order Processing
===========================================

Production-grade example with comprehensive error handling, validation,
and monitoring for processing e-commerce orders.

Features:
- Data validation
- Error handling with dead letter queue
- Business logic processing
- Audit trails
- Monitoring hooks

Use case: E-commerce, financial services, API processing

Run:
    python -m bytewax.run examples.production_ready:flow
"""

import json
import time
from datetime import datetime
from decimal import Decimal
from typing import Dict, Any, Optional

from bytewax import operators as op
from bytewax.dataflow import Dataflow
from bytewax_pulsar_connector import (
    PulsarSource,
    PulsarSink,
    PulsarDynamicSink,
    PulsarSinkMessage,
    ProducerConfig,
    ConsumerConfig,
    PulsarCompressionType,
    operators as pop,
)
from bytewax_pulsar_connector.models import PulsarConsumerType


class OrderValidator:
    """Validate and process e-commerce orders."""
    
    @staticmethod
    def validate_order(order: Dict[str, Any]) -> Dict[str, Any]:
        """Validate order data and raise exceptions for invalid orders."""
        # Required fields
        required = ['order_id', 'customer_id', 'items', 'total']
        for field in required:
            if field not in order:
                raise ValueError(f"Missing required field: {field}")
        
        # Validate order ID format
        if not isinstance(order['order_id'], str) or len(order['order_id']) < 5:
            raise ValueError("Invalid order_id format")
        
        # Validate items
        if not isinstance(order['items'], list) or len(order['items']) == 0:
            raise ValueError("Order must contain at least one item")
        
        # Validate total
        total = Decimal(str(order['total']))
        if total <= 0:
            raise ValueError("Order total must be positive")
        
        # Calculate item total for verification
        calculated_total = Decimal('0')
        for item in order['items']:
            if 'price' not in item or 'quantity' not in item:
                raise ValueError("Item missing price or quantity")
            calculated_total += Decimal(str(item['price'])) * item['quantity']
        
        # Check if totals match (with small tolerance for rounding)
        if abs(calculated_total - total) > Decimal('0.01'):
            raise ValueError(f"Total mismatch: calculated {calculated_total}, got {total}")
        
        return order
    
    @staticmethod
    def enrich_order(order: Dict[str, Any]) -> Dict[str, Any]:
        """Add processing metadata and business logic."""
        # Add processing timestamp
        order['processed_at'] = datetime.now(tz=timezone.utc).isoformat()
        
        # Determine order priority
        total = Decimal(str(order['total']))
        if total > 1000:
            order['priority'] = 'high'
        elif total > 100:
            order['priority'] = 'medium'
        else:
            order['priority'] = 'low'
        
        # Add fraud risk score (simplified)
        order['risk_score'] = OrderValidator.calculate_risk_score(order)
        order['requires_review'] = order['risk_score'] > 0.7
        
        # Add fulfillment metadata
        order['fulfillment_status'] = 'pending'
        order['estimated_delivery'] = OrderValidator.estimate_delivery(order)
        
        return order
    
    @staticmethod
    def calculate_risk_score(order: Dict[str, Any]) -> float:
        """Calculate fraud risk score (0-1)."""
        score = 0.0
        
        # High value orders
        if Decimal(str(order['total'])) > 5000:
            score += 0.3
        
        # New customer (simplified check)
        if order.get('customer_id', '').startswith('new_'):
            score += 0.2
        
        # Rush delivery
        if order.get('shipping_method') == 'express':
            score += 0.1
        
        # Multiple high-value items
        expensive_items = sum(1 for item in order['items'] 
                            if Decimal(str(item['price'])) > 500)
        if expensive_items > 3:
            score += 0.2
        
        return min(score, 1.0)
    
    @staticmethod
    def estimate_delivery(order: Dict[str, Any]) -> str:
        """Estimate delivery date based on shipping method."""
        days = {
            'express': 1,
            'priority': 3,
            'standard': 5,
            'economy': 7
        }
        shipping = order.get('shipping_method', 'standard')
        delivery_days = days.get(shipping, 5)
        
        # Add processing time
        delivery_date = datetime.utcnow()
        delivery_date = delivery_date.replace(
            day=delivery_date.day + delivery_days
        )
        return delivery_date.isoformat()


def process_order(msg) -> PulsarSinkMessage:
    """Process an order with validation and enrichment."""
    try:
        # Parse order data
        order = json.loads(msg.value.decode('utf-8'))
        
        # Validate order
        validated_order = OrderValidator.validate_order(order)
        
        # Enrich with business logic
        enriched_order = OrderValidator.enrich_order(validated_order)
        
        # Create success message - route to output-topic
        return PulsarSinkMessage(
            value=json.dumps(enriched_order, default=str).encode('utf-8'),
            topic='output-topic',  # Success goes to output-topic
            properties={
                **msg.properties,
                'status': 'processed',
                'priority': enriched_order['priority'],
                'risk_score': str(enriched_order['risk_score']),
                'processor_version': '1.0.0'
            },
            partition_key=enriched_order['customer_id'],
            event_timestamp=int(time.time() * 1000)
        )
        
    except Exception as e:
        # Create error message for dead letter queue
        error_data = {
            'error_type': type(e).__name__,
            'error_message': str(e),
            'failed_at': datetime.now(tz=timezone.utc).isoformat(),
            'original_message': msg.value.decode('utf-8', errors='ignore'),
            'message_id': msg.message_id,
            'retry_count': int(msg.properties.get('retry_count', 0)) + 1
        }
        
        return PulsarSinkMessage(
            value=json.dumps(error_data).encode('utf-8'),
            topic='error-topic',  # Route errors to error-topic
            properties={
                **msg.properties,
                'status': 'error',
                'error': str(e),
                'retry_count': str(error_data['retry_count'])
            }
        )


def main():
    # Create dataflow
    flow = Dataflow("production-order-processing")
    
    # Consumer config with reliability settings
    consumer_config = ConsumerConfig(
        consumer_type=PulsarConsumerType.Shared,
        receiver_queue_size=1000,
        ack_timeout_millis=60000,  # 1 minute
        negative_ack_redelivery_delay_ms=5000,  # 5 seconds
        consumer_name="order-processor"
    )
    
    # Create source
    source = PulsarSource(
        service_url="pulsar://localhost:6650",
        topics=["input-topic"],
        subscription_name="order-processing",
        consumer_config=consumer_config
    )
    
    # Producer config with reliability settings
    producer_config = ProducerConfig(
        compression_type=PulsarCompressionType.ZSTD,  # Better compression
        batching_enabled=True,
        batching_max_messages=100,  # Smaller batches for lower latency
        batching_max_publish_delay_ms=100,
        max_pending_messages=5000,
        block_if_queue_full=True,
        send_timeout_millis=30000,
        producer_name="order-producer"
    )
    
    # Create dynamic sink that routes based on topic in message
    sink = PulsarDynamicSink(
        service_url="pulsar://localhost:6650",
        producer_config=producer_config
    )
    
    # Build pipeline
    messages = op.input("read-orders", flow, source)
    
    # Process orders with validation (returns messages with topic set)
    processed = op.map("process-orders", messages, process_order)
    
    # Output to appropriate topics (output-topic or error-topic)
    op.output("write-orders", processed, sink)
    
    return flow


# Create flow at module level for bytewax.run
flow = main() 