#!/usr/bin/env python3
"""
Message Delivery Tracking and Monitoring
=======================================

Track message delivery with callbacks, collect metrics, and monitor
pipeline health in real-time.

Features:
- Message delivery callbacks
- Real-time metrics collection
- Throughput and latency tracking
- Error rate monitoring
- SLA compliance checking

Use case: Audit trails, SLA monitoring, debugging, performance tuning

Run:
    python -m bytewax.run examples.message_tracking:flow
"""

import json
import time
import threading
from datetime import datetime, timezone
from typing import Dict, List, Optional
from collections import defaultdict, deque

import pulsar
from bytewax import operators as op
from bytewax.dataflow import Dataflow
from bytewax_pulsar_connector import (
    PulsarSource,
    PulsarDynamicSink,
    PulsarSinkMessage,
    ProducerConfig,
    ConsumerConfig,
    PulsarCompressionType,
)
from bytewax_pulsar_connector.models import PulsarConsumerType


class MetricsCollector:
    """Collect and report pipeline metrics."""
    
    def __init__(self, window_size_seconds: int = 60):
        self.window_size = window_size_seconds
        self.messages_sent = 0
        self.messages_failed = 0
        self.latencies = deque(maxlen=10000)  # Keep last 10k latencies
        self.message_sizes = deque(maxlen=10000)
        self.errors_by_type = defaultdict(int)
        self.messages_by_topic = defaultdict(int)
        self.topic_timestamps = defaultdict(list)  # Track when messages were sent to each topic
        self.start_time = time.time()
        self.lock = threading.Lock()
        
        # SLA thresholds
        self.sla_latency_ms = 100  # 100ms latency SLA
        self.sla_success_rate = 0.999  # 99.9% success rate
        
        # Track previous state for change detection
        self.prev_messages_sent = 0
        self.prev_messages_by_topic = {}
        
    def on_message_sent(self, result: pulsar.Result, msg_id: pulsar.MessageId):
        """Callback when message is successfully sent."""
        with self.lock:
            self.messages_sent += 1
    
    def on_message_failed(self, error: Exception, result: dict):
        """Track failed messages."""
        with self.lock:
            self.messages_failed += 1
            self.errors_by_type[type(error).__name__] += 1
    
    def get_metrics(self) -> Dict:
        """Get current metrics snapshot."""
        with self.lock:
            total_messages = self.messages_sent + self.messages_failed
            success_rate = self.messages_sent / total_messages if total_messages > 0 else 0
            
            # Calculate latency percentiles
            latencies_sorted = sorted(self.latencies) if self.latencies else [0]
            p50 = latencies_sorted[len(latencies_sorted) // 2]
            p95 = latencies_sorted[int(len(latencies_sorted) * 0.95)]
            p99 = latencies_sorted[int(len(latencies_sorted) * 0.99)]
            
            # Calculate throughput
            elapsed = time.time() - self.start_time
            throughput = self.messages_sent / elapsed if elapsed > 0 else 0
            
            # Check SLA compliance
            sla_latency_met = p99 <= self.sla_latency_ms
            sla_success_met = success_rate >= self.sla_success_rate
            
            return {
                'messages_sent': self.messages_sent,
                'messages_failed': self.messages_failed,
                'success_rate': success_rate,
                'throughput_per_sec': throughput,
                'latency_p50_ms': p50,
                'latency_p95_ms': p95,
                'latency_p99_ms': p99,
                'avg_message_size': sum(self.message_sizes) / len(self.message_sizes) if self.message_sizes else 0,
                'errors_by_type': dict(self.errors_by_type),
                'messages_by_topic': dict(self.messages_by_topic),
                'sla_compliance': {
                    'latency_met': sla_latency_met,
                    'success_rate_met': sla_success_met,
                    'overall': sla_latency_met and sla_success_met
                },
                'runtime_seconds': elapsed
            }
    
    def print_dashboard(self, force_print=False):
        """Print a metrics dashboard."""
        # Get metrics first (this will acquire and release the lock)
        metrics = self.get_metrics()
        
        with self.lock:
            # Calculate total tracked messages
            total_tracked = sum(self.messages_by_topic.values())
            
            # Check if there are changes
            has_changes = (
                self.messages_sent != self.prev_messages_sent or
                self.messages_by_topic != self.prev_messages_by_topic or
                total_tracked > 0 and self.prev_messages_sent == 0  # First messages
            )
            
            # Show metrics if there are changes or force_print is True
            if not has_changes and not force_print:
                print(f"\n📊 PIPELINE METRICS: No change (Total: {self.messages_sent} sent via callback, {total_tracked} tracked by topic)")
                return
            
            # Update previous state
            self.prev_messages_sent = self.messages_sent
            self.prev_messages_by_topic = dict(self.messages_by_topic)
        print("\n" + "="*60)
        print("📊 PIPELINE METRICS DASHBOARD")
        print("="*60)
        
        print(f"\n📈 Throughput:")
        print(f"   Messages tracked: {total_tracked:,}")
        print(f"   Messages sent (callback): {metrics['messages_sent']:,}")
        print(f"   Messages failed: {metrics['messages_failed']:,}")
        print(f"   Success rate: {metrics['success_rate']:.2%}")
        print(f"   Throughput: {metrics['throughput_per_sec']:,.2f} msg/sec")
        
        print(f"\n⏱️  Latency:")
        print(f"   P50: {metrics['latency_p50_ms']:.2f} ms")
        print(f"   P95: {metrics['latency_p95_ms']:.2f} ms")
        print(f"   P99: {metrics['latency_p99_ms']:.2f} ms")
        
        print(f"\n📊 Messages by Topic:")
        if metrics['messages_by_topic']:
            total_tracked = sum(metrics['messages_by_topic'].values())
            current_time = time.time()
            
            for topic, count in sorted(metrics['messages_by_topic'].items()):
                percentage = (count / total_tracked * 100) if total_tracked > 0 else 0
                
                # Calculate recent activity (last 30 seconds)
                with self.lock:
                    recent_timestamps = [
                        ts for ts in self.topic_timestamps.get(topic, [])
                        if current_time - ts <= 30
                    ]
                    recent_count = len(recent_timestamps)
                    recent_rate = recent_count / 30.0 if recent_count > 0 else 0
                
                activity_indicator = "🔥" if recent_count > 0 else "💤"
                print(f"   {activity_indicator} {topic}: {count:,} ({percentage:.1f}%) | Recent: {recent_count} msgs/30s ({recent_rate:.2f}/s)")
            
            print(f"   ─────────────────────────────")
            print(f"   TOTAL: {total_tracked:,} messages")
        else:
            print("   No messages sent yet")
        
        if metrics['errors_by_type']:
            print(f"\n❌ Errors by Type:")
            for error_type, count in metrics['errors_by_type'].items():
                print(f"   {error_type}: {count}")
        
        print(f"\n✅ SLA Compliance:")
        print(f"   Latency (<{self.sla_latency_ms}ms): {'✓' if metrics['sla_compliance']['latency_met'] else '✗'}")
        print(f"   Success (>{self.sla_success_rate:.1%}): {'✓' if metrics['sla_compliance']['success_rate_met'] else '✗'}")
        print(f"   Overall: {'✓ PASS' if metrics['sla_compliance']['overall'] else '✗ FAIL'}")
        
        print(f"\n⏰ Runtime: {metrics['runtime_seconds']:.2f} seconds")
        print("="*60)


# Global metrics collector
metrics = MetricsCollector()


def create_tracked_callback(topic: str):
    """Create a callback that tracks message delivery."""
    def callback(result: pulsar.Result, msg_id: pulsar.MessageId):
        metadata = {
            'topic': topic,
            'timestamp': time.time()
        }
        # Simple tracking since we can't modify result
        metrics.on_message_sent(result, msg_id)
    return callback


def track_message_sent(msg: PulsarSinkMessage) -> PulsarSinkMessage:
    """Track that we're about to send a message to a topic."""
    with metrics.lock:
        current_time = time.time()
        metrics.messages_by_topic[msg.topic] += 1
        metrics.topic_timestamps[msg.topic].append(current_time)
        
        # Keep only last 100 timestamps per topic to avoid memory issues
        if len(metrics.topic_timestamps[msg.topic]) > 100:
            metrics.topic_timestamps[msg.topic] = metrics.topic_timestamps[msg.topic][-100:]
        
        # Debug output to confirm messages are being tracked
        total_messages = sum(metrics.messages_by_topic.values())
        if total_messages == 1 or total_messages % 10 == 0:
            print(f"✉️  Tracked {total_messages} messages so far")
    return msg


def process_with_tracking(msg) -> Optional[PulsarSinkMessage]:
    """Process message with tracking metadata."""
    start_time = time.time()
    
    try:
        # Parse message
        data = json.loads(msg.value.decode('utf-8'))
        
        # Simulate processing
        processing_type = data.get('type', 'standard')
        if processing_type == 'heavy':
            time.sleep(0.01)  # Simulate heavy processing
        
        # Add tracking metadata
        data['tracking'] = {
            'processed_at': datetime.now(tz=timezone.utc).isoformat(),
            'processing_time_ms': (time.time() - start_time) * 1000,
            'pipeline': 'message-tracking-example',
            'message_id': msg.message_id
        }
        
        # Determine output topic based on priority
        priority = data.get('priority', 'normal')
        output_topic = f"tracked-{priority}"
        
        # Create tracked message
        output = PulsarSinkMessage(
            key=msg.key,
            value=json.dumps(data).encode('utf-8'),
            topic=output_topic,  # Dynamic topic routing
            properties={
                **msg.properties,
                'tracked': 'true',
                'processing_time': str(data['tracking']['processing_time_ms'])
            },
            partition_key=msg.partition_key if hasattr(msg, 'partition_key') else None,
            event_timestamp=msg.event_timestamp if hasattr(msg, 'event_timestamp') else None
        )
        
        return output
        
    except Exception as e:
        # Create error data
        error_data = {
            'original_data': msg.value.decode('utf-8', errors='ignore'),
            'error': str(e),
            'error_type': type(e).__name__,
            'failed_at': datetime.now(tz=timezone.utc).isoformat(),
            'processing_time_ms': (time.time() - start_time) * 1000
        }
        
        # Track as error but still a sent message
        metrics.errors_by_type[type(e).__name__] += 1
        
        # Create error message - this will still be sent successfully
        error_message = PulsarSinkMessage(
            key=msg.key,
            value=json.dumps(error_data).encode('utf-8'),
            topic='error-topic',  # Route errors to error-topic
            properties={
                **msg.properties,
                'status': 'error',
                'error': str(e),
                'error_type': type(e).__name__
            }
        )
        
        return error_message


def main():
    # Create dataflow
    flow = Dataflow("message-tracking")
    
    # Consumer configuration
    consumer_config = ConsumerConfig(
        consumer_type=PulsarConsumerType.Shared,
        receiver_queue_size=5000,
        consumer_name="tracking-consumer"
    )
    
    # Source
    source = PulsarSource(
        service_url="pulsar://localhost:6650",
        topics=["input-topic"],
        subscription_name="message-tracker",
        consumer_config=consumer_config
    )
    
    # Producer configuration with callback
    producer_config = ProducerConfig(
        compression_type=PulsarCompressionType.LZ4,
        batching_enabled=True,
        batching_max_messages=500,
        max_pending_messages=10000,
        producer_name="tracking-producer",
        message_callback=lambda result, msg_id: metrics.on_message_sent(result, msg_id)
    )
    
    # Create dynamic sink for routing to different topics
    sink = PulsarDynamicSink(
        service_url="pulsar://localhost:6650",
        producer_config=producer_config,
    )
    
    # Build pipeline
    messages = op.input("read", flow, source)
    tracked = op.map("track", messages, process_with_tracking)
    filtered = op.filter("filter-none", tracked, lambda x: x is not None)
    counted = op.map("count-by-topic", filtered, track_message_sent)
    op.output("write", counted, sink)
    
    return flow


# Start metrics reporting thread
def report_metrics():
    print("📊 Starting metrics reporter - updates every 10 second")
    force_print_counter = 0
    while True:
        time.sleep(10)  # Report every 10 seconds
        force_print_counter += 1
        # Force print every 30 seconds (3 intervals)
        force_print = (force_print_counter % 3 == 0)
        metrics.print_dashboard(force_print=force_print)


# Create and start the reporter thread
reporter = threading.Thread(target=report_metrics, daemon=True)
reporter.start()

# Create flow at module level for bytewax.run
flow = main()
