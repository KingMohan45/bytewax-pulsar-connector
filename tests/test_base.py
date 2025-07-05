"""
Base test class for Bytewax-Pulsar connector testing.
Inspired by AmqpProtocolHandlerTestBase patterns from StreamNative AoP project.

"""

import asyncio
import logging
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Union
from unittest.mock import MagicMock, Mock, patch
from contextlib import contextmanager
import pytest
from collections import defaultdict

import pulsar
from bytewax_pulsar_connector.pulsar_manager import PulsarManager
from bytewax_pulsar_connector.models import ConsumerConfig, ProducerConfig


class MockPulsarMessage:
    """Mock Pulsar message for testing."""
    
    def __init__(self, data: bytes, properties: Dict[str, str] = None, message_id: str = None):
        self._data = data
        self._properties = properties or {}
        self._message_id = message_id or f"msg-{int(time.time() * 1000)}"
        self._publish_timestamp = int(time.time() * 1000)
    
    def data(self) -> bytes:
        return self._data
    
    def properties(self) -> Dict[str, str]:
        return self._properties
    
    def message_id(self) -> str:
        return self._message_id
    
    def publish_timestamp(self) -> int:
        return self._publish_timestamp
    
    def topic_name(self) -> str:
        return "test-topic"


class MockPulsarProducer:
    """Mock Pulsar producer for testing."""
    
    def __init__(self, topic: str, producer_name: str = None):
        self.topic = topic
        self.producer_name = producer_name
        self.is_connected = True
        self._closed = False
        self.send_callback = None
    
    def send(self, content: Union[bytes, str], properties: Dict[str, str] = None) -> pulsar.MessageId:
        """Mock send operation."""
        if self._closed:
            raise Exception("Producer is closed")
        if isinstance(content, str):
            content = content.encode('utf-8')
        
        return pulsar.MessageId(partition=0, ledger_id=0, entry_id=0, batch_index=0)
    
    def send_async(self, content: Union[bytes, str], callback=None, properties: Dict[str, str] = None) -> str:
        """Mock async send operation."""
        message_id = self.send(content, properties)
        if callback:
            callback(pulsar.Result.Ok, message_id)
        return message_id
    
    def flush(self):
        """Mock flush operation."""
        pass
    
    def close(self):
        """Mock close operation."""
        self._closed = True
    
    def is_closed(self) -> bool:
        return self._closed


class MockPulsarConsumer:
    """Mock Pulsar consumer for testing."""
    
    def __init__(self, topics: List[str], subscription_name: str, consumer_name: str = None):
        self.topics = topics
        self.subscription_name = subscription_name
        self.consumer_name = consumer_name
        self.message_queue = []
        self.is_connected = True
        self._closed = False
        self.acknowledged_messages = []
        self.rejected_messages = []
    
    def receive(self, timeout_millis: int = 1000) -> MockPulsarMessage:
        """Mock receive operation."""
        if self._closed:
            raise Exception("Consumer is closed")
        
        if not self.message_queue:
            raise Exception("No messages available")
        
        return self.message_queue.pop(0)
    
    def acknowledge(self, message: MockPulsarMessage):
        """Mock acknowledge operation."""
        self.acknowledged_messages.append(message.message_id())
    
    def negative_acknowledge(self, message: MockPulsarMessage):
        """Mock negative acknowledge operation."""
        self.rejected_messages.append(message.message_id())
    
    def close(self):
        """Mock close operation."""
        self._closed = True
    
    def is_closed(self) -> bool:
        return self._closed
    
    def add_message(self, data: bytes, properties: Dict[str, str] = None):
        """Add a message to the consumer queue for testing."""
        message = MockPulsarMessage(data, properties)
        self.message_queue.append(message)
        return message


class MockPulsarClient:
    """Mock Pulsar client for testing."""
    
    def __init__(self, service_url: str):
        self.service_url = service_url
        self.producers = {}
        self.consumers = {}
        self.is_connected = True
        self._closed = False
    
    def create_producer(self, topic: str, producer_name: str = None, **kwargs) -> MockPulsarProducer:
        """Mock producer creation."""
        if self._closed:
            raise Exception("Client is closed")
        
        producer = MockPulsarProducer(topic, producer_name)
        self.producers[f"{topic}:{producer_name or 'default'}"] = producer
        return producer
    
    def subscribe(self, topics: Union[str, List[str]], subscription_name: str, 
                  consumer_name: str = None, **kwargs) -> MockPulsarConsumer:
        """Mock consumer subscription."""
        if self._closed:
            raise Exception("Client is closed")
        
        if isinstance(topics, str):
            topics = [topics]
        
        consumer = MockPulsarConsumer(topics, subscription_name, consumer_name)
        self.consumers[f"{'-'.join(topics)}:{subscription_name}"] = consumer
        return consumer
    
    def close(self):
        """Mock close operation."""
        for producer in self.producers.values():
            producer.close()
        for consumer in self.consumers.values():
            consumer.close()
        self._closed = True


class PulsarTestBase:
    """
    Base test class for Pulsar connector testing.
    Provides mock infrastructure and utilities for testing Pulsar interactions.
    """
    
    def setup_method(self, method):
        """Setup for each test method."""
        self.mock_clients = {}
        self.mock_producers = {}
        self.mock_consumers = {}
        self.test_messages = []
        
        # Clear PulsarManager state
        PulsarManager._clients.clear()
        PulsarManager._producers.clear()
        
        # Setup logging
        logging.basicConfig(level=logging.DEBUG)
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def teardown_method(self, method):
        """Cleanup after each test method."""
        # Close all mocked connections
        for client in self.mock_clients.values():
            client.close()
        
        # Clear PulsarManager state
        PulsarManager._clients.clear()
        PulsarManager._producers.clear()
        
        self.mock_clients.clear()
        self.mock_producers.clear()
        self.mock_consumers.clear()
        self.test_messages.clear()
    
    @contextmanager
    def mock_pulsar_client(self, service_url: str = "pulsar://localhost:6650"):
        """Context manager for mocking Pulsar client."""
        with patch('pulsar.Client') as mock_client_class:
            mock_client = MockPulsarClient(service_url)
            mock_client_class.return_value = mock_client
            self.mock_clients[service_url] = mock_client
            yield mock_client
    
    def create_test_producer(self, service_url: str, topic: str, worker_index: int = 0,
                           producer_config: ProducerConfig = None) -> MockPulsarProducer:
        """Create a test producer using PulsarManager."""
        with self.mock_pulsar_client(service_url):
            producer = PulsarManager.get_create_producer(
                service_url=service_url,
                topic_name=topic,
                worker_index=worker_index,
                producer_config=producer_config
            )
            return producer
    
    def create_test_consumer(self, service_url: str, topics: List[str], 
                           subscription_name: str, worker_index: int = 0,
                           consumer_config: ConsumerConfig = None) -> MockPulsarConsumer:
        """Create a test consumer using PulsarManager."""
        with self.mock_pulsar_client(service_url):
            consumer = PulsarManager.create_consumer(
                service_url=service_url,
                topics=topics,
                subscription_name=subscription_name,
                worker_index=worker_index,
                consumer_config=consumer_config
            )
            return consumer
    
    def send_test_message(
            self,
            producer: MockPulsarProducer, 
            content: str, 
            properties: Dict[str, str] = None
        ) -> str:
        """Send a test message through producer."""
        success_count = 0
        failed_count = 0
        try:
            message_id = producer.send(content.encode('utf-8'), properties)
            success_count += 1
        except pulsar.exceptions.PulsarException:
            failed_count += 1
        return message_id, success_count, failed_count
    
    def send_test_message_async(self, producer: MockPulsarProducer, content: str, properties: Dict[str, str] = None) -> str:
        """Send a test message through producer asynchronously."""
        success_count = 0
        failed_count = 0
        def send_callback(res, msg_id):
            nonlocal success_count, failed_count
            if res == pulsar.Result.Ok:
                success_count += 1
            else:
                failed_count += 1
        message_id = producer.send_async(content.encode('utf-8'), send_callback, properties)
        # wait till callback is called
        while success_count == 0 and failed_count == 0:
            time.sleep(0.1)
        return message_id, success_count, failed_count
    
    def add_test_message_to_consumer(self, consumer: MockPulsarConsumer, 
                                   content: str, properties: Dict[str, str] = None) -> MockPulsarMessage:
        """Add a test message to consumer queue."""
        message = consumer.add_message(content.encode('utf-8'), properties)
        return message
    
    def create_mock_consumer(self, consumer_key: str) -> MockPulsarConsumer:
        """Create a mock consumer with the given key."""
        # Parse the consumer key to extract topics and subscription
        # Format: subscription_name::topic::worker_index
        parts = consumer_key.split("::")
        if len(parts) >= 2:
            subscription_name = parts[0]
            topic = parts[1]
            topics = [topic]
        else:
            # Fallback for different key formats
            subscription_name = "test-subscription"
            topics = ["test-topic"]
        
        consumer = MockPulsarConsumer(topics, subscription_name)
        return consumer
    
    def assert_message_acknowledged(self, consumer: MockPulsarConsumer, message_id: str):
        """Assert that a message was acknowledged."""
        assert message_id in consumer.acknowledged_messages, f"Message {message_id} was not acknowledged"
    
    def assert_producer_count(self, expected_count: int):
        """Assert the number of active producers."""
        total_producers = sum(len(producers) for producers in PulsarManager._producers.values())
        assert total_producers == expected_count, f"Expected {expected_count} producers, got {total_producers}"
    
    def assert_client_count(self, expected_count: int):
        """Assert the number of active clients."""
        assert len(PulsarManager._clients) == expected_count, f"Expected {expected_count} clients, got {len(PulsarManager._clients)}"
    
    def wait_for_condition(self, condition_func, timeout: float = 5.0, interval: float = 0.1):
        """Wait for a condition to be true with timeout."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            if condition_func():
                return True
            time.sleep(interval)
        return False
    
    def simulate_connection_failure(self, client: MockPulsarClient):
        """Simulate connection failure for testing error handling."""
        client.is_connected = False
        for producer in client.producers.values():
            producer.is_connected = False
        for consumer in client.consumers.values():
            consumer.is_connected = False
    
    def simulate_message_processing_delay(self, delay_seconds: float = 0.1):
        """Simulate processing delay for testing async operations."""
        time.sleep(delay_seconds)


class PulsarIntegrationTestBase(PulsarTestBase):
    """
    Base class for integration tests that require actual Pulsar cluster.
    Falls back to mocks if Pulsar is not available.
    """
    
    def setup_method(self, method):
        """Setup for integration tests."""
        super().setup_method(method)
        self.use_real_pulsar = self._check_pulsar_availability()
        
        if not self.use_real_pulsar:
            self.logger.warning("Pulsar not available, using mocks for integration tests")
    
    def _check_pulsar_availability(self) -> bool:
        """Check if real Pulsar cluster is available."""
        try:
            client = pulsar.Client(
                service_url='pulsar://localhost:6650',
                connection_timeout_ms=2000,
                operation_timeout_seconds=5
            )
            
            # Try to create a producer (lightweight operation)
            producer = client.create_producer('health-check-topic')
            
            # If we can create a producer, Pulsar is responsive
            producer.close()
            client.close()
            
            print("Pulsar is healthy")
            return True
            
        except Exception as e:
            print(f"Pulsar health check failed: {e}")
            return False
    
    @contextmanager
    def pulsar_client_or_mock(self, service_url: str = "pulsar://localhost:6650"):
        """Use real Pulsar client if available, otherwise use mock."""
        if self.use_real_pulsar:
            client = pulsar.Client(service_url)
            try:
                yield client
            finally:
                client.close()
        else:
            with self.mock_pulsar_client(service_url) as mock_client:
                yield mock_client
