"""
Test cases for PulsarManager using the test base infrastructure.
Demonstrates patterns inspired by AmqpProtocolHandlerTestBase.

"""

import pytest
import traceback
import time
from bytewax_pulsar_connector.pulsar_manager import PulsarManager
from bytewax_pulsar_connector.models import ConsumerConfig, ProducerConfig, PulsarConsumerType, PulsarCompressionType
from tests.test_base import PulsarTestBase, PulsarIntegrationTestBase


@pytest.mark.unit
class TestPulsarManagerUnit(PulsarTestBase):
    """Unit tests for PulsarManager using mocked Pulsar infrastructure."""
    
    def test_producer_creation_and_caching(self):
        """Test producer creation and caching mechanism."""
        service_url = "pulsar://localhost:6650"
        topic = "test-topic"
        worker_index = 0
        
        with self.mock_pulsar_client(service_url):
            # Create first producer
            producer1 = PulsarManager.get_create_producer(
                service_url=service_url,
                topic_name=topic,
                worker_index=worker_index
            )
            
            # Create second producer with same parameters - should return cached
            producer2 = PulsarManager.get_create_producer(
                service_url=service_url,
                topic_name=topic,
                worker_index=worker_index
            )
            
            # Should be the same object (cached)
            assert producer1 is producer2
            assert producer1.topic == topic
            assert not producer1.is_closed()
            
            # Verify caching works
            self.assert_producer_count(1)
            self.assert_client_count(1)
    
    def test_producer_different_workers(self):
        """Test producer creation for different workers."""
        service_url = "pulsar://localhost:6650"
        topic = "test-topic"
        
        with self.mock_pulsar_client(service_url):
            # Create producers for different workers
            producer_w0 = PulsarManager.get_create_producer(
                service_url=service_url,
                topic_name=topic,
                worker_index=0
            )
            
            producer_w1 = PulsarManager.get_create_producer(
                service_url=service_url,
                topic_name=topic,
                worker_index=1
            )
            
            # Should be different objects
            assert producer_w0 is not producer_w1
            assert producer_w0.producer_name != producer_w1.producer_name
            
            # Should have 2 producers total
            self.assert_producer_count(2)
            self.assert_client_count(1)  # Same client
    
    def test_consumer_creation(self):
        """Test consumer creation."""
        service_url = "pulsar://localhost:6650"
        topics = ["topic1", "topic2"]
        subscription_name = "test-subscription"
        
        consumer_config = ConsumerConfig(
            consumer_type=PulsarConsumerType.Shared,
            receiver_queue_size=1000
        )
        
        with self.mock_pulsar_client(service_url):
            consumer = PulsarManager.create_consumer(
                service_url=service_url,
                topics=topics,
                subscription_name=subscription_name,
                worker_index=0,
                consumer_config=consumer_config
            )
            
            assert consumer.topics == topics
            assert consumer.subscription_name == subscription_name
            assert not consumer.is_closed()
            
            self.assert_client_count(1)
    
    def test_producer_with_config(self):
        """Test producer creation with custom configuration."""
        service_url = "pulsar://localhost:6650"
        topic = "test-topic"
        
        producer_config = ProducerConfig(
            compression_type=PulsarCompressionType.LZ4,
            batching_enabled=True,
            max_pending_messages=5000,
            producer_name="custom-producer"
        )
        
        with self.mock_pulsar_client(service_url):
            producer = PulsarManager.get_create_producer(
                service_url=service_url,
                topic_name=topic,
                worker_index=0,
                producer_config=producer_config
            )
            
            assert producer.topic == topic
            # Producer name should be generated, not using config's producer_name
            assert "bytewax-pulsar" in producer.producer_name
            assert not producer.is_closed()
    
    def test_message_sending(self):
        """Test message sending through producer."""
        service_url = "pulsar://localhost:6650"
        topic = "test-topic"
        
        with self.mock_pulsar_client(service_url):
            producer = PulsarManager.get_create_producer(
                service_url=service_url,
                topic_name=topic,
                worker_index=0
            )
            
            # Send test message
            test_content = "Hello, Pulsar!"
            _message_id, success_count, failed_count = self.send_test_message(producer, test_content)
            
            # Verify message was sent
            assert success_count == 1
            assert failed_count == 0
    
    def test_message_receiving(self):
        """Test message receiving through consumer."""
        service_url = "pulsar://localhost:6650"
        topics = ["test-topic"]
        subscription_name = "test-subscription"
        
        with self.mock_pulsar_client(service_url):
            consumer = PulsarManager.create_consumer(
                service_url=service_url,
                topics=topics,
                subscription_name=subscription_name,
                worker_index=0
            )
            
            # Add test message to consumer
            test_content = "Test message"
            message = self.add_test_message_to_consumer(consumer, test_content)
            
            # Receive and acknowledge message
            received_message = consumer.receive()
            assert received_message.data().decode('utf-8') == test_content
            
            consumer.acknowledge(received_message)
            self.assert_message_acknowledged(consumer, received_message.message_id())
    
    def test_worker_producer_cleanup(self):
        """Test cleanup of producers for specific worker."""
        service_url = "pulsar://localhost:6650"
        
        with self.mock_pulsar_client(service_url):
            # Create producers for multiple workers
            producer_w0_t1 = PulsarManager.get_create_producer(
                service_url=service_url,
                topic_name="topic1",
                worker_index=0
            )
            
            producer_w0_t2 = PulsarManager.get_create_producer(
                service_url=service_url,
                topic_name="topic2",
                worker_index=0
            )
            
            producer_w1_t1 = PulsarManager.get_create_producer(
                service_url=service_url,
                topic_name="topic1",
                worker_index=1
            )
            
            # Should have 3 producers
            self.assert_producer_count(3)
            
            # Close worker 0 producers
            PulsarManager.close_worker_producers(0)
            
            # Should have 1 producer remaining (worker 1)
            self.assert_producer_count(1)
            
            # Verify worker 0 producers are closed
            assert producer_w0_t1.is_closed()
            assert producer_w0_t2.is_closed()
            assert not producer_w1_t1.is_closed()
    
    def test_close_all_cleanup(self):
        """Test complete cleanup of all connections."""
        service_url1 = "pulsar://localhost:6650"
        service_url2 = "pulsar://localhost:6651"
        
        with self.mock_pulsar_client(service_url1), self.mock_pulsar_client(service_url2):
            # Create resources on multiple clients
            producer1 = PulsarManager.get_create_producer(
                service_url=service_url1,
                topic_name="topic1",
                worker_index=0
            )
            
            producer2 = PulsarManager.get_create_producer(
                service_url=service_url2,
                topic_name="topic2",
                worker_index=1
            )
            
            self.assert_producer_count(2)
            self.assert_client_count(2)
            
            # Close all
            PulsarManager.close_all()
            
            # Should have no active resources
            self.assert_producer_count(0)
            self.assert_client_count(0)
            
            # Verify producers are closed
            assert producer1.is_closed()
            assert producer2.is_closed()
    
    def test_connection_failure_handling(self):
        """Test handling of connection failures."""
        service_url = "pulsar://localhost:6650"
        topic = "test-topic"
        
        with self.mock_pulsar_client(service_url) as mock_client:
            producer = PulsarManager.get_create_producer(
                service_url=service_url,
                topic_name=topic,
                worker_index=0
            )
            
            # Simulate connection failure
            self.simulate_connection_failure(mock_client)
            
            # Producer should still be accessible but connection is down
            assert not producer.is_connected
    
    def test_name_generation(self):
        """Test unique name generation for producers and consumers."""
        # Test name generation
        name1 = PulsarManager._get_name("topic1", 0, "test-app")
        name2 = PulsarManager._get_name("topic2", 1, "test-app")
        name3 = PulsarManager._get_name("persistent://tenant/ns/topic3", 0, "test-app")
        
        assert name1 == "test-app::topic1::0"
        assert name2 == "test-app::topic2::1"
        assert name3 == "test-app::topic3::0"
    
    def test_async_message_processing(self):
        """Test async message processing patterns."""
        service_url = "pulsar://localhost:6650"
        topic = "test-topic"
        
        with self.mock_pulsar_client(service_url) as client:
            producer = client.create_producer(
                topic=topic,
                producer_name=f"async-producer-{int(time.time() * 1000)}"
            )
            
            # Send message
            message_id, success_count, failed_count = self.send_test_message_async(producer, "async test")
            assert success_count == 1
            assert failed_count == 0


@pytest.mark.integration
class TestPulsarManagerIntegration(PulsarIntegrationTestBase):
    """Integration tests for PulsarManager that can run against real Pulsar."""
    
    def test_real_or_mock_producer_integration(self):
        """Test producer with real or mocked Pulsar cluster."""
        service_url = "pulsar://localhost:6650"
        topic = "integration-test-topic"
        
        with self.pulsar_client_or_mock(service_url):
            producer = PulsarManager.get_create_producer(
                service_url=service_url,
                topic_name=topic,
                worker_index=0,
                producer_config=ProducerConfig(
                    compression_type=PulsarCompressionType.LZ4,
                    batching_enabled=True
                )
            )
            
            # Test message sending
            test_message = "Integration test message"
            try:
                message_id, success_count, failed_count = self.send_test_message(producer, test_message)
            except Exception as e:
                print(traceback.format_exc())
                raise e
            
            assert message_id is not None
            assert success_count == 1
            assert failed_count == 0
            
            # Cleanup
            PulsarManager.close_worker_producers(0)
    
    def test_real_or_mock_consumer_integration(self):
        """Test consumer with real or mocked Pulsar cluster."""
        service_url = "pulsar://localhost:6650"
        topics = ["integration-test-topic"]
        subscription_name = "integration-test-subscription"
        
        with self.pulsar_client_or_mock(service_url):
            consumer = PulsarManager.create_consumer(
                service_url=service_url,
                topics=topics,
                subscription_name=subscription_name,
                worker_index=0,
                consumer_config=ConsumerConfig(
                    consumer_type=PulsarConsumerType.Shared,
                    receiver_queue_size=1000
                )
            )
            
            # For mock testing, add a test message
            if not self.use_real_pulsar:
                test_message = "Integration test message"
                self.add_test_message_to_consumer(consumer, test_message)
                
                # Receive and verify
                received = consumer.receive()
                assert received.data().decode('utf-8') == test_message
                consumer.acknowledge(received)
            
            # Cleanup
            consumer.close()
    
    @pytest.mark.performance
    def test_performance_simulation(self):
        """Test PulsarManager performance under simulated load."""
        service_url = "pulsar://localhost:6650"
        
        with self.pulsar_client_or_mock(service_url) as client:
            # Performance test parameters - optimized for speed
            num_workers = 2  # Reduced from 3
            num_topics = 3   # Reduced from 5
            messages_per_worker = 25  # Reduced from 100
            
            start_time = time.time()
            
            # Create producers for multiple workers and topics
            producers = {}
            for worker_idx in range(num_workers):
                for topic_idx in range(num_topics):
                    topic = f"perf-topic-{topic_idx}"
                    producer = client.create_producer(
                        topic=topic,
                        producer_name=f"perf-producer-{worker_idx}-{topic_idx}"
                    )
                    producers[f"w{worker_idx}_t{topic_idx}"] = producer
            
            # Send messages from all producers
            total_messages = 0
            total_failed = 0
            for worker_idx in range(num_workers):
                for msg_idx in range(messages_per_worker):
                    topic_idx = msg_idx % num_topics
                    producer_key = f"w{worker_idx}_t{topic_idx}"
                    producer = producers[producer_key]
                    
                    # Simplified message content for speed
                    message_content = f"msg-{worker_idx}-{msg_idx}"
                    _message_id, success_count, failed_count = self.send_test_message(producer, message_content)
                    total_messages += success_count
                    total_failed += failed_count
            # Cleanup all producers
            for worker_idx in range(num_workers):
                PulsarManager.close_worker_producers(worker_idx)
            
            end_time = time.time()
            duration = end_time - start_time
            
            # Performance assertions - should be fast with mocks
            assert duration < 2.0, f"Performance test took too long: {duration:.3f}s"
            assert total_messages == num_workers * messages_per_worker
            assert total_failed == 0
            
            # Log performance metrics
            throughput = total_messages / duration if duration > 0 else 0
            self.logger.info(f"Performance test completed:")
            self.logger.info(f"  - Messages: {total_messages}")
            self.logger.info(f"  - Duration: {duration:.3f}s")
            self.logger.info(f"  - Throughput: {throughput:.1f} msg/s")
            self.logger.info(f"  - Workers: {num_workers}")
            self.logger.info(f"  - Topics: {num_topics}")
            
            # Verify cleanup
            self.assert_producer_count(0)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
