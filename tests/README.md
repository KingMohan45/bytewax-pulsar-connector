# Bytewax-Pulsar Connector Testing Infrastructure

This testing framework is inspired by the `AmqpProtocolHandlerTestBase` patterns from the StreamNative AoP project, adapted specifically for testing Bytewax-Pulsar connectors.

## Overview

The testing infrastructure provides:

- **Comprehensive Mocking**: Full Pulsar client, producer, and consumer mocks
- **Protocol Handler Testing**: Patterns adapted from AoP's AMQP testing framework  
- **Hybrid Testing**: Can run with mocks or real Pulsar clusters
- **Performance Testing**: Simulated load testing capabilities
- **Error Simulation**: Connection failures, backpressure, and fault tolerance testing

## Architecture

### Base Classes

#### `PulsarTestBase`
- Foundation for all unit tests using mocked Pulsar infrastructure
- Provides mock clients, producers, and consumers
- Automatic cleanup and state management
- Utilities for message assertions and verification

#### `PulsarIntegrationTestBase`  
- Extends `PulsarTestBase` for integration tests
- Auto-detects Pulsar availability and falls back to mocks
- Hybrid testing approach: real Pulsar when available, mocks otherwise

### Mock Components

#### `MockPulsarClient`
- Complete Pulsar client simulation
- Manages producers and consumers
- Connection state tracking
- Failure simulation capabilities

#### `MockPulsarProducer`
- Message sending simulation  
- Message tracking and verification
- Async callback support
- Connection state management

#### `MockPulsarConsumer`
- Message receiving simulation
- Queue-based message delivery
- Acknowledgment tracking
- Subscription management

## Usage Examples

### Basic Unit Test

```python
from tests.test_base import PulsarTestBase

class TestMyFeature(PulsarTestBase):
    def test_producer_creation(self):
        with self.mock_pulsar_client("pulsar://localhost:6650"):
            producer = PulsarManager.get_create_producer(
                service_url="pulsar://localhost:6650",
                topic_name="test-topic",
                worker_index=0
            )
            
            message_id = self.send_test_message(producer, "Hello, Pulsar!")
```

## Running Tests

### Unit Tests (Mocked)
```bash
# Run all unit tests with mocked Pulsar
make test-unit

# Run with coverage
make test-coverage
```

### Integration Tests
```bash
# Run integration tests with real Pulsar
make test-with-pulsar

# Run integration tests manually
make pulsar-start
make test-integration
make pulsar-stop
```

This framework provides a solid foundation for testing Pulsar connectors while maintaining the proven patterns from the StreamNative AoP project. 