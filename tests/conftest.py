"""
Pytest configuration and shared fixtures for Bytewax-Pulsar connector tests.
Provides command-line options for selective test execution without environment variables.

"""

import uuid
import pytest
import logging

# Configure logging for tests
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Disable verbose logging from some libraries during tests
logging.getLogger('pulsar').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)


# Shared fixtures
@pytest.fixture(scope="session")
def test_topic():
    """Global test configuration."""
    return f"test-topic-{uuid.uuid4()}"

@pytest.fixture(scope="session")
def test_subscription():
    """Global test configuration."""
    return f"test-subscription-{uuid.uuid4()}"

@pytest.fixture(scope="session")
def pulsar_url():
    """Pulsar URL for testing."""
    return "pulsar://localhost:6650"


@pytest.fixture(autouse=True)
def cleanup_pulsar_manager():
    """Automatically cleanup PulsarManager state after each test."""
    yield
    # Cleanup is handled in test base classes
    pass


@pytest.fixture
def sample_messages():
    """Sample messages for testing."""
    return [
        {"id": 1, "type": "sensor", "value": 23.5, "timestamp": 1640995200},
        {"id": 2, "type": "event", "action": "click", "user": "test_user"},
        {"id": 3, "type": "metric", "name": "cpu_usage", "value": 85.2},
        {"id": 4, "type": "alert", "level": "warning", "message": "High memory usage"},
        {"id": 5, "type": "log", "level": "info", "message": "System started successfully"}
    ]


@pytest.fixture
def test_topics():
    """Standard test topic names."""
    return {
        "input": "test-input-topic",
        "output": "test-output-topic", 
        "alerts": "test-alerts-topic",
        "metrics": "test-metrics-topic",
        "logs": "test-logs-topic"
    }
