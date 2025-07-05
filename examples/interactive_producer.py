#!/usr/bin/env python3
"""
Interactive Pulsar Producer for Testing
======================================

This script provides an interactive way to generate test messages to a Pulsar topic
using the PulsarManager from the bytewax-pulsar connector.

Usage:
    python examples/interactive_producer.py
"""

import json
import sys
import time
from datetime import datetime
from typing import Optional

# Add parent directory to path to import local modules
sys.path.insert(0, '.')

from bytewax_pulsar_connector.pulsar_manager import PulsarManager


def get_user_input(prompt: str, default: Optional[str] = None) -> str:
    """Get user input with optional default value."""
    if default:
        prompt = f"{prompt} [{default}]"
    user_input = input(f"{prompt}: ").strip()
    return user_input if user_input else (default or "")


def generate_message(index: int, message_type: str) -> dict:
    """Generate a test message based on type."""
    timestamp = int(time.time() * 1000)
    
    if message_type == "sensor":
        return {
            "id": f"sensor_{index}",
            "type": ["heavy", "standard"][index % 2],
            "timestamp": timestamp,
            "temperature": 20 + (index % 10),
            "humidity": 40 + (index % 20),
            "location": f"zone_{index % 5}",
            "status": "active" if index % 3 != 0 else "maintenance"
        }
    elif message_type == "event":
        return {
            "event_id": f"evt_{index:06d}",
            "timestamp": timestamp,
            "type": ["login", "logout", "update", "delete"][index % 4],
            "user_id": f"user_{index % 100}",
            "metadata": {
                "source": "interactive_producer",
                "session": f"session_{index % 10}"
            }
        }
    elif message_type == "transaction":
        # Calculate item details first
        item_price = round(1.0 + (index % 100) * 0.1, 2)
        item_quantity = index % 10 + 1
        calculated_total = round(item_price * item_quantity, 2)
        
        return {
            "transaction_id": f"txn_{index:08d}",
            "timestamp": timestamp,
            "amount": calculated_total,
            "items": [
                {
                    "sku": f"item_{index:06d}",
                    "price": item_price,
                    "quantity": item_quantity
                }
            ],
            "order_id": f"order_{index:06d}",
            "customer_id": f"customer_{index % 100}",
            "total": calculated_total,
            "currency": ["USD", "EUR", "GBP"][index % 3],
            "merchant": f"merchant_{index % 50}",
            "status": "completed" if index % 10 != 0 else "pending"
        }
    else:  # simple
        return {
            "message_id": index,
            "timestamp": timestamp,
            "data": f"Test message {index}",
            "sequence": index
        }


def main():
    """Main interactive producer function."""
    print("=" * 60)
    print("Interactive Pulsar Producer for Testing")
    print("=" * 60)
    print()
    
    # Get configuration from user
    pulsar_url = get_user_input("Pulsar URL", "pulsar://localhost:6650")
    topic = get_user_input("Topic name", "input-topic")
    
    # Message configuration
    print("\nMessage Types:")
    print("1. simple   - Basic test messages")
    print("2. sensor   - IoT sensor data")
    print("3. event    - User events")
    print("4. transaction - Financial transactions")
    message_type = get_user_input("Message type (1-4)", "1")
    
    message_types = {
        "1": "simple",
        "2": "sensor", 
        "3": "event",
        "4": "transaction"
    }
    message_type = message_types.get(message_type, "simple")
    
    # Number of messages
    num_messages = get_user_input("Number of messages to send", "10")
    try:
        num_messages = int(num_messages)
    except ValueError:
        print("Invalid number, using default: 10")
        num_messages = 10
    
    # Delay between messages
    delay = get_user_input("Delay between messages (seconds)", "1")
    try:
        delay = float(delay)
    except ValueError:
        print("Invalid delay, using default: 1 second")
        delay = 1.0
    
    # Confirm settings
    print("\n" + "=" * 60)
    print("Configuration:")
    print(f"  Pulsar URL: {pulsar_url}")
    print(f"  Topic: {topic}")
    print(f"  Message Type: {message_type}")
    print(f"  Number of Messages: {num_messages}")
    print(f"  Delay: {delay}s")
    print("=" * 60)
    
    confirm = get_user_input("\nProceed? (y/n)", "y")
    if confirm.lower() != 'y':
        print("Cancelled.")
        return
    
    # Initialize PulsarManager
    print("\nConnecting to Pulsar...")
    try:
        manager = PulsarManager.create_consumer(
            service_url=pulsar_url,
            topics=[topic],
            subscription_name="interactive-producer",
            worker_index=0,
        )
        
        # Create producer
        producer = PulsarManager.get_create_producer(
            service_url=pulsar_url,
            topic_name=topic,
            worker_index=0,
        )
        print(f"✓ Connected to Pulsar at {pulsar_url}")
        print(f"✓ Producer created for topic: {topic}")
        
    except Exception as e:
        print(f"✗ Failed to connect to Pulsar: {e}")
        print("\nMake sure Pulsar is running locally:")
        print("  docker run -it -p 6650:6650 -p 8080:8080 apachepulsar/pulsar:latest bin/pulsar standalone")
        return
    
    # Send messages
    print(f"\nSending {num_messages} messages...")
    print("-" * 60)
    
    try:
        for i in range(num_messages):
            # Generate message
            message = generate_message(i, message_type)
            
            # Send message
            message_bytes = json.dumps(message).encode('utf-8')
            producer.send(message_bytes)
            
            # Display progress
            print(f"[{i+1}/{num_messages}] Sent: {json.dumps(message, indent=2)}")
            
            # Delay between messages (except for the last one)
            if i < num_messages - 1 and delay > 0:
                time.sleep(delay)
        
        print("-" * 60)
        print(f"✓ Successfully sent {num_messages} messages to '{topic}'")
        
    except KeyboardInterrupt:
        print("\n\n✗ Interrupted by user")
    except Exception as e:
        print(f"\n✗ Error sending messages: {e}")
    finally:
        # Cleanup
        PulsarManager.close_all()
        print("\n✓ Cleaned up connections")


if __name__ == "__main__":
    main() 