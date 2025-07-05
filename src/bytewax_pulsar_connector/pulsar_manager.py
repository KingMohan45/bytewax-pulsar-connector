"""
Handles pulsar client connections for producers and consumers for each worker separately

"""

import logging
from collections import defaultdict
from typing import Dict, List

import pulsar
from bytewax_pulsar_connector.models import ConsumerConfig, ProducerConfig

# Use standard logging instead of app-specific logger to avoid dependencies
logger = logging.getLogger(__name__)


class PulsarManager:
    """
    Class to manage the pulsar client and producers/consumers for different topics.
    This class is a singleton and should be accessed using the class methods.
    """

    _clients: Dict[str, pulsar.Client] = {}
    _producers: Dict[int, Dict[str, pulsar.Producer]] = defaultdict(dict)

    @classmethod
    def _get_client(cls, service_url: str) -> pulsar.Client:
        """Get or create a Pulsar client for the given service URL."""
        if service_url not in cls._clients:
            cls._clients[service_url] = pulsar.Client(service_url)
        return cls._clients[service_url]

    @staticmethod
    def _get_name(
        topic_name: str, worker_index: int, app_name: str = "bytewax-pulsar"
    ) -> str:
        """
        Generate a unique name for the consumer or producer based on the topic name and worker index.
        """
        topic_suffix = topic_name.split("/")[-1] if "/" in topic_name else topic_name
        return f"{app_name}::{topic_suffix}::{worker_index}"

    @classmethod
    def create_consumer( # pylint: disable=too-many-arguments, too-many-positional-arguments
        cls,
        service_url: str,
        topics: List[str],
        subscription_name: str,
        worker_index: int = 0,
        consumer_config: ConsumerConfig = None,
    ) -> pulsar.Consumer:
        """
        Returns a consumer for the given topics and worker index.

        :arg service_url: Pulsar service URL
        :arg topics: List of topics to subscribe to
        :arg subscription_name: Subscription name
        :arg worker_index: Worker index for naming
        :arg consumer_config: Additional consumer configuration
        :returns: Pulsar consumer
        """
        consumer_config = consumer_config or ConsumerConfig()
        client = cls._get_client(service_url)

        # Generate consumer name based on first topic
        consumer_name = cls._get_name(topics[0], worker_index, subscription_name)

        return client.subscribe(
            topics,
            subscription_name=subscription_name,
            consumer_name=consumer_name,
            **consumer_config.model_dump(exclude={"consumer_name"}),
        )

    @classmethod
    def get_create_producer(
        cls,
        service_url: str,
        topic_name: str,
        worker_index: int = 0,
        producer_config: ProducerConfig = None,
    ) -> pulsar.Producer:
        """
        Returns a producer for the given topic and worker index.
        The producer is created if it doesn't exist already.

        :arg service_url: Pulsar service URL
        :arg topic_name: Topic name to produce to
        :arg worker_index: Worker index for naming and caching
        :arg producer_config: Additional producer configuration
        :returns: Pulsar producer
        """
        producer_config = producer_config or ProducerConfig()
        client = cls._get_client(service_url)

        producer_name = cls._get_name(topic_name, worker_index)

        if cls._producers[worker_index].get(producer_name) is None:
            cls._producers[worker_index][producer_name] = client.create_producer(
                topic=topic_name,
                producer_name=producer_name,
                **producer_config.model_dump(
                    exclude={"producer_name", "message_callback"}
                ),
            )

        return cls._producers[worker_index][producer_name]

    @classmethod
    def close_worker_producers(cls, worker_index: int):
        """
        Close all producers for a specific worker.

        :arg worker_index: Worker index to close producers for
        """
        if worker_index not in cls._producers:
            return

        for producer in cls._producers[worker_index].values():
            try:
                producer.flush()
                producer.close()
            except Exception as e:  # pylint: disable=broad-exception-caught
                logger.warning("Error closing producer: %s", e)

        del cls._producers[worker_index]
        logger.info(
            "Closed all producers for worker %s. Remaining producers: %s",
            worker_index,
            list(cls._producers.keys()),
        )

    @classmethod
    def close_all(cls):
        """
        Close all clients and producers.
        """
        # Close all producers
        for worker_index in list(cls._producers.keys()):
            cls.close_worker_producers(worker_index)

        # Close all clients
        for client in cls._clients.values():
            try:
                client.close()
            except Exception as e:  # pylint: disable=broad-exception-caught
                logger.warning("Error closing client: %s", e)

        cls._clients.clear()
        logger.info("Closed all Pulsar clients and producers")
