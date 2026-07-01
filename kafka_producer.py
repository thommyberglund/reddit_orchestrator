"""
Kafka producer for sending JSON-LD formatted Reddit data.
"""

import json
import logging
from typing import Dict, Any, Optional
from confluent_kafka import Producer, KafkaException
import os
import sys
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add project directory to Python path for imports
project_dir = os.path.dirname(os.path.abspath(__file__))
if project_dir not in sys.path:
    sys.path.insert(0, project_dir)


class KafkaProducer:
    """
    Kafka producer wrapper for sending messages to a topic.
    """
    
    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        topic: Optional[str] = None,
        client_id: Optional[str] = None
    ):
        """
        Initialize Kafka producer.
        
        Args:
            bootstrap_servers: Comma-separated list of Kafka broker addresses (uses env var KAFKA_BOOTSTRAP_SERVERS)
            topic: Kafka topic to produce messages to (uses env var KAFKA_TOPIC)
            client_id: Client identifier (uses env var KAFKA_CLIENT_ID)
        """
        self.bootstrap_servers = bootstrap_servers or os.getenv('KAFKA_BOOTSTRAP_SERVERS', '192.168.1.1:9092')
        self.topic = topic or os.getenv('KAFKA_TOPIC', 'reddit-data')
        self.client_id = client_id or os.getenv('KAFKA_CLIENT_ID', 'reddit-handler-py')
        self.producer = None
        self._initialize_producer()
    
    def _initialize_producer(self) -> None:
        """Initialize the Kafka producer with configuration."""
        config = {
            'bootstrap.servers': self.bootstrap_servers,
            'client.id': self.client_id,
            'message.max.bytes': 10000000,  # 10MB max message size
            'queue.buffering.max.messages': 100000,
            'queue.buffering.max.ms': 200,
            'batch.num.messages': 1000,
            'enable.idempotence': True
        }
        
        # Add SASL authentication if configured
        if os.getenv('KAFKA_USERNAME') and os.getenv('KAFKA_PASSWORD'):
            config['security.protocol'] = os.getenv('KAFKA_SECURITY_PROTOCOL', 'SASL_PLAINTEXT')
            config['sasl.mechanisms'] = os.getenv('KAFKA_SASL_MECHANISM', 'PLAIN')
            config['sasl.username'] = os.getenv('KAFKA_USERNAME')
            config['sasl.password'] = os.getenv('KAFKA_PASSWORD')
        
        try:
            self.producer = Producer(config)
            logging.info(f"Kafka producer initialized for topic: {self.topic}")
        except Exception as e:
            logging.error(f"Failed to initialize Kafka producer: {e}")
            raise
    
    def produce_message(
        self,
        data: Dict[str, Any],
        key: Optional[str] = None
    ) -> bool:
        """
        Produce a message to the Kafka topic.
        
        Args:
            data: Dictionary containing the message data (will be JSON serialized)
            key: Optional key for the message
            
        Returns:
            bool: True if message was successfully produced, False otherwise
        """
        if not self.producer:
            logging.error("Kafka producer not initialized")
            return False
        
        try:
            # Serialize data to JSON string
            message_value = json.dumps(data, ensure_ascii=False).encode('utf-8')
            
            # Use post reddit_id or generate a key
            if key is None and isinstance(data, dict):
                post_id = data.get('post', {}).get('reddit_id')
                if post_id:
                    key = post_id
                else:
                    key = json.dumps(data.get('post', {}).get('title', ''), ensure_ascii=False)
            
            # Ensure key is bytes
            message_key = key.encode('utf-8') if key else None
            
            # Produce message
            self.producer.produce(
                topic=self.topic,
                value=message_value,
                key=message_key
            )
            
            # Wait for any outstanding messages to be delivered
            self.producer.flush()
            
            logging.info(f"Message produced to topic {self.topic}")
            if key:
                logging.info(f"Message key: {key}")
            
            return True
            
        except BufferError as e:
            logging.error(f"Buffer error producing message: {e}")
            return False
        except KafkaException as e:
            logging.error(f"Kafka error producing message: {e}")
            return False
        except Exception as e:
            logging.error(f"Unexpected error producing message: {e}")
            return False
    
    def produce_json_ld(self, json_ld_data: Dict[str, Any]) -> bool:
        """
        Produce JSON-LD formatted data to Kafka topic.
        
        Args:
            json_ld_data: JSON-LD formatted data dictionary
            
        Returns:
            bool: True if message was successfully produced, False otherwise
        """
        # Extract post ID for key if available
        key = None
        if '@graph' in json_ld_data:
            for item in json_ld_data['@graph']:
                if item.get('@type') == 'SocialMediaPosting':
                    key = item.get('identifier') or item.get('@id')
                    break
        
        return self.produce_message(json_ld_data, key)
    
    def close(self) -> None:
        """Close the Kafka producer."""
        if self.producer:
            try:
                self.producer.flush()
                self.producer = None
                logging.info("Kafka producer closed")
            except Exception as e:
                logging.error(f"Error closing Kafka producer: {e}")


# Global Kafka producer instance
_kafka_producer: Optional[KafkaProducer] = None


def get_kafka_producer(
    bootstrap_servers: Optional[str] = None,
    topic: Optional[str] = None
) -> KafkaProducer:
    """
    Get or create the global Kafka producer instance.
    
    Args:
        bootstrap_servers: Kafka bootstrap servers (uses env var if not provided)
        topic: Kafka topic (uses env var if not provided)
        
    Returns:
        KafkaProducer instance
    """
    global _kafka_producer
    
    if _kafka_producer is None:
        servers = bootstrap_servers or os.getenv('KAFKA_BOOTSTRAP_SERVERS', '192.168.1.1:9092')
        topic_name = topic or os.getenv('KAFKA_TOPIC', 'reddit-data')
        _kafka_producer = KafkaProducer(
            bootstrap_servers=servers,
            topic=topic_name
        )
    
    return _kafka_producer


def set_kafka_producer(producer: KafkaProducer) -> None:
    """Set the global Kafka producer instance."""
    global _kafka_producer
    _kafka_producer = producer
