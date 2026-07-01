"""
Kafka consumer for listening to JSON-LD formatted Reddit data.
Writes comments and posts to RustFS S3-compatible storage.
"""

import json
import logging
from typing import Dict, Any, Optional
from confluent_kafka import Consumer, KafkaException, KafkaError
import os
import sys
import threading
import time
from datetime import datetime, UTC
import io
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError

# Load environment variables from .env file
load_dotenv()

# Add project directory to Python path for imports
project_dir = os.path.dirname(os.path.abspath(__file__))
if project_dir not in sys.path:
    sys.path.insert(0, project_dir)


class RustFSStorage:
    """
    RustFS S3-compatible storage handler for writing messages to buckets.
    """
    
    def __init__(
        self,
        endpoint: str = "192.168.1.1",
        port: int = 9000,
        access_key: str = "rustfsadmin",
        secret_key: str = "rustfsadmin",
        secure: bool = False
    ):
        """
        Initialize RustFS S3-compatible client.
        
        Args:
            endpoint: RustFS server hostname/IP
            port: RustFS server port (default: 9000)
            access_key: RustFS access key (default: rustfsadmin)
            secret_key: RustFS secret key (default: rustfsadmin)
            secure: Use HTTPS (default: False for HTTP)
        """
        self.endpoint = endpoint
        self.port = port
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = secure
        
        # Construct full endpoint URL
        full_endpoint = f"http{'s' if secure else ''}://{endpoint}:{port}"
        
        # Initialize S3 client with RustFS endpoint
        self.client = boto3.client(
            's3',
            endpoint_url=full_endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            verify=False if not secure else True
        )
        
        logging.info(f"RustFS client initialized for endpoint: {full_endpoint}")
    
    def ensure_bucket_exists(self, bucket_name: str) -> bool:
        """
        Check if a bucket exists, create if it doesn't.
        
        Args:
            bucket_name: Name of the bucket
            
        Returns:
            bool: True if bucket exists or was created, False otherwise
        """
        try:
            # Check if bucket exists
            self.client.head_bucket(Bucket=bucket_name)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                # Bucket doesn't exist, create it
                try:
                    self.client.create_bucket(Bucket=bucket_name)
                    logging.info(f"Created bucket: {bucket_name}")
                    return True
                except ClientError as e2:
                    logging.error(f"Error creating bucket {bucket_name}: {e2}")
                    return False
            else:
                logging.error(f"Error checking bucket {bucket_name}: {e}")
                return False
    
    def write_json_to_bucket(self, bucket_name: str, key: str, data: Dict[str, Any]) -> bool:
        """
        Write JSON data to a RustFS bucket.
        
        Args:
            bucket_name: Name of the bucket
            key: Object key/path in the bucket
            data: JSON-serializable data to write
            
        Returns:
            bool: True if write was successful, False otherwise
        """
        try:
            # Ensure bucket exists
            self.ensure_bucket_exists(bucket_name)
            
            # Serialize data to JSON string
            json_str = json.dumps(data, indent=2, ensure_ascii=False)
            json_bytes = json_str.encode('utf-8')
            
            # Write to RustFS
            self.client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=json_bytes,
                ContentType='application/json'
            )
            
            logging.info(f"Wrote object to {bucket_name}/{key}")
            return True
            
        except ClientError as e:
            logging.error(f"Error writing to {bucket_name}/{key}: {e}")
            return False
        except Exception as e:
            logging.error(f"Unexpected error writing to RustFS: {e}")
            return False


class KafkaConsumer:
    """
    Kafka consumer wrapper for receiving messages from a topic.
    """
    
    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        topic: Optional[str] = None,
        group_id: Optional[str] = None,
        auto_offset_reset: Optional[str] = None,
        rustfs_endpoint: Optional[str] = None,
        rustfs_port: Optional[int] = None,
        rustfs_access_key: Optional[str] = None,
        rustfs_secret_key: Optional[str] = None,
        rustfs_secure: Optional[bool] = None,
        posts_bucket: Optional[str] = None,
        comments_bucket: Optional[str] = None
    ):
        """
        Initialize Kafka consumer.
        
        Args:
            bootstrap_servers: Comma-separated list of Kafka broker addresses (uses env var KAFKA_BOOTSTRAP_SERVERS)
            topic: Kafka topic to consume messages from (uses env var KAFKA_TOPIC)
            group_id: Consumer group ID (uses env var KAFKA_CONSUMER_GROUP)
            auto_offset_reset: What to do when there is no initial offset ('earliest' or 'latest') (uses env var KAFKA_AUTO_OFFSET_RESET)
            rustfs_endpoint: RustFS server hostname/IP (uses env var RUSTFS_ENDPOINT)
            rustfs_port: RustFS server port (default: 9000) (uses env var RUSTFS_PORT)
            rustfs_access_key: RustFS access key (uses env var RUSTFS_ACCESS_KEY)
            rustfs_secret_key: RustFS secret key (uses env var RUSTFS_SECRET_KEY)
            rustfs_secure: Use HTTPS for RustFS (uses env var RUSTFS_SECURE)
            posts_bucket: Bucket name for posts (uses env var RUSTFS_POSTS_BUCKET)
            comments_bucket: Bucket name for comments (uses env var RUSTFS_COMMENTS_BUCKET)
        """
        self.bootstrap_servers = bootstrap_servers or os.getenv('KAFKA_BOOTSTRAP_SERVERS', '192.168.1.1:9092')
        self.topic = topic or os.getenv('KAFKA_TOPIC', 'reddit-data')
        self.group_id = group_id or os.getenv('KAFKA_CONSUMER_GROUP', 'reddit-handler-py-consumer')
        self.auto_offset_reset = auto_offset_reset or os.getenv('KAFKA_AUTO_OFFSET_RESET', 'earliest')
        self.consumer = None
        self._running = False
        
        # RustFS configuration
        self.rustfs_endpoint = rustfs_endpoint or os.getenv('RUSTFS_ENDPOINT', '192.168.1.1')
        self.rustfs_port = rustfs_port or int(os.getenv('RUSTFS_PORT', '9000'))
        self.rustfs_access_key = rustfs_access_key or os.getenv('RUSTFS_ACCESS_KEY', 'rustfsadmin')
        self.rustfs_secret_key = rustfs_secret_key or os.getenv('RUSTFS_SECRET_KEY', 'rustfsadmin')
        self.rustfs_secure = rustfs_secure if rustfs_secure is not None else (os.getenv('RUSTFS_SECURE', 'false').lower() == 'true')
        self.posts_bucket = posts_bucket or os.getenv('RUSTFS_POSTS_BUCKET', 'reddit-posts')
        self.comments_bucket = comments_bucket or os.getenv('RUSTFS_COMMENTS_BUCKET', 'reddit-comments')
        self.rustfs_client = None
        
        self._initialize_consumer()
        self._initialize_rustfs()
    
    def _initialize_consumer(self) -> None:
        """Initialize the Kafka consumer with configuration."""
        config = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': self.auto_offset_reset,
            'enable.auto.commit': False,
            'session.timeout.ms': 6000,
            'default.topic.config': {
                'auto.offset.reset': self.auto_offset_reset
            }
        }
        
        # Add SASL authentication if configured
        if os.getenv('KAFKA_USERNAME') and os.getenv('KAFKA_PASSWORD'):
            config['security.protocol'] = os.getenv('KAFKA_SECURITY_PROTOCOL', 'SASL_PLAINTEXT')
            config['sasl.mechanisms'] = os.getenv('KAFKA_SASL_MECHANISM', 'PLAIN')
            config['sasl.username'] = os.getenv('KAFKA_USERNAME')
            config['sasl.password'] = os.getenv('KAFKA_PASSWORD')
        
        try:
            self.consumer = Consumer(config)
            logging.info(f"Kafka consumer initialized for topic: {self.topic}, group: {self.group_id}")
        except Exception as e:
            logging.error(f"Failed to initialize Kafka consumer: {e}")
            raise
    
    def _initialize_rustfs(self) -> None:
        """Initialize the RustFS S3-compatible client."""
        try:
            self.rustfs_client = RustFSStorage(
                endpoint=self.rustfs_endpoint,
                port=self.rustfs_port,
                access_key=self.rustfs_access_key,
                secret_key=self.rustfs_secret_key,
                secure=self.rustfs_secure
            )
            logging.info(f"RustFS client initialized for endpoint: {self.rustfs_endpoint}:{self.rustfs_port}")
            
            # Ensure buckets exist
            self.rustfs_client.ensure_bucket_exists(self.posts_bucket)
            self.rustfs_client.ensure_bucket_exists(self.comments_bucket)
            logging.info(f"RustFS buckets verified: {self.posts_bucket}, {self.comments_bucket}")
            
        except Exception as e:
            logging.error(f"Failed to initialize RustFS client: {e}")
            # Continue without RustFS - will log errors when trying to write
            self.rustfs_client = None
    
    def consume_messages(self, callback=None, timeout: float = 1.0) -> None:
        """
        Start consuming messages from the Kafka topic.
        
        Args:
            callback: Optional callback function to process each message
            timeout: Poll timeout in seconds
        """
        if not self.consumer:
            logging.error("Kafka consumer not initialized")
            return
        
        self._running = True
        self.consumer.subscribe([self.topic])
        
        logging.info(f"Started consuming messages from topic: {self.topic}")
        
        try:
            while self._running:
                msg = self.consumer.poll(timeout=timeout)
                
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        continue
                    else:
                        logging.error(f"Consumer error: {msg.error()}")
                        continue
                
                # Process the message
                self._process_message(msg, callback)
                
                # Manually commit the message offset
                self.consumer.commit(msg)
                
        except KeyboardInterrupt:
            logging.info("Consumer interrupted by user")
        except Exception as e:
            logging.error(f"Error in consumer: {e}")
        finally:
            self.close()
    
    def _process_message(self, msg, callback=None) -> None:
        """
        Process a single Kafka message and write to RustFS.
        
        Args:
            msg: Kafka message object
            callback: Optional callback function to process the message
        """
        try:
            # Decode the message value
            message_value = msg.value().decode('utf-8')
            data = json.loads(message_value)
            
            # Display the JSON-LD data in a pretty-printed format
            json_ld_str = json.dumps(data, indent=2, ensure_ascii=False)
            print("\n" + "=" * 60)
            print("Received Kafka message:")
            print("=" * 60)
            print(f"Topic: {msg.topic()}")
            print(f"Partition: {msg.partition()}")
            print(f"Offset: {msg.offset()}")
            print(f"Key: {msg.key().decode('utf-8') if msg.key() else 'None'}")
            print("\nJSON-LD Content:")
            print(json_ld_str)
            print("=" * 60 + "\n")
            
            # Write each post and comment to RustFS
            self._write_to_rustfs(data)
            
            # Call the callback if provided
            if callback:
                callback(msg.topic(), msg.partition(), msg.offset(), data)
                
        except json.JSONDecodeError as e:
            logging.error(f"Failed to decode message as JSON: {e}")
            print(f"Raw message: {msg.value()}")
        except Exception as e:
            logging.error(f"Error processing message: {e}")
    
    def _write_to_rustfs(self, json_ld_data: Dict[str, Any]) -> None:
        """
        Write JSON-LD data to RustFS buckets.
        Each post is written to reddit-posts bucket.
        Each comment is written to reddit-comments bucket.
        
        Handles both formats:
        - Direct items in @graph
        - Nested items in DataDownload.hasPart
        
        Args:
            json_ld_data: JSON-LD formatted data from Kafka message
        """
        if self.rustfs_client is None:
            logging.warning("RustFS client not initialized, skipping write to RustFS")
            return
        
        try:
            # Generate a timestamp for the object key
            timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S_%f")
            
            # Extract posts and comments from @graph
            if '@graph' not in json_ld_data:
                logging.warning("No @graph in JSON-LD data, skipping RustFS write")
                return
            
            for item in json_ld_data['@graph']:
                item_type = item.get('@type', '')
                item_id = item.get('@id', 'unknown')
                
                # Check for nested items in hasPart (DataDownload structure)
                if item_type == 'DataDownload' and 'hasPart' in item:
                    for part_item in item['hasPart']:
                        part_type = part_item.get('@type', '')
                        part_id = part_item.get('@id', 'unknown')
                        
                        # Write SocialMediaPosting to reddit-posts bucket
                        if part_type == 'SocialMediaPosting':
                            post_key = f"posts/{timestamp}_{part_id.replace('/', '_')}.json"
                            success = self._write_item_to_rustfs(
                                self.posts_bucket, 
                                post_key, 
                                part_item
                            )
                            if success:
                                print(f"  -> Written post to RustFS: {self.posts_bucket}/{post_key}")
                        
                        # Write Comment to reddit-comments bucket
                        elif part_type == 'Comment':
                            comment_key = f"comments/{timestamp}_{part_id.replace('/', '_')}.json"
                            success = self._write_item_to_rustfs(
                                self.comments_bucket,
                                comment_key,
                                part_item
                            )
                            if success:
                                print(f"  -> Written comment to RustFS: {self.comments_bucket}/{comment_key}")
                
                # Also check top-level items (fallback for other formats)
                elif item_type == 'SocialMediaPosting':
                    post_key = f"posts/{timestamp}_{item_id.replace('/', '_')}.json"
                    success = self._write_item_to_rustfs(
                        self.posts_bucket, 
                        post_key, 
                        item
                    )
                    if success:
                        print(f"  -> Written post to RustFS: {self.posts_bucket}/{post_key}")
                
                elif item_type == 'Comment':
                    comment_key = f"comments/{timestamp}_{item_id.replace('/', '_')}.json"
                    success = self._write_item_to_rustfs(
                        self.comments_bucket,
                        comment_key,
                        item
                    )
                    if success:
                        print(f"  -> Written comment to RustFS: {self.comments_bucket}/{comment_key}")
        
        except Exception as e:
            logging.error(f"Error writing to RustFS: {e}")
    
    def _write_item_to_rustfs(self, bucket: str, key: str, data: Dict[str, Any]) -> bool:
        """
        Write a single item to RustFS bucket.
        
        Args:
            bucket: Bucket name
            key: Object key
            data: Data to write
            
        Returns:
            bool: True if write succeeded
        """
        if self.rustfs_client is None:
            return False
        
        try:
            return self.rustfs_client.write_json_to_bucket(bucket, key, data)
            
        except Exception as e:
            logging.error(f"Error writing {bucket}/{key}: {e}")
            return False
    
    def consume_single_message(self, timeout: float = 5.0) -> Optional[Dict[str, Any]]:
        """
        Consume a single message from the Kafka topic.
        
        Args:
            timeout: Maximum time to wait for a message in seconds
            
        Returns:
            Dictionary containing the message data, or None if no message received
        """
        if not self.consumer:
            logging.error("Kafka consumer not initialized")
            return None
        
        self.consumer.subscribe([self.topic])
        
        start_time = time.time()
        while time.time() - start_time < timeout:
            msg = self.consumer.poll(timeout=min(1.0, timeout - (time.time() - start_time)))
            
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logging.error(f"Consumer error: {msg.error()}")
                    continue
            
            try:
                message_value = msg.value().decode('utf-8')
                data = json.loads(message_value)
                
                # Display the JSON-LD data
                json_ld_str = json.dumps(data, indent=2, ensure_ascii=False)
                print("\n" + "=" * 60)
                print("Received Kafka message:")
                print("=" * 60)
                print(f"Topic: {msg.topic()}")
                print(f"Partition: {msg.partition()}")
                print(f"Offset: {msg.offset()}")
                print(f"Key: {msg.key().decode('utf-8') if msg.key() else 'None'}")
                print("\nJSON-LD Content:")
                print(json_ld_str)
                print("=" * 60 + "\n")
                
                self.consumer.commit(msg)
                return data
                
            except json.JSONDecodeError as e:
                logging.error(f"Failed to decode message as JSON: {e}")
                print(f"Raw message: {msg.value()}")
            except Exception as e:
                logging.error(f"Error processing message: {e}")
        
        return None
    
    def stop(self) -> None:
        """Stop the consumer loop."""
        self._running = False
    
    def close(self) -> None:
        """Close the Kafka consumer."""
        if self.consumer:
            try:
                self.consumer.close()
                self.consumer = None
                logging.info("Kafka consumer closed")
            except Exception as e:
                logging.error(f"Error closing Kafka consumer: {e}")
        
        # RustFS client doesn't need explicit close, just set to None
        self.rustfs_client = None


def start_consumer(
    bootstrap_servers: Optional[str] = None,
    topic: Optional[str] = None,
    group_id: Optional[str] = None,
    auto_offset_reset: Optional[str] = None,
    rustfs_endpoint: Optional[str] = None,
    rustfs_port: Optional[int] = None,
    rustfs_access_key: Optional[str] = None,
    rustfs_secret_key: Optional[str] = None,
    rustfs_secure: Optional[bool] = None,
    posts_bucket: Optional[str] = None,
    comments_bucket: Optional[str] = None
) -> KafkaConsumer:
    """
    Create and start a Kafka consumer.
    
    All configuration is read from environment variables if not provided as arguments.
    
    Args:
        bootstrap_servers: Kafka bootstrap servers (uses env var KAFKA_BOOTSTRAP_SERVERS)
        topic: Kafka topic (uses env var KAFKA_TOPIC)
        group_id: Consumer group ID (uses env var KAFKA_CONSUMER_GROUP)
        auto_offset_reset: Offset reset policy ('earliest' or 'latest', uses env var KAFKA_AUTO_OFFSET_RESET)
        rustfs_endpoint: RustFS endpoint (uses env var RUSTFS_ENDPOINT)
        rustfs_port: RustFS port (uses env var RUSTFS_PORT)
        rustfs_access_key: RustFS access key (uses env var RUSTFS_ACCESS_KEY)
        rustfs_secret_key: RustFS secret key (uses env var RUSTFS_SECRET_KEY)
        rustfs_secure: Use HTTPS for RustFS (uses env var RUSTFS_SECURE)
        posts_bucket: Bucket name for posts (uses env var RUSTFS_POSTS_BUCKET)
        comments_bucket: Bucket name for comments (uses env var RUSTFS_COMMENTS_BUCKET)
        
    Returns:
        KafkaConsumer instance
    """
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        rustfs_endpoint=rustfs_endpoint,
        rustfs_port=rustfs_port,
        rustfs_access_key=rustfs_access_key,
        rustfs_secret_key=rustfs_secret_key,
        rustfs_secure=rustfs_secure,
        posts_bucket=posts_bucket,
        comments_bucket=comments_bucket
    )
    
    return consumer


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("Starting Kafka consumer...")
    print("Press Ctrl+C to stop\n")
    
    # Create and start the consumer
    consumer = start_consumer()
    
    try:
        # Consume messages indefinitely
        consumer.consume_messages()
    except KeyboardInterrupt:
        print("\nStopping consumer...")
        consumer.stop()
    except Exception as e:
        print(f"Error: {e}")
        consumer.close()
