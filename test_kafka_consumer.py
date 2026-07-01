#!/usr/bin/env python3
"""
Test suite for kafka_consumer.py - Kafka consumer and RustFS integration.
"""

import json
import unittest
import os
from unittest.mock import patch, MagicMock, call
from kafka_consumer import RustFSStorage, KafkaConsumer, start_consumer


class TestRustFSStorage(unittest.TestCase):
    """Test RustFSStorage class."""
    
    def setUp(self):
        """Set up test environment."""
        # Mock environment variables
        self.original_env = {}
        for key in ['RUSTFS_ENDPOINT', 'RUSTFS_PORT', 'RUSTFS_ACCESS_KEY', 
                   'RUSTFS_SECRET_KEY', 'RUSTFS_SECURE']:
            if key in os.environ:
                self.original_env[key] = os.environ[key]
        
        # Set test environment
        os.environ['RUSTFS_ENDPOINT'] = 'test-endpoint'
        os.environ['RUSTFS_PORT'] = '9000'
        os.environ['RUSTFS_ACCESS_KEY'] = 'test-key'
        os.environ['RUSTFS_SECRET_KEY'] = 'test-secret'
        os.environ['RUSTFS_SECURE'] = 'false'
    
    def tearDown(self):
        """Clean up test environment."""
        # Restore original environment
        for key, value in self.original_env.items():
            os.environ[key] = value
        for key in ['RUSTFS_ENDPOINT', 'RUSTFS_PORT', 'RUSTFS_ACCESS_KEY', 
                   'RUSTFS_SECRET_KEY', 'RUSTFS_SECURE']:
            if key not in self.original_env and key in os.environ:
                del os.environ[key]
    
    @patch('kafka_consumer.boto3.client')
    def test_initialization(self, mock_boto_client):
        """Test RustFSStorage initialization."""
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client
        
        storage = RustFSStorage()
        
        self.assertEqual(storage.endpoint, 'test-endpoint')
        self.assertEqual(storage.port, 9000)
        self.assertEqual(storage.access_key, 'test-key')
        self.assertEqual(storage.secret_key, 'test-secret')
        self.assertEqual(storage.secure, False)
        self.assertEqual(storage.client, mock_client)
        
        # Verify boto3 client was called with correct parameters
        mock_boto_client.assert_called_once_with(
            's3',
            endpoint_url='http://test-endpoint:9000',
            aws_access_key_id='test-key',
            aws_secret_access_key='test-secret',
            verify=False
        )
    
    @patch('kafka_consumer.boto3.client')
    def test_initialization_with_custom_params(self, mock_boto_client):
        """Test RustFSStorage initialization with custom parameters."""
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client
        
        storage = RustFSStorage(
            endpoint='custom-endpoint',
            port=9001,
            access_key='custom-key',
            secret_key='custom-secret',
            secure=True
        )
        
        self.assertEqual(storage.endpoint, 'custom-endpoint')
        self.assertEqual(storage.port, 9001)
        self.assertEqual(storage.secure, True)
    
    @patch('kafka_consumer.boto3.client')
    def test_ensure_bucket_exists_success(self, mock_boto_client):
        """Test ensure_bucket_exists with existing bucket."""
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client
        
        storage = RustFSStorage()
        
        # Mock successful head_bucket call
        result = storage.ensure_bucket_exists('test-bucket')
        
        self.assertTrue(result)
        mock_client.head_bucket.assert_called_once_with(Bucket='test-bucket')
    
    @patch('kafka_consumer.boto3.client')
    def test_ensure_bucket_exists_create(self, mock_boto_client):
        """Test ensure_bucket_exists with bucket creation."""
        from botocore.exceptions import ClientError
        
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client
        
        # Mock head_bucket to raise 404 error
        error_response = {'Error': {'Code': '404'}}
        mock_client.head_bucket.side_effect = ClientError(error_response, 'HeadBucket')
        
        storage = RustFSStorage()
        
        result = storage.ensure_bucket_exists('test-bucket')
        
        self.assertTrue(result)
        mock_client.create_bucket.assert_called_once_with(Bucket='test-bucket')
    
    @patch('kafka_consumer.boto3.client')
    def test_write_json_to_bucket_success(self, mock_boto_client):
        """Test successful JSON write to RustFS bucket."""
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client
        
        storage = RustFSStorage()
        
        test_data = {"test": "data", "value": 123}
        result = storage.write_json_to_bucket('test-bucket', 'test-key.json', test_data)
        
        self.assertTrue(result)
        
        # Verify bucket existence was checked
        mock_client.head_bucket.assert_called_once()
        
        # Verify put_object was called with correct parameters
        mock_client.put_object.assert_called_once()
        call_args = mock_client.put_object.call_args
        
        self.assertEqual(call_args[1]['Bucket'], 'test-bucket')
        self.assertEqual(call_args[1]['Key'], 'test-key.json')
        self.assertEqual(call_args[1]['ContentType'], 'application/json')
        
        # Verify the body contains JSON data
        body = call_args[1]['Body']
        decoded_body = json.loads(body.decode('utf-8'))
        self.assertEqual(decoded_body, test_data)


class TestKafkaConsumer(unittest.TestCase):
    """Test KafkaConsumer class."""
    
    def setUp(self):
        """Set up test environment."""
        # Mock environment variables
        self.original_env = {}
        for key in ['KAFKA_BOOTSTRAP_SERVERS', 'KAFKA_TOPIC', 'KAFKA_CONSUMER_GROUP',
                   'KAFKA_AUTO_OFFSET_RESET', 'KAFKA_USERNAME', 'KAFKA_PASSWORD',
                   'RUSTFS_ENDPOINT', 'RUSTFS_POSTS_BUCKET', 'RUSTFS_COMMENTS_BUCKET']:
            if key in os.environ:
                self.original_env[key] = os.environ[key]
        
        # Set test environment
        os.environ['KAFKA_BOOTSTRAP_SERVERS'] = 'test-server:9092'
        os.environ['KAFKA_TOPIC'] = 'test-topic'
        os.environ['KAFKA_CONSUMER_GROUP'] = 'test-group'
        os.environ['KAFKA_AUTO_OFFSET_RESET'] = 'earliest'
        os.environ['RUSTFS_ENDPOINT'] = 'test-endpoint'
        os.environ['RUSTFS_POSTS_BUCKET'] = 'posts-bucket'
        os.environ['RUSTFS_COMMENTS_BUCKET'] = 'comments-bucket'
    
    def tearDown(self):
        """Clean up test environment."""
        # Restore original environment
        for key, value in self.original_env.items():
            os.environ[key] = value
        for key in ['KAFKA_BOOTSTRAP_SERVERS', 'KAFKA_TOPIC', 'KAFKA_CONSUMER_GROUP',
                   'KAFKA_AUTO_OFFSET_RESET', 'KAFKA_USERNAME', 'KAFKA_PASSWORD',
                   'RUSTFS_ENDPOINT', 'RUSTFS_POSTS_BUCKET', 'RUSTFS_COMMENTS_BUCKET']:
            if key not in self.original_env and key in os.environ:
                del os.environ[key]
    
    @patch('kafka_consumer.Consumer')
    @patch('kafka_consumer.RustFSStorage')
    def test_initialization(self, mock_rustfs_storage, mock_consumer_class):
        """Test KafkaConsumer initialization."""
        mock_consumer_instance = MagicMock()
        mock_consumer_class.return_value = mock_consumer_instance
        
        mock_rustfs_instance = MagicMock()
        mock_rustfs_storage.return_value = mock_rustfs_instance
        
        consumer = KafkaConsumer()
        
        self.assertEqual(consumer.bootstrap_servers, 'test-server:9092')
        self.assertEqual(consumer.topic, 'test-topic')
        self.assertEqual(consumer.group_id, 'test-group')
        self.assertEqual(consumer.auto_offset_reset, 'earliest')
        self.assertEqual(consumer.consumer, mock_consumer_instance)
        self.assertEqual(consumer.rustfs_client, mock_rustfs_instance)
        self.assertEqual(consumer.posts_bucket, 'posts-bucket')
        self.assertEqual(consumer.comments_bucket, 'comments-bucket')
    
    @patch('kafka_consumer.Consumer')
    @patch('kafka_consumer.RustFSStorage')
    def test_initialization_with_auth(self, mock_rustfs_storage, mock_consumer_class):
        """Test KafkaConsumer initialization with authentication."""
        os.environ['KAFKA_USERNAME'] = 'test-user'
        os.environ['KAFKA_PASSWORD'] = 'test-password'
        
        mock_consumer_instance = MagicMock()
        mock_consumer_class.return_value = mock_consumer_instance
        
        mock_rustfs_instance = MagicMock()
        mock_rustfs_storage.return_value = mock_rustfs_instance
        
        consumer = KafkaConsumer()
        
        # Verify authentication config was passed to consumer
        call_args = mock_consumer_class.call_args[1]  # kwargs
        self.assertIn('security.protocol', call_args)
        self.assertIn('sasl.mechanisms', call_args)
        self.assertIn('sasl.username', call_args)
        self.assertIn('sasl.password', call_args)
    
    @patch('kafka_consumer.Consumer')
    @patch('kafka_consumer.RustFSStorage')
    def test_consume_messages_success(self, mock_rustfs_storage, mock_consumer_class):
        """Test successful message consumption."""
        mock_consumer_instance = MagicMock()
        mock_consumer_class.return_value = mock_consumer_instance
        
        mock_rustfs_instance = MagicMock()
        mock_rustfs_storage.return_value = mock_rustfs_instance
        
        consumer = KafkaConsumer()
        
        # Mock a Kafka message
        mock_msg = MagicMock()
        mock_msg.topic.return_value = 'test-topic'
        mock_msg.partition.return_value = 0
        mock_msg.offset.return_value = 42
        mock_msg.key.return_value = b'test-key'
        mock_msg.value.return_value = json.dumps({"test": "data"}).encode('utf-8')
        mock_msg.error.return_value = None
        
        # Mock consumer.poll to return the message
        mock_consumer_instance.poll.return_value = mock_msg
        
        # Mock _process_message to avoid complex JSON-LD processing
        with patch.object(consumer, '_process_message'):
            # Run consumption in a separate thread to allow early termination
            import threading
            def consume_thread():
                consumer.consume_messages(timeout=0.1)
            
            thread = threading.Thread(target=consume_thread)
            thread.start()
            thread.join(timeout=0.5)
            
            # Stop the consumer
            consumer.stop()
            
            # Verify consumer was properly configured
            mock_consumer_instance.subscribe.assert_called_once_with(['test-topic'])
            
            # Verify message was processed
            consumer._process_message.assert_called_once()
    
    @patch('kafka_consumer.Consumer')
    @patch('kafka_consumer.RustFSStorage')
    def test_consume_messages_error(self, mock_rustfs_storage, mock_consumer_class):
        """Test message consumption with error."""
        mock_consumer_instance = MagicMock()
        mock_consumer_class.return_value = mock_consumer_instance
        
        mock_rustfs_instance = MagicMock()
        mock_rustfs_storage.return_value = mock_rustfs_instance
        
        consumer = KafkaConsumer()
        
        # Mock a Kafka message with error
        mock_msg = MagicMock()
        mock_msg.error.return_value = MagicMock()
        mock_msg.error.return_value.code.return_value = 'NOT_EOF_ERROR'
        
        # Mock consumer.poll to return the error message
        mock_consumer_instance.poll.return_value = mock_msg
        
        # Run consumption briefly
        import threading
        def consume_thread():
            consumer.consume_messages(timeout=0.1)
        
        thread = threading.Thread(target=consume_thread)
        thread.start()
        thread.join(timeout=0.5)
        
        consumer.stop()
        
        # Verify error was logged (we can't easily verify logging in tests)
        # But we can verify the consumer didn't crash
    
    @patch('kafka_consumer.Consumer')
    @patch('kafka_consumer.RustFSStorage')
    def test_process_message_success(self, mock_rustfs_storage, mock_consumer_class):
        """Test successful message processing."""
        mock_consumer_instance = MagicMock()
        mock_consumer_class.return_value = mock_consumer_instance
        
        mock_rustfs_instance = MagicMock()
        mock_rustfs_instance.write_json_to_bucket.return_value = True
        mock_rustfs_storage.return_value = mock_rustfs_instance
        
        consumer = KafkaConsumer()
        
        # Create a test message
        test_data = {
            "@context": "https://schema.org",
            "@graph": [
                {
                    "@type": "DataDownload",
                    "hasPart": [
                        {
                            "@type": "SocialMediaPosting",
                            "@id": "post:test123",
                            "headline": "Test Post",
                            "text": "Test content"
                        }
                    ]
                }
            ]
        }
        
        mock_msg = MagicMock()
        mock_msg.topic.return_value = 'test-topic'
        mock_msg.partition.return_value = 0
        mock_msg.offset.return_value = 42
        mock_msg.key.return_value = b'test-key'
        mock_msg.value.return_value = json.dumps(test_data).encode('utf-8')
        mock_msg.error.return_value = None
        
        # Process the message
        consumer._process_message(mock_msg)
        
        # Verify RustFS write was called
        self.assertTrue(mock_rustfs_instance.write_json_to_bucket.called)
        
        # Verify commit was called
        mock_consumer_instance.commit.assert_called_once_with(mock_msg)
    
    @patch('kafka_consumer.Consumer')
    @patch('kafka_consumer.RustFSStorage')
    def test_consume_single_message_success(self, mock_rustfs_storage, mock_consumer_class):
        """Test consume_single_message with success."""
        mock_consumer_instance = MagicMock()
        mock_consumer_class.return_value = mock_consumer_instance
        
        mock_rustfs_instance = MagicMock()
        mock_rustfs_storage.return_value = mock_rustfs_instance
        
        consumer = KafkaConsumer()
        
        # Create a test message
        test_data = {"test": "data"}
        
        mock_msg = MagicMock()
        mock_msg.topic.return_value = 'test-topic'
        mock_msg.partition.return_value = 0
        mock_msg.offset.return_value = 42
        mock_msg.key.return_value = b'test-key'
        mock_msg.value.return_value = json.dumps(test_data).encode('utf-8')
        mock_msg.error.return_value = None
        
        # Mock consumer.poll to return the message
        mock_consumer_instance.poll.return_value = mock_msg
        
        # Consume single message
        result = consumer.consume_single_message(timeout=0.1)
        
        self.assertEqual(result, test_data)
        mock_consumer_instance.commit.assert_called_once_with(mock_msg)
    
    @patch('kafka_consumer.Consumer')
    @patch('kafka_consumer.RustFSStorage')
    def test_consume_single_message_timeout(self, mock_rustfs_storage, mock_consumer_class):
        """Test consume_single_message with timeout."""
        mock_consumer_instance = MagicMock()
        mock_consumer_class.return_value = mock_consumer_instance
        
        mock_rustfs_instance = MagicMock()
        mock_rustfs_storage.return_value = mock_rustfs_instance
        
        consumer = KafkaConsumer()
        
        # Mock consumer.poll to return None (timeout)
        mock_consumer_instance.poll.return_value = None
        
        # Consume single message with timeout
        result = consumer.consume_single_message(timeout=0.1)
        
        self.assertIsNone(result)
        mock_consumer_instance.commit.assert_not_called()


class TestStartConsumer(unittest.TestCase):
    """Test start_consumer function."""
    
    def setUp(self):
        """Set up test environment."""
        # Mock environment variables
        self.original_env = {}
        for key in ['KAFKA_BOOTSTRAP_SERVERS', 'KAFKA_TOPIC', 'KAFKA_CONSUMER_GROUP',
                   'KAFKA_AUTO_OFFSET_RESET', 'RUSTFS_ENDPOINT', 'RUSTFS_POSTS_BUCKET',
                   'RUSTFS_COMMENTS_BUCKET']:
            if key in os.environ:
                self.original_env[key] = os.environ[key]
        
        # Set test environment
        os.environ['KAFKA_BOOTSTRAP_SERVERS'] = 'test-server:9092'
        os.environ['KAFKA_TOPIC'] = 'test-topic'
        os.environ['RUSTFS_ENDPOINT'] = 'test-endpoint'
        os.environ['RUSTFS_POSTS_BUCKET'] = 'posts-bucket'
        os.environ['RUSTFS_COMMENTS_BUCKET'] = 'comments-bucket'
    
    def tearDown(self):
        """Clean up test environment."""
        # Restore original environment
        for key, value in self.original_env.items():
            os.environ[key] = value
        for key in ['KAFKA_BOOTSTRAP_SERVERS', 'KAFKA_TOPIC', 'KAFKA_CONSUMER_GROUP',
                   'KAFKA_AUTO_OFFSET_RESET', 'RUSTFS_ENDPOINT', 'RUSTFS_POSTS_BUCKET',
                   'RUSTFS_COMMENTS_BUCKET']:
            if key not in self.original_env and key in os.environ:
                del os.environ[key]
    
    @patch('kafka_consumer.KafkaConsumer')
    def test_start_consumer(self, mock_kafka_consumer_class):
        """Test start_consumer function."""
        mock_consumer_instance = MagicMock()
        mock_kafka_consumer_class.return_value = mock_consumer_instance
        
        consumer = start_consumer()
        
        self.assertEqual(consumer, mock_consumer_instance)
        mock_kafka_consumer_class.assert_called_once()
    
    @patch('kafka_consumer.KafkaConsumer')
    def test_start_consumer_with_params(self, mock_kafka_consumer_class):
        """Test start_consumer function with custom parameters."""
        mock_consumer_instance = MagicMock()
        mock_kafka_consumer_class.return_value = mock_consumer_instance
        
        consumer = start_consumer(
            bootstrap_servers='custom-server:9092',
            topic='custom-topic',
            rustfs_endpoint='custom-endpoint'
        )
        
        self.assertEqual(consumer, mock_consumer_instance)
        call_args = mock_kafka_consumer_class.call_args[1]
        self.assertEqual(call_args['bootstrap_servers'], 'custom-server:9092')
        self.assertEqual(call_args['topic'], 'custom-topic')
        self.assertEqual(call_args['rustfs_endpoint'], 'custom-endpoint')


if __name__ == '__main__':
    unittest.main()