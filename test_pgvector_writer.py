#!/usr/bin/env python3
"""
Test suite for pgvector_writer.py - PostgreSQL with pgvector functionality.
"""

import json
import unittest
import os
from unittest.mock import patch, MagicMock
from pgvector_writer import PgVectorDB, EmbeddingGenerator, PgVectorWriterConsumer, start_pgvector_writer


class TestPgVectorDB(unittest.TestCase):
    """Test PgVectorDB class."""
    
    def setUp(self):
        """Set up test environment."""
        # Mock environment variables
        self.original_env = {}
        for key in ['PG_HOST', 'PG_PORT', 'PG_DATABASE', 'PG_USER', 'PG_PASSWORD']:
            if key in os.environ:
                self.original_env[key] = os.environ[key]
        
        # Set test environment
        os.environ['PG_HOST'] = 'test-host'
        os.environ['PG_PORT'] = '5432'
        os.environ['PG_DATABASE'] = 'test-db'
        os.environ['PG_USER'] = 'test-user'
        os.environ['PG_PASSWORD'] = 'test-password'
    
    def tearDown(self):
        """Clean up test environment."""
        # Restore original environment
        for key, value in self.original_env.items():
            os.environ[key] = value
        for key in ['PG_HOST', 'PG_PORT', 'PG_DATABASE', 'PG_USER', 'PG_PASSWORD']:
            if key not in self.original_env and key in os.environ:
                del os.environ[key]
    
    @patch('pgvector_writer.psycopg2.connect')
    def test_initialization(self, mock_connect):
        """Test PgVectorDB initialization."""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        db = PgVectorDB()
        
        self.assertEqual(db.host, 'test-host')
        self.assertEqual(db.port, 5432)
        self.assertEqual(db.database, 'test-db')
        self.assertEqual(db.user, 'test-user')
        self.assertEqual(db.password, 'test-password')
        self.assertEqual(db.connection, mock_connection)
        
        # Verify connection was established with correct parameters
        mock_connect.assert_called_once_with(
            host='test-host',
            port=5432,
            database='test-db',
            user='test-user',
            password='test-password'
        )
    
    @patch('pgvector_writer.psycopg2.connect')
    def test_initialization_with_custom_params(self, mock_connect):
        """Test PgVectorDB initialization with custom parameters."""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        db = PgVectorDB(
            host='custom-host',
            port=5433,
            database='custom-db',
            user='custom-user',
            password='custom-password'
        )
        
        self.assertEqual(db.host, 'custom-host')
        self.assertEqual(db.port, 5433)
        self.assertEqual(db.database, 'custom-db')
    
    @patch('pgvector_writer.psycopg2.connect')
    def test_connect_success(self, mock_connect):
        """Test successful database connection."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        db = PgVectorDB()
        db.connect()
        
        self.assertEqual(db.connection, mock_connection)
        
        # Verify pgvector extension was enabled
        mock_cursor.execute.assert_called_with("CREATE EXTENSION IF NOT EXISTS vector")
        mock_connection.commit.assert_called_once()
    
    @patch('pgvector_writer.psycopg2.connect')
    def test_connect_failure(self, mock_connect):
        """Test database connection failure."""
        mock_connect.side_effect = Exception("Connection failed")
        
        db = PgVectorDB()
        
        with self.assertRaises(Exception):
            db.connect()
    
    @patch('pgvector_writer.psycopg2.connect')
    def test_close(self, mock_connect):
        """Test database connection close."""
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        db = PgVectorDB()
        db.connect()
        db.close()
        
        mock_connection.close.assert_called_once()
        self.assertIsNone(db.connection)
    
    @patch('pgvector_writer.psycopg2.connect')
    def test_initialize_tables(self, mock_connect):
        """Test table initialization."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        db = PgVectorDB()
        db.connect()
        db.initialize_tables()
        
        # Verify all expected tables were created
        expected_tables = [
            'reddit_posts',
            'reddit_comments', 
            'reddit_users',
            'post_embeddings',
            'comment_embeddings',
            'processing_log'
        ]
        
        # Verify indexes were created
        expected_indexes = [
            'idx_post_embeddings',
            'idx_comment_embeddings'
        ]
        
        # Check that create table statements were executed
        create_statements = [call[0][0] for call in mock_cursor.execute.call_args_list]
        
        for table in expected_tables:
            table_created = any(f'CREATE TABLE IF NOT EXISTS {table}' in stmt for stmt in create_statements)
            self.assertTrue(table_created, f"Table {table} was not created")
        
        for index in expected_indexes:
            index_created = any(f'CREATE INDEX IF NOT EXISTS {index}' in stmt for stmt in create_statements)
            self.assertTrue(index_created, f"Index {index} was not created")
    
    @patch('pgvector_writer.psycopg2.connect')
    def test_store_post_success(self, mock_connect):
        """Test successful post storage."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.rowcount = 1  # Simulate successful insert
        mock_connect.return_value = mock_connection
        
        db = PgVectorDB()
        db.connect()
        
        post_data = {
            "reddit_id": "t3_test123",
            "title": "Test Post",
            "content": "Test content",
            "author": "test_author",
            "subreddit": "test_sub",
            "score": 100
        }
        
        embeddings = [0.1, 0.2, 0.3]  # Mock embeddings
        
        post_id = db.store_post(post_data, embeddings)
        
        self.assertIsNotNone(post_id)
        
        # Verify post was inserted
        insert_call = None
        for call in mock_cursor.execute.call_args_list:
            if 'INSERT INTO reddit_posts' in call[0][0]:
                insert_call = call
                break
        
        self.assertIsNotNone(insert_call)
        
        # Verify embeddings were stored
        embeddings_inserted = any('INSERT INTO post_embeddings' in call[0][0] 
                                  for call in mock_cursor.execute.call_args_list)
        self.assertTrue(embeddings_inserted)
    
    @patch('pgvector_writer.psycopg2.connect')
    def test_store_post_duplicate(self, mock_connect):
        """Test post storage with duplicate (existing post)."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.rowcount = 0  # Simulate no insert (duplicate)
        
        # Mock fetchone to return existing post ID
        mock_cursor.fetchone.return_value = ('existing-post-id',)
        mock_connect.return_value = mock_connection
        
        db = PgVectorDB()
        db.connect()
        
        post_data = {
            "reddit_id": "t3_existing",
            "title": "Existing Post",
            "content": "Existing content"
        }
        
        post_id = db.store_post(post_data)
        
        self.assertEqual(post_id, 'existing-post-id')
    
    @patch('pgvector_writer.psycopg2.connect')
    def test_store_comment_success(self, mock_connect):
        """Test successful comment storage."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.rowcount = 1  # Simulate successful insert
        mock_connect.return_value = mock_connection
        
        db = PgVectorDB()
        db.connect()
        
        comment_data = {
            "reddit_id": "t1_test123",
            "post_id": "post123",
            "content": "Test comment",
            "author": "test_author",
            "score": 50,
            "depth": 1,
            "parent_id": "parent123"
        }
        
        embeddings = [0.1, 0.2, 0.3]  # Mock embeddings
        
        comment_id = db.store_comment(comment_data, embeddings)
        
        self.assertIsNotNone(comment_id)
        
        # Verify comment was inserted
        insert_call = None
        for call in mock_cursor.execute.call_args_list:
            if 'INSERT INTO reddit_comments' in call[0][0]:
                insert_call = call
                break
        
        self.assertIsNotNone(insert_call)
    
    @patch('pgvector_writer.psycopg2.connect')
    def test_store_user_success(self, mock_connect):
        """Test successful user storage."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        db = PgVectorDB()
        db.connect()
        
        user_data = {
            "username": "test_user",
            "metadata": {"role": "tester"}
        }
        
        user_id = db.store_user(user_data)
        
        self.assertIsNotNone(user_id)
        
        # Verify user was inserted/updated
        insert_call = None
        for call in mock_cursor.execute.call_args_list:
            if 'INSERT INTO reddit_users' in call[0][0]:
                insert_call = call
                break
        
        self.assertIsNotNone(insert_call)
    
    @patch('pgvector_writer.psycopg2.connect')
    def test_log_processing(self, mock_connect):
        """Test processing log functionality."""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_connection
        
        db = PgVectorDB()
        db.connect()
        
        # Log successful processing
        db.log_processing("msg123", "completed")
        
        # Log failed processing
        db.log_processing("msg456", "failed", "Test error")
        
        # Verify both log entries were made
        log_calls = [call for call in mock_cursor.execute.call_args_list 
                    if 'INSERT INTO processing_log' in call[0][0]]
        
        self.assertEqual(len(log_calls), 2)


class TestEmbeddingGenerator(unittest.TestCase):
    """Test EmbeddingGenerator class."""
    
    @patch('pgvector_writer.SentenceTransformer')
    def test_initialization_with_model(self, mock_sentence_transformer):
        """Test EmbeddingGenerator initialization with model available."""
        mock_model = MagicMock()
        mock_sentence_transformer.return_value = mock_model
        
        # Temporarily set the flag to True
        import pgvector_writer
        original_flag = pgvector_writer.SENTENCE_TRANSFORMERS_AVAILABLE
        pgvector_writer.SENTENCE_TRANSFORMERS_AVAILABLE = True
        
        try:
            generator = EmbeddingGenerator()
            self.assertEqual(generator.model_name, "all-MiniLM-L6-v2")
            self.assertEqual(generator.model, mock_model)
            mock_sentence_transformer.assert_called_once_with("all-MiniLM-L6-v2")
        finally:
            pgvector_writer.SENTENCE_TRANSFORMERS_AVAILABLE = original_flag
    
    @patch('pgvector_writer.SentenceTransformer')
    def test_initialization_without_model(self, mock_sentence_transformer):
        """Test EmbeddingGenerator initialization without model."""
        # Temporarily set the flag to False
        import pgvector_writer
        original_flag = pgvector_writer.SENTENCE_TRANSFORMERS_AVAILABLE
        pgvector_writer.SENTENCE_TRANSFORMERS_AVAILABLE = False
        
        try:
            generator = EmbeddingGenerator()
            self.assertEqual(generator.model_name, "all-MiniLM-L6-v2")
            self.assertIsNone(generator.model)
            mock_sentence_transformer.assert_not_called()
        finally:
            pgvector_writer.SENTENCE_TRANSFORMERS_AVAILABLE = original_flag
    
    @patch('pgvector_writer.SentenceTransformer')
    def test_generate_success(self, mock_sentence_transformer):
        """Test successful embedding generation."""
        mock_model = MagicMock()
        mock_model.encode.return_value = [0.1, 0.2, 0.3]
        mock_sentence_transformer.return_value = mock_model
        
        # Temporarily set the flag to True
        import pgvector_writer
        original_flag = pgvector_writer.SENTENCE_TRANSFORMERS_AVAILABLE
        pgvector_writer.SENTENCE_TRANSFORMERS_AVAILABLE = True
        
        try:
            generator = EmbeddingGenerator()
            embeddings = generator.generate("Test text content")
            
            self.assertIsNotNone(embeddings)
            self.assertEqual(embeddings, [0.1, 0.2, 0.3])
            mock_model.encode.assert_called_once()
        finally:
            pgvector_writer.SENTENCE_TRANSFORMERS_AVAILABLE = original_flag
    
    @patch('pgvector_writer.SentenceTransformer')
    def test_generate_empty_text(self, mock_sentence_transformer):
        """Test embedding generation with empty text."""
        mock_model = MagicMock()
        mock_sentence_transformer.return_value = mock_model
        
        # Temporarily set the flag to True
        import pgvector_writer
        original_flag = pgvector_writer.SENTENCE_TRANSFORMERS_AVAILABLE
        pgvector_writer.SENTENCE_TRANSFORMERS_AVAILABLE = True
        
        try:
            generator = EmbeddingGenerator()
            embeddings = generator.generate("")
            
            self.assertIsNone(embeddings)
            mock_model.encode.assert_not_called()
        finally:
            pgvector_writer.SENTENCE_TRANSFORMERS_AVAILABLE = original_flag
    
    @patch('pgvector_writer.SentenceTransformer')
    def test_generate_no_model(self, mock_sentence_transformer):
        """Test embedding generation without model."""
        # Temporarily set the flag to False
        import pgvector_writer
        original_flag = pgvector_writer.SENTENCE_TRANSFORMERS_AVAILABLE
        pgvector_writer.SENTENCE_TRANSFORMERS_AVAILABLE = False
        
        try:
            generator = EmbeddingGenerator()
            embeddings = generator.generate("Test text content")
            
            self.assertIsNone(embeddings)
            mock_sentence_transformer.assert_not_called()
        finally:
            pgvector_writer.SENTENCE_TRANSFORMERS_AVAILABLE = original_flag
    
    @patch('pgvector_writer.SentenceTransformer')
    def test_generate_long_text_truncation(self, mock_sentence_transformer):
        """Test embedding generation with long text truncation."""
        mock_model = MagicMock()
        mock_model.encode.return_value = [0.1, 0.2, 0.3]
        mock_sentence_transformer.return_value = mock_model
        
        # Temporarily set the flag to True
        import pgvector_writer
        original_flag = pgvector_writer.SENTENCE_TRANSFORMERS_AVAILABLE
        pgvector_writer.SENTENCE_TRANSFORMERS_AVAILABLE = True
        
        try:
            generator = EmbeddingGenerator()
            long_text = "A" * 1000  # Very long text
            embeddings = generator.generate(long_text)
            
            self.assertIsNotNone(embeddings)
            # Verify text was truncated to 512 characters
            call_args = mock_model.encode.call_args[0][0]
            self.assertEqual(len(call_args), 512)
        finally:
            pgvector_writer.SENTENCE_TRANSFORMERS_AVAILABLE = original_flag


class TestPgVectorWriterConsumer(unittest.TestCase):
    """Test PgVectorWriterConsumer class."""
    
    def setUp(self):
        """Set up test environment."""
        # Mock environment variables
        self.original_env = {}
        for key in ['KAFKA_BOOTSTRAP_SERVERS', 'KAFKA_TOPIC', 'KAFKA_CONSUMER_GROUP',
                   'KAFKA_AUTO_OFFSET_RESET', 'PG_HOST', 'PG_PORT', 'PG_DATABASE',
                   'PG_USER', 'PG_PASSWORD']:
            if key in os.environ:
                self.original_env[key] = os.environ[key]
        
        # Set test environment
        os.environ['KAFKA_BOOTSTRAP_SERVERS'] = 'test-server:9092'
        os.environ['KAFKA_TOPIC'] = 'test-topic'
        os.environ['KAFKA_CONSUMER_GROUP'] = 'test-group'
        os.environ['KAFKA_AUTO_OFFSET_RESET'] = 'earliest'
        os.environ['PG_HOST'] = 'test-host'
        os.environ['PG_PORT'] = '5432'
        os.environ['PG_DATABASE'] = 'test-db'
        os.environ['PG_USER'] = 'test-user'
        os.environ['PG_PASSWORD'] = 'test-password'
    
    def tearDown(self):
        """Clean up test environment."""
        # Restore original environment
        for key, value in self.original_env.items():
            os.environ[key] = value
        for key in ['KAFKA_BOOTSTRAP_SERVERS', 'KAFKA_TOPIC', 'KAFKA_CONSUMER_GROUP',
                   'KAFKA_AUTO_OFFSET_RESET', 'PG_HOST', 'PG_PORT', 'PG_DATABASE',
                   'PG_USER', 'PG_PASSWORD']:
            if key not in self.original_env and key in os.environ:
                del os.environ[key]
    
    @patch('pgvector_writer.Consumer')
    @patch('pgvector_writer.PgVectorDB')
    @patch('pgvector_writer.EmbeddingGenerator')
    def test_initialization(self, mock_embedding_gen, mock_pg_db, mock_consumer_class):
        """Test PgVectorWriterConsumer initialization."""
        mock_consumer_instance = MagicMock()
        mock_consumer_class.return_value = mock_consumer_instance
        
        mock_db_instance = MagicMock()
        mock_pg_db.return_value = mock_db_instance
        
        mock_embedding_gen_instance = MagicMock()
        mock_embedding_gen.return_value = mock_embedding_gen_instance
        
        consumer = PgVectorWriterConsumer()
        
        self.assertEqual(consumer.bootstrap_servers, 'test-server:9092')
        self.assertEqual(consumer.topic, 'test-topic')
        self.assertEqual(consumer.group_id, 'test-group')
        self.assertEqual(consumer.auto_offset_reset, 'earliest')
        self.assertEqual(consumer.pg_db, mock_db_instance)
        self.assertEqual(consumer.embedding_generator, mock_embedding_gen_instance)
        self.assertIsNone(consumer.consumer)
    
    @patch('pgvector_writer.Consumer')
    @patch('pgvector_writer.PgVectorDB')
    @patch('pgvector_writer.EmbeddingGenerator')
    def test_initialization_with_auth(self, mock_embedding_gen, mock_pg_db, mock_consumer_class):
        """Test PgVectorWriterConsumer initialization with authentication."""
        os.environ['KAFKA_USERNAME'] = 'test-user'
        os.environ['KAFKA_PASSWORD'] = 'test-password'
        
        mock_consumer_instance = MagicMock()
        mock_consumer_class.return_value = mock_consumer_instance
        
        mock_db_instance = MagicMock()
        mock_pg_db.return_value = mock_db_instance
        
        mock_embedding_gen_instance = MagicMock()
        mock_embedding_gen.return_value = mock_embedding_gen_instance
        
        consumer = PgVectorWriterConsumer()
        
        # Verify authentication config was passed to consumer
        call_args = mock_consumer_class.call_args[1]  # kwargs
        self.assertIn('security.protocol', call_args)
        self.assertIn('sasl.mechanisms', call_args)
        self.assertIn('sasl.username', call_args)
        self.assertIn('sasl.password', call_args)
    
    @patch('pgvector_writer.Consumer')
    @patch('pgvector_writer.PgVectorDB')
    @patch('pgvector_writer.EmbeddingGenerator')
    def test_start_and_stop(self, mock_embedding_gen, mock_pg_db, mock_consumer_class):
        """Test consumer start and stop."""
        mock_consumer_instance = MagicMock()
        mock_consumer_class.return_value = mock_consumer_instance
        
        mock_db_instance = MagicMock()
        mock_pg_db.return_value = mock_db_instance
        
        mock_embedding_gen_instance = MagicMock()
        mock_embedding_gen.return_value = mock_embedding_gen_instance
        
        consumer = PgVectorWriterConsumer()
        
        # Mock consumer.poll to return None (no messages)
        mock_consumer_instance.poll.return_value = None
        
        # Start consumer in a separate thread for early termination
        import threading
        def consume_thread():
            consumer.start()
        
        thread = threading.Thread(target=consume_thread)
        thread.start()
        
        # Let it run briefly
        thread.join(timeout=0.5)
        
        # Stop the consumer
        consumer.stop()
        
        # Verify consumer was properly initialized and configured
        self.assertIsNotNone(consumer.consumer)
        mock_consumer_instance.subscribe.assert_called_once_with(['test-topic'])
        mock_db_instance.connect.assert_called_once()
        mock_db_instance.initialize_tables.assert_called_once()
        
        # Verify consumer was closed
        mock_consumer_instance.close.assert_called_once()
        mock_db_instance.close.assert_called_once()
    
    @patch('pgvector_writer.Consumer')
    @patch('pgvector_writer.PgVectorDB')
    @patch('pgvector_writer.EmbeddingGenerator')
    def test_process_message_success(self, mock_embedding_gen, mock_pg_db, mock_consumer_class):
        """Test successful message processing."""
        mock_consumer_instance = MagicMock()
        mock_consumer_class.return_value = mock_consumer_instance
        
        mock_db_instance = MagicMock()
        mock_pg_db.return_value = mock_db_instance
        
        mock_embedding_gen_instance = MagicMock()
        mock_embedding_gen_instance.generate.return_value = [0.1, 0.2, 0.3]
        mock_embedding_gen.return_value = mock_embedding_gen_instance
        
        consumer = PgVectorWriterConsumer()
        
        # Create a test message
        test_data = {
            "post": {
                "reddit_id": "t3_test123",
                "title": "Test Post",
                "content": "Test content",
                "author": "test_author",
                "subreddit": "test_sub",
                "score": 100
            },
            "comments": [
                {
                    "reddit_id": "t1_test456",
                    "content": "Test comment",
                    "author": "comment_author",
                    "score": 50,
                    "depth": 1,
                    "parent_id": "t3_test123"
                }
            ],
            "users": [
                {"username": "test_author"},
                {"username": "comment_author"}
            ]
        }
        
        mock_msg = MagicMock()
        mock_msg.value.return_value = json.dumps(test_data).encode('utf-8')
        
        # Process the message
        consumer._process_message(mock_msg)
        
        # Verify data was stored
        mock_db_instance.store_post.assert_called_once()
        mock_db_instance.store_comment.assert_called_once()
        mock_db_instance.store_user.assert_called()
        mock_db_instance.log_processing.assert_called_once_with("unknown", "completed")
    
    @patch('pgvector_writer.Consumer')
    @patch('pgvector_writer.PgVectorDB')
    @patch('pgvector_writer.EmbeddingGenerator')
    def test_process_message_json_error(self, mock_embedding_gen, mock_pg_db, mock_consumer_class):
        """Test message processing with JSON decode error."""
        mock_consumer_instance = MagicMock()
        mock_consumer_class.return_value = mock_consumer_instance
        
        mock_db_instance = MagicMock()
        mock_pg_db.return_value = mock_db_instance
        
        mock_embedding_gen_instance = MagicMock()
        mock_embedding_gen.return_value = mock_embedding_gen_instance
        
        consumer = PgVectorWriterConsumer()
        
        # Create a message with invalid JSON
        mock_msg = MagicMock()
        mock_msg.value.return_value = b'invalid json'
        
        # Process the message (should handle the error gracefully)
        consumer._process_message(mock_msg)
        
        # Verify error was logged
        mock_db_instance.log_processing.assert_called_once()
        call_args = mock_db_instance.log_processing.call_args[0]
        self.assertEqual(call_args[1], "failed")


class TestStartPgVectorWriter(unittest.TestCase):
    """Test start_pgvector_writer function."""
    
    def setUp(self):
        """Set up test environment."""
        # Mock environment variables
        self.original_env = {}
        for key in ['KAFKA_BOOTSTRAP_SERVERS', 'KAFKA_TOPIC', 'KAFKA_CONSUMER_GROUP',
                   'KAFKA_AUTO_OFFSET_RESET', 'PG_HOST', 'PG_PORT', 'PG_DATABASE',
                   'PG_USER', 'PG_PASSWORD']:
            if key in os.environ:
                self.original_env[key] = os.environ[key]
        
        # Set test environment
        os.environ['KAFKA_BOOTSTRAP_SERVERS'] = 'test-server:9092'
        os.environ['KAFKA_TOPIC'] = 'test-topic'
        os.environ['PG_HOST'] = 'test-host'
        os.environ['PG_PORT'] = '5432'
        os.environ['PG_DATABASE'] = 'test-db'
    
    def tearDown(self):
        """Clean up test environment."""
        # Restore original environment
        for key, value in self.original_env.items():
            os.environ[key] = value
        for key in ['KAFKA_BOOTSTRAP_SERVERS', 'KAFKA_TOPIC', 'KAFKA_CONSUMER_GROUP',
                   'KAFKA_AUTO_OFFSET_RESET', 'PG_HOST', 'PG_PORT', 'PG_DATABASE',
                   'PG_USER', 'PG_PASSWORD']:
            if key not in self.original_env and key in os.environ:
                del os.environ[key]
    
    @patch('pgvector_writer.PgVectorWriterConsumer')
    def test_start_pgvector_writer(self, mock_consumer_class):
        """Test start_pgvector_writer function."""
        mock_consumer_instance = MagicMock()
        mock_consumer_class.return_value = mock_consumer_instance
        
        consumer = start_pgvector_writer()
        
        self.assertEqual(consumer, mock_consumer_instance)
        mock_consumer_class.assert_called_once()
    
    @patch('pgvector_writer.PgVectorWriterConsumer')
    def test_start_pgvector_writer_with_params(self, mock_consumer_class):
        """Test start_pgvector_writer function with custom parameters."""
        mock_consumer_instance = MagicMock()
        mock_consumer_class.return_value = mock_consumer_instance
        
        consumer = start_pgvector_writer(
            bootstrap_servers='custom-server:9092',
            topic='custom-topic',
            pg_host='custom-host'
        )
        
        self.assertEqual(consumer, mock_consumer_instance)
        call_args = mock_consumer_class.call_args[1]
        self.assertEqual(call_args['bootstrap_servers'], 'custom-server:9092')
        self.assertEqual(call_args['topic'], 'custom-topic')


if __name__ == '__main__':
    unittest.main()