#!/usr/bin/env python3
"""
Test suite for neo4j_writer.py - Neo4j knowledge graph functionality.
"""

import json
import unittest
import os
from unittest.mock import patch, MagicMock
from neo4j_writer import (
    Neo4jConnection, EntityExtractor, KnowledgeGraphBuilder, 
    Neo4jWriterConsumer, start_neo4j_writer
)


class TestNeo4jConnection(unittest.TestCase):
    """Test Neo4jConnection class."""
    
    def setUp(self):
        """Set up test environment."""
        # Mock environment variables
        self.original_env = {}
        for key in ['NEO4J_URI', 'NEO4J_USER', 'NEO4J_PASSWORD']:
            if key in os.environ:
                self.original_env[key] = os.environ[key]
        
        # Set test environment
        os.environ['NEO4J_URI'] = 'bolt://test-server:7687'
        os.environ['NEO4J_USER'] = 'test-user'
        os.environ['NEO4J_PASSWORD'] = 'test-password'
    
    def tearDown(self):
        """Clean up test environment."""
        # Restore original environment
        for key, value in self.original_env.items():
            os.environ[key] = value
        for key in ['NEO4J_URI', 'NEO4J_USER', 'NEO4J_PASSWORD']:
            if key not in self.original_env and key in os.environ:
                del os.environ[key]
    
    @patch('neo4j_writer.GraphDatabase.driver')
    def test_initialization(self, mock_driver):
        """Test Neo4jConnection initialization."""
        mock_driver_instance = MagicMock()
        mock_driver.return_value = mock_driver_instance
        
        connection = Neo4jConnection()
        
        self.assertEqual(connection.uri, 'bolt://test-server:7687')
        self.assertEqual(connection.user, 'test-user')
        self.assertEqual(connection.password, 'test-password')
        self.assertEqual(connection.driver, mock_driver_instance)
        
        # Verify driver was created with correct parameters
        mock_driver.assert_called_once_with(
            'bolt://test-server:7687',
            auth=('test-user', 'test-password')
        )
    
    @patch('neo4j_writer.GraphDatabase.driver')
    def test_initialization_with_custom_params(self, mock_driver):
        """Test Neo4jConnection initialization with custom parameters."""
        mock_driver_instance = MagicMock()
        mock_driver.return_value = mock_driver_instance
        
        connection = Neo4jConnection(
            uri='bolt://custom-server:7687',
            user='custom-user',
            password='custom-password'
        )
        
        self.assertEqual(connection.uri, 'bolt://custom-server:7687')
        self.assertEqual(connection.user, 'custom-user')
        self.assertEqual(connection.password, 'custom-password')
    
    @patch('neo4j_writer.GraphDatabase.driver')
    def test_connect_success(self, mock_driver):
        """Test successful Neo4j connection."""
        mock_driver_instance = MagicMock()
        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.consume = MagicMock()
        
        mock_driver_instance.session.return_value.__enter__.return_value = mock_session
        mock_session.run.return_value = mock_result
        mock_driver.return_value = mock_driver_instance
        
        connection = Neo4jConnection()
        connection.connect()
        
        self.assertEqual(connection.driver, mock_driver_instance)
        
        # Verify connection test query was executed
        mock_session.run.assert_called_once_with("RETURN 1")
        mock_result.consume.assert_called_once()
    
    @patch('neo4j_writer.GraphDatabase.driver')
    def test_connect_failure(self, mock_driver):
        """Test Neo4j connection failure."""
        mock_driver.side_effect = Exception("Connection failed")
        
        connection = Neo4jConnection()
        
        with self.assertRaises(Exception):
            connection.connect()
    
    @patch('neo4j_writer.GraphDatabase.driver')
    def test_close(self, mock_driver):
        """Test Neo4j connection close."""
        mock_driver_instance = MagicMock()
        mock_driver.return_value = mock_driver_instance
        
        connection = Neo4jConnection()
        connection.connect()
        connection.close()
        
        mock_driver_instance.close.assert_called_once()
        self.assertIsNone(connection.driver)
    
    @patch('neo4j_writer.GraphDatabase.driver')
    def test_execute_query(self, mock_driver):
        """Test query execution."""
        mock_driver_instance = MagicMock()
        mock_session = MagicMock()
        mock_result = MagicMock()
        
        mock_driver_instance.session.return_value.__enter__.return_value = mock_session
        mock_session.run.return_value = mock_result
        mock_driver.return_value = mock_driver_instance
        
        connection = Neo4jConnection()
        result = connection.execute_query("TEST QUERY", {"param": "value"})
        
        self.assertEqual(result, mock_result)
        mock_session.run.assert_called_once_with("TEST QUERY", {"param": "value"})
    
    @patch('neo4j_writer.GraphDatabase.driver')
    def test_execute_write_transaction(self, mock_driver):
        """Test write transaction execution."""
        mock_driver_instance = MagicMock()
        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_result.consume = MagicMock()
        
        mock_driver_instance.session.return_value.__enter__.return_value = mock_session
        mock_session.execute_write.return_value = mock_result
        mock_driver.return_value = mock_driver_instance
        
        connection = Neo4jConnection()
        result = connection.execute_write_transaction("TEST WRITE QUERY", {"param": "value"})
        
        self.assertEqual(result, mock_result)
        mock_session.execute_write.assert_called_once()


class TestEntityExtractor(unittest.TestCase):
    """Test EntityExtractor class."""
    
    def test_extract_from_jsonld_simple(self):
        """Test entity extraction from simple JSON-LD data."""
        extractor = EntityExtractor()
        
        jsonld_data = {
            "@graph": [
                {
                    "@type": "SocialMediaPosting",
                    "@id": "post:test123",
                    "headline": "Test Post",
                    "text": "Test content",
                    "author": "user:test_author",
                    "upvoteCount": 100,
                    "inLanguage": "test_sub",
                    "datePublished": "2023-01-01T00:00:00Z"
                },
                {
                    "@type": "Person",
                    "@id": "user:test_author",
                    "name": "test_author"
                }
            ]
        }
        
        entities, relationships = extractor.extract_from_jsonld(jsonld_data)
        
        # Verify entities
        self.assertEqual(len(entities), 2)
        
        post_entity = next(e for e in entities if e["type"] == "Post")
        self.assertEqual(post_entity["id"], "post:test123")
        self.assertEqual(post_entity["title"], "Test Post")
        self.assertEqual(post_entity["content"], "Test content")
        
        user_entity = next(e for e in entities if e["type"] == "User")
        self.assertEqual(user_entity["id"], "user:test_author")
        self.assertEqual(user_entity["username"], "test_author")
        
        # Verify relationships
        self.assertEqual(len(relationships), 2)  # POSTED_IN and AUTHORED
        
        posted_in_rel = next(r for r in relationships if r["type"] == "POSTED_IN")
        self.assertEqual(posted_in_rel["source"], "post:test123")
        self.assertEqual(posted_in_rel["target"], "subreddit:test_sub")
        
        authored_rel = next(r for r in relationships if r["type"] == "AUTHORED")
        self.assertEqual(authored_rel["source"], "user:test_author")
        self.assertEqual(authored_rel["target"], "post:test123")
    
    def test_extract_from_jsonld_with_comments(self):
        """Test entity extraction from JSON-LD data with comments."""
        extractor = EntityExtractor()
        
        jsonld_data = {
            "@graph": [
                {
                    "@type": "DataDownload",
                    "hasPart": [
                        {
                            "@type": "SocialMediaPosting",
                            "@id": "post:test123",
                            "headline": "Test Post",
                            "text": "Test content",
                            "author": "user:test_author",
                            "inLanguage": "test_sub"
                        },
                        {
                            "@type": "Comment",
                            "@id": "comment:test456",
                            "text": "Test comment",
                            "author": "user:comment_author",
                            "replyTo": {"@id": "post:test123"},
                            "depth": 1
                        },
                        {
                            "@type": "Person",
                            "@id": "user:test_author",
                            "name": "test_author"
                        },
                        {
                            "@type": "Person",
                            "@id": "user:comment_author",
                            "name": "comment_author"
                        }
                    ]
                }
            ]
        }
        
        entities, relationships = extractor.extract_from_jsonld(jsonld_data)
        
        # Verify entities
        self.assertEqual(len(entities), 4)  # Post, Comment, 2 Users, 1 Subreddit
        
        # Verify relationships
        self.assertEqual(len(relationships), 4)  # POSTED_IN, 2 AUTHORED, 1 REPLIES_TO
        
        replies_to_rel = next(r for r in relationships if r["type"] == "REPLIES_TO")
        self.assertEqual(replies_to_rel["source"], "comment:test456")
        self.assertEqual(replies_to_rel["target"], "post:test123")
    
    def test_extract_from_regular_format(self):
        """Test entity extraction from regular format data."""
        extractor = EntityExtractor()
        
        regular_data = {
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
            ]
        }
        
        entities, relationships = extractor.extract_from_regular(regular_data)
        
        # Verify entities
        self.assertEqual(len(entities), 4)  # Post, Comment, 2 Users, 1 Subreddit
        
        # Verify relationships
        self.assertEqual(len(relationships), 4)  # POSTED_IN, 2 AUTHORED, 1 REPLIES_TO


class TestKnowledgeGraphBuilder(unittest.TestCase):
    """Test KnowledgeGraphBuilder class."""
    
    @patch('neo4j_writer.Neo4jConnection')
    def test_initialization(self, mock_neo4j_connection):
        """Test KnowledgeGraphBuilder initialization."""
        mock_connection = MagicMock()
        mock_neo4j_connection.return_value = mock_connection
        
        builder = KnowledgeGraphBuilder(mock_connection)
        
        self.assertEqual(builder.neo4j, mock_connection)
        self.assertIsInstance(builder.entity_extractor, EntityExtractor)
    
    @patch('neo4j_writer.Neo4jConnection')
    def test_create_constraints(self, mock_neo4j_connection):
        """Test constraint creation."""
        mock_connection = MagicMock()
        mock_neo4j_connection.return_value = mock_connection
        
        builder = KnowledgeGraphBuilder(mock_connection)
        builder.create_constraints()
        
        # Verify constraint queries were executed
        expected_constraints = [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Post) REQUIRE p.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Comment) REQUIRE c.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (u:User) REQUIRE u.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (s:Subreddit) REQUIRE s.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (u:User) REQUIRE u.username IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (s:Subreddit) REQUIRE s.name IS UNIQUE"
        ]
        
        actual_calls = [call[0][0] for call in mock_connection.execute_write_transaction.call_args_list]
        
        for constraint in expected_constraints:
            self.assertIn(constraint, actual_calls)
    
    @patch('neo4j_writer.Neo4jConnection')
    def test_create_indexes(self, mock_neo4j_connection):
        """Test index creation."""
        mock_connection = MagicMock()
        mock_neo4j_connection.return_value = mock_connection
        
        builder = KnowledgeGraphBuilder(mock_connection)
        builder.create_indexes()
        
        # Verify index queries were executed
        expected_indexes = [
            "CREATE INDEX IF NOT EXISTS FOR (p:Post) ON (p.title)",
            "CREATE INDEX IF NOT EXISTS FOR (p:Post) ON (p.score)",
            "CREATE INDEX IF NOT EXISTS FOR (c:Comment) ON (c.content)",
            "CREATE INDEX IF NOT EXISTS FOR (u:User) ON (u.username)",
            "CREATE INDEX IF NOT EXISTS FOR (s:Subreddit) ON (s.name)"
        ]
        
        actual_calls = [call[0][0] for call in mock_connection.execute_write_transaction.call_args_list]
        
        for index in expected_indexes:
            self.assertIn(index, actual_calls)
    
    @patch('neo4j_writer.Neo4jConnection')
    def test_add_entities_and_relationships(self, mock_neo4j_connection):
        """Test adding entities and relationships to the graph."""
        mock_connection = MagicMock()
        mock_neo4j_connection.return_value = mock_connection
        
        builder = KnowledgeGraphBuilder(mock_connection)
        
        # Test entities and relationships
        entities = [
            {
                "id": "post:test123",
                "type": "Post",
                "title": "Test Post",
                "content": "Test content",
                "score": 100
            },
            {
                "id": "user:test_author",
                "type": "User",
                "username": "test_author"
            },
            {
                "id": "subreddit:test_sub",
                "type": "Subreddit",
                "name": "test_sub"
            }
        ]
        
        relationships = [
            {
                "source": "user:test_author",
                "target": "post:test123",
                "type": "AUTHORED"
            },
            {
                "source": "post:test123",
                "target": "subreddit:test_sub",
                "type": "POSTED_IN"
            }
        ]
        
        builder.add_entities_and_relationships(entities, relationships)
        
        # Verify the transaction was executed
        mock_connection.execute_write_transaction.assert_called_once()
        
        # Verify the query contains all expected elements
        query = mock_connection.execute_write_transaction.call_args[0][0]
        self.assertIn("MERGE (p0:Post {id: $post_0.id})", query)
        self.assertIn("MERGE (u0:User {id: $user_0.id})", query)
        self.assertIn("MERGE (s0:Subreddit {id: $subreddit_0.id})", query)
        self.assertIn("MERGE (source)-[r0:AUTHORED]->(target)", query)
        self.assertIn("MERGE (source)-[r1:POSTED_IN]->(target)", query)


class TestNeo4jWriterConsumer(unittest.TestCase):
    """Test Neo4jWriterConsumer class."""
    
    def setUp(self):
        """Set up test environment."""
        # Mock environment variables
        self.original_env = {}
        for key in ['KAFKA_BOOTSTRAP_SERVERS', 'KAFKA_TOPIC', 'KAFKA_CONSUMER_GROUP',
                   'KAFKA_AUTO_OFFSET_RESET', 'NEO4J_URI', 'NEO4J_USER', 'NEO4J_PASSWORD']:
            if key in os.environ:
                self.original_env[key] = os.environ[key]
        
        # Set test environment
        os.environ['KAFKA_BOOTSTRAP_SERVERS'] = 'test-server:9092'
        os.environ['KAFKA_TOPIC'] = 'test-topic'
        os.environ['KAFKA_CONSUMER_GROUP'] = 'test-group'
        os.environ['KAFKA_AUTO_OFFSET_RESET'] = 'earliest'
        os.environ['NEO4J_URI'] = 'bolt://test-server:7687'
        os.environ['NEO4J_USER'] = 'test-user'
        os.environ['NEO4J_PASSWORD'] = 'test-password'
    
    def tearDown(self):
        """Clean up test environment."""
        # Restore original environment
        for key, value in self.original_env.items():
            os.environ[key] = value
        for key in ['KAFKA_BOOTSTRAP_SERVERS', 'KAFKA_TOPIC', 'KAFKA_CONSUMER_GROUP',
                   'KAFKA_AUTO_OFFSET_RESET', 'NEO4J_URI', 'NEO4J_USER', 'NEO4J_PASSWORD']:
            if key not in self.original_env and key in os.environ:
                del os.environ[key]
    
    @patch('neo4j_writer.Consumer')
    @patch('neo4j_writer.Neo4jConnection')
    def test_initialization(self, mock_neo4j_connection, mock_consumer_class):
        """Test Neo4jWriterConsumer initialization."""
        mock_consumer_instance = MagicMock()
        mock_consumer_class.return_value = mock_consumer_instance
        
        mock_connection = MagicMock()
        mock_neo4j_connection.return_value = mock_connection
        
        consumer = Neo4jWriterConsumer()
        
        self.assertEqual(consumer.bootstrap_servers, 'test-server:9092')
        self.assertEqual(consumer.topic, 'test-topic')
        self.assertEqual(consumer.group_id, 'test-group')
        self.assertEqual(consumer.auto_offset_reset, 'earliest')
        self.assertEqual(consumer.neo4j_conn, mock_connection)
        self.assertIsNone(consumer.graph_builder)
        self.assertIsNone(consumer.consumer)
    
    @patch('neo4j_writer.Consumer')
    @patch('neo4j_writer.Neo4jConnection')
    def test_initialization_with_auth(self, mock_neo4j_connection, mock_consumer_class):
        """Test Neo4jWriterConsumer initialization with authentication."""
        os.environ['KAFKA_USERNAME'] = 'test-user'
        os.environ['KAFKA_PASSWORD'] = 'test-password'
        
        mock_consumer_instance = MagicMock()
        mock_consumer_class.return_value = mock_consumer_instance
        
        mock_connection = MagicMock()
        mock_neo4j_connection.return_value = mock_connection
        
        consumer = Neo4jWriterConsumer()
        
        # Verify authentication config was passed to consumer
        call_args = mock_consumer_class.call_args[1]  # kwargs
        self.assertIn('security.protocol', call_args)
        self.assertIn('sasl.mechanisms', call_args)
        self.assertIn('sasl.username', call_args)
        self.assertIn('sasl.password', call_args)
    
    @patch('neo4j_writer.Consumer')
    @patch('neo4j_writer.Neo4jConnection')
    @patch('neo4j_writer.KnowledgeGraphBuilder')
    def test_start_and_stop(self, mock_graph_builder, mock_neo4j_connection, mock_consumer_class):
        """Test consumer start and stop."""
        mock_consumer_instance = MagicMock()
        mock_consumer_class.return_value = mock_consumer_instance
        
        mock_connection = MagicMock()
        mock_neo4j_connection.return_value = mock_connection
        
        mock_builder_instance = MagicMock()
        mock_graph_builder.return_value = mock_builder_instance
        
        consumer = Neo4jWriterConsumer()
        
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
        self.assertIsNotNone(consumer.graph_builder)
        mock_consumer_instance.subscribe.assert_called_once_with(['test-topic'])
        mock_connection.connect.assert_called_once()
        mock_builder_instance.initialize_graph.assert_called_once()
        
        # Verify consumer was closed
        mock_consumer_instance.close.assert_called_once()
    
    @patch('neo4j_writer.Consumer')
    @patch('neo4j_writer.Neo4jConnection')
    @patch('neo4j_writer.KnowledgeGraphBuilder')
    def test_process_message_success(self, mock_graph_builder, mock_neo4j_connection, mock_consumer_class):
        """Test successful message processing."""
        mock_consumer_instance = MagicMock()
        mock_consumer_class.return_value = mock_consumer_instance
        
        mock_connection = MagicMock()
        mock_neo4j_connection.return_value = mock_connection
        
        mock_builder_instance = MagicMock()
        mock_graph_builder.return_value = mock_builder_instance
        
        consumer = Neo4jWriterConsumer()
        
        # Create a test message with JSON-LD data
        test_data = {
            "@graph": [
                {
                    "@type": "SocialMediaPosting",
                    "@id": "post:test123",
                    "headline": "Test Post",
                    "text": "Test content",
                    "author": "user:test_author",
                    "inLanguage": "test_sub"
                },
                {
                    "@type": "Person",
                    "@id": "user:test_author",
                    "name": "test_author"
                }
            ]
        }
        
        mock_msg = MagicMock()
        mock_msg.value.return_value = json.dumps(test_data).encode('utf-8')
        
        # Mock entity extraction
        mock_extractor = MagicMock()
        mock_extractor.extract_from_jsonld.return_value = (
            [{"id": "post:test123", "type": "Post"}],
            [{"source": "user:test_author", "target": "post:test123", "type": "AUTHORED"}]
        )
        
        with patch.object(consumer, 'entity_extractor', mock_extractor):
            # Process the message
            consumer._process_message(mock_msg)
            
            # Verify entities and relationships were added to graph
            mock_builder_instance.add_entities_and_relationships.assert_called_once()
    
    @patch('neo4j_writer.Consumer')
    @patch('neo4j_writer.Neo4jConnection')
    @patch('neo4j_writer.KnowledgeGraphBuilder')
    def test_process_message_regular_format(self, mock_graph_builder, mock_neo4j_connection, mock_consumer_class):
        """Test message processing with regular format data."""
        mock_consumer_instance = MagicMock()
        mock_consumer_class.return_value = mock_consumer_instance
        
        mock_connection = MagicMock()
        mock_neo4j_connection.return_value = mock_connection
        
        mock_builder_instance = MagicMock()
        mock_graph_builder.return_value = mock_builder_instance
        
        consumer = Neo4jWriterConsumer()
        
        # Create a test message with regular format data
        test_data = {
            "post": {
                "reddit_id": "t3_test123",
                "title": "Test Post",
                "content": "Test content",
                "author": "test_author",
                "subreddit": "test_sub"
            }
        }
        
        mock_msg = MagicMock()
        mock_msg.value.return_value = json.dumps(test_data).encode('utf-8')
        
        # Mock entity extraction
        mock_extractor = MagicMock()
        mock_extractor.extract_from_regular.return_value = (
            [{"id": "post:test123", "type": "Post"}],
            [{"source": "user:test_author", "target": "post:test123", "type": "AUTHORED"}]
        )
        
        with patch.object(consumer, 'entity_extractor', mock_extractor):
            # Process the message
            consumer._process_message(mock_msg)
            
            # Verify entities and relationships were added to graph
            mock_builder_instance.add_entities_and_relationships.assert_called_once()
    
    @patch('neo4j_writer.Consumer')
    @patch('neo4j_writer.Neo4jConnection')
    @patch('neo4j_writer.KnowledgeGraphBuilder')
    def test_process_message_json_error(self, mock_graph_builder, mock_neo4j_connection, mock_consumer_class):
        """Test message processing with JSON decode error."""
        mock_consumer_instance = MagicMock()
        mock_consumer_class.return_value = mock_consumer_instance
        
        mock_connection = MagicMock()
        mock_neo4j_connection.return_value = mock_connection
        
        mock_builder_instance = MagicMock()
        mock_graph_builder.return_value = mock_builder_instance
        
        consumer = Neo4jWriterConsumer()
        
        # Create a message with invalid JSON
        mock_msg = MagicMock()
        mock_msg.value.return_value = b'invalid json'
        
        # Process the message (should handle the error gracefully)
        consumer._process_message(mock_msg)
        
        # Verify no entities/relationships were added (due to error)
        mock_builder_instance.add_entities_and_relationships.assert_not_called()


class TestStartNeo4jWriter(unittest.TestCase):
    """Test start_neo4j_writer function."""
    
    def setUp(self):
        """Set up test environment."""
        # Mock environment variables
        self.original_env = {}
        for key in ['KAFKA_BOOTSTRAP_SERVERS', 'KAFKA_TOPIC', 'KAFKA_CONSUMER_GROUP',
                   'KAFKA_AUTO_OFFSET_RESET', 'NEO4J_URI', 'NEO4J_USER', 'NEO4J_PASSWORD']:
            if key in os.environ:
                self.original_env[key] = os.environ[key]
        
        # Set test environment
        os.environ['KAFKA_BOOTSTRAP_SERVERS'] = 'test-server:9092'
        os.environ['KAFKA_TOPIC'] = 'test-topic'
        os.environ['NEO4J_URI'] = 'bolt://test-server:7687'
    
    def tearDown(self):
        """Clean up test environment."""
        # Restore original environment
        for key, value in self.original_env.items():
            os.environ[key] = value
        for key in ['KAFKA_BOOTSTRAP_SERVERS', 'KAFKA_TOPIC', 'KAFKA_CONSUMER_GROUP',
                   'KAFKA_AUTO_OFFSET_RESET', 'NEO4J_URI', 'NEO4J_USER', 'NEO4J_PASSWORD']:
            if key not in self.original_env and key in os.environ:
                del os.environ[key]
    
    @patch('neo4j_writer.Neo4jWriterConsumer')
    def test_start_neo4j_writer(self, mock_consumer_class):
        """Test start_neo4j_writer function."""
        mock_consumer_instance = MagicMock()
        mock_consumer_class.return_value = mock_consumer_instance
        
        consumer = start_neo4j_writer()
        
        self.assertEqual(consumer, mock_consumer_instance)
        mock_consumer_class.assert_called_once()
    
    @patch('neo4j_writer.Neo4jWriterConsumer')
    def test_start_neo4j_writer_with_params(self, mock_consumer_class):
        """Test start_neo4j_writer function with custom parameters."""
        mock_consumer_instance = MagicMock()
        mock_consumer_class.return_value = mock_consumer_instance
        
        consumer = start_neo4j_writer(
            bootstrap_servers='custom-server:9092',
            topic='custom-topic',
            neo4j_uri='bolt://custom-server:7687'
        )
        
        self.assertEqual(consumer, mock_consumer_instance)
        call_args = mock_consumer_class.call_args[1]
        self.assertEqual(call_args['bootstrap_servers'], 'custom-server:9092')
        self.assertEqual(call_args['topic'], 'custom-topic')
        self.assertEqual(call_args['neo4j_uri'], 'bolt://custom-server:7687')


if __name__ == '__main__':
    unittest.main()