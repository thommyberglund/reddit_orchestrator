#!/usr/bin/env python3
"""
Test suite for app.py - Flask API application.
"""

import json
import unittest
from unittest.mock import patch, MagicMock
from app import app, get_producer
from models import RedditData
from extractor import extract_post_and_comments


class TestApp(unittest.TestCase):
    """Test Flask application."""
    
    def setUp(self):
        """Set up test client."""
        self.app = app.test_client()
        self.app.testing = True
    
    def test_hello_handler(self):
        """Test root endpoint."""
        response = self.app.get('/')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertIn("message", data)
        self.assertIn("endpoints", data)
        self.assertEqual(data["message"], "Reddit Handler API - Python Implementation")
    
    def test_health_handler(self):
        """Test health endpoint."""
        response = self.app.get('/health')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertIn("status", data)
        self.assertEqual(data["status"], "healthy")
    
    @patch('app.get_producer')
    def test_reddit_handler_invalid_method(self, mock_get_producer):
        """Test reddit handler with invalid method."""
        response = self.app.get('/api/reddit')
        self.assertEqual(response.status_code, 405)
        data = json.loads(response.data)
        self.assertIn("error", data)
    
    @patch('app.get_producer')
    def test_reddit_handler_invalid_json(self, mock_get_producer):
        """Test reddit handler with invalid JSON."""
        mock_get_producer.return_value = None
        
        response = self.app.post('/api/reddit', data="invalid json", content_type='application/json')
        self.assertEqual(response.status_code, 400)
        data = json.loads(response.data)
        self.assertIn("error", data)
    
    @patch('app.get_producer')
    def test_reddit_handler_invalid_format(self, mock_get_producer):
        """Test reddit handler with invalid data format."""
        mock_get_producer.return_value = None
        
        # Send a dict instead of list
        response = self.app.post('/api/reddit', 
                                data=json.dumps({"not": "a list"}), 
                                content_type='application/json')
        self.assertEqual(response.status_code, 400)
        data = json.loads(response.data)
        self.assertIn("error", data)
    
    @patch('app.get_producer')
    @patch('extractor.extract_post_and_comments')
    def test_reddit_handler_success(self, mock_extract, mock_get_producer):
        """Test reddit handler with valid data."""
        # Mock producer
        mock_producer = MagicMock()
        mock_producer.produce_json_ld.return_value = True
        mock_get_producer.return_value = mock_producer
        
        # Mock extraction
        mock_reddit_data = RedditData()
        mock_reddit_data.post.title = "Test Post"
        mock_reddit_data.post.author = "test_author"
        mock_reddit_data.post.content = "Test content"
        mock_reddit_data.post.reddit_id = "t3_test"
        mock_reddit_data.post.subreddit = "test_sub"
        mock_reddit_data.post.score = 100
        
        mock_extract.return_value = mock_reddit_data
        
        # Test data
        test_data = [
            {
                "kind": "Listing",
                "data": {
                    "children": [
                        {
                            "kind": "t3",
                            "data": {
                                "title": "Test Post",
                                "author": "test_author",
                                "selftext": "Test content",
                                "id": "t3_test",
                                "subreddit": "test_sub",
                                "score": 100
                            }
                        }
                    ]
                }
            }
        ]
        
        response = self.app.post('/api/reddit', 
                                data=json.dumps(test_data), 
                                content_type='application/json')
        
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        
        # Verify response structure
        self.assertIn("post", data)
        self.assertIn("comments", data)
        self.assertIn("users", data)
        
        # Verify post data
        self.assertEqual(data["post"]["title"], "Test Post")
        self.assertEqual(data["post"]["author"], "test_author")
        self.assertEqual(data["post"]["content"], "Test content")
        
        # Verify extraction was called
        mock_extract.assert_called_once()
        
        # Verify producer was called
        mock_producer.produce_json_ld.assert_called_once()
    
    @patch('app.get_producer')
    @patch('extractor.extract_post_and_comments')
    def test_reddit_handler_kafka_failure(self, mock_extract, mock_get_producer):
        """Test reddit handler when Kafka fails."""
        # Mock producer that fails
        mock_producer = MagicMock()
        mock_producer.produce_json_ld.return_value = False
        mock_get_producer.return_value = mock_producer
        
        # Mock extraction
        mock_reddit_data = RedditData()
        mock_reddit_data.post.title = "Test Post"
        mock_extract.return_value = mock_reddit_data
        
        test_data = [
            {
                "kind": "Listing",
                "data": {
                    "children": [
                        {
                            "kind": "t3",
                            "data": {
                                "title": "Test Post",
                                "author": "test_author",
                                "selftext": "Test content",
                                "id": "t3_test"
                            }
                        }
                    ]
                }
            }
        ]
        
        response = self.app.post('/api/reddit', 
                                data=json.dumps(test_data), 
                                content_type='application/json')
        
        # Should still return 200 even if Kafka fails
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertIn("post", data)


class TestGetProducer(unittest.TestCase):
    """Test get_producer function."""
    
    @patch('app.get_kafka_producer')
    def test_get_producer_success(self, mock_get_kafka_producer):
        """Test successful producer initialization."""
        mock_producer = MagicMock()
        mock_get_kafka_producer.return_value = mock_producer
        
        result = get_producer()
        self.assertEqual(result, mock_producer)
        mock_get_kafka_producer.assert_called_once()
    
    @patch('app.get_kafka_producer')
    def test_get_producer_failure(self, mock_get_kafka_producer):
        """Test producer initialization failure."""
        mock_get_kafka_producer.side_effect = Exception("Kafka error")
        
        result = get_producer()
        self.assertIsNone(result)


if __name__ == '__main__':
    unittest.main()