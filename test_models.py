#!/usr/bin/env python3
"""
Test suite for models.py - data models and conversion functions.
"""

import json
import unittest
from datetime import datetime
from models import (
    User, Post, Comment, RedditData, UserStats,
    new_reddit_data, new_comment, new_user
)


class TestUser(unittest.TestCase):
    """Test User model."""
    
    def test_user_creation(self):
        """Test User creation and properties."""
        user = User(id=1, username="test_user")
        self.assertEqual(user.username, "test_user")
        self.assertEqual(user.id, 1)
    
    def test_user_to_dict(self):
        """Test User to_dict method."""
        user = User(id=1, username="test_user")
        user_dict = user.to_dict()
        self.assertEqual(user_dict["username"], "test_user")
        self.assertEqual(user_dict["id"], 1)
    
    def test_user_to_dict_no_id(self):
        """Test User to_dict without id."""
        user = User(username="test_user")
        user_dict = user.to_dict()
        self.assertEqual(user_dict["username"], "test_user")
        self.assertNotIn("id", user_dict)


class TestPost(unittest.TestCase):
    """Test Post model."""
    
    def test_post_creation(self):
        """Test Post creation and properties."""
        post = Post(
            title="Test Title",
            author="test_author",
            content="Test content",
            reddit_id="t3_test",
            subreddit="test_sub",
            score=100
        )
        self.assertEqual(post.title, "Test Title")
        self.assertEqual(post.author, "test_author")
        self.assertEqual(post.content, "Test content")
        self.assertEqual(post.reddit_id, "t3_test")
        self.assertEqual(post.subreddit, "test_sub")
        self.assertEqual(post.score, 100)
    
    def test_post_to_dict(self):
        """Test Post to_dict method."""
        post = Post(
            title="Test Title",
            author="test_author",
            content="Test content",
            reddit_id="t3_test",
            subreddit="test_sub",
            score=100
        )
        post_dict = post.to_dict()
        self.assertEqual(post_dict["title"], "Test Title")
        self.assertEqual(post_dict["author"], "test_author")
        self.assertEqual(post_dict["content"], "Test content")
        self.assertEqual(post_dict["reddit_id"], "t3_test")
        self.assertEqual(post_dict["subreddit"], "test_sub")
        self.assertEqual(post_dict["score"], 100)
    
    def test_post_to_dict_optional_fields(self):
        """Test Post to_dict with optional fields missing."""
        post = Post(title="Test Title", author="test_author", content="Test content")
        post_dict = post.to_dict()
        self.assertEqual(post_dict["title"], "Test Title")
        self.assertNotIn("reddit_id", post_dict)
        self.assertNotIn("subreddit", post_dict)
        self.assertNotIn("score", post_dict)


class TestComment(unittest.TestCase):
    """Test Comment model."""
    
    def test_comment_creation(self):
        """Test Comment creation and properties."""
        comment = Comment(
            author="test_author",
            content="Test comment",
            parent_id="parent123",
            depth=1,
            reddit_id="t1_test",
            score=50
        )
        self.assertEqual(comment.author, "test_author")
        self.assertEqual(comment.content, "Test comment")
        self.assertEqual(comment.parent_id, "parent123")
        self.assertEqual(comment.depth, 1)
        self.assertEqual(comment.reddit_id, "t1_test")
        self.assertEqual(comment.score, 50)
        self.assertIsNotNone(comment.id)  # UUID should be generated
    
    def test_comment_to_dict(self):
        """Test Comment to_dict method."""
        comment = Comment(
            author="test_author",
            content="Test comment",
            parent_id="parent123",
            depth=1,
            reddit_id="t1_test",
            score=50
        )
        comment_dict = comment.to_dict()
        self.assertEqual(comment_dict["author"], "test_author")
        self.assertEqual(comment_dict["content"], "Test comment")
        self.assertEqual(comment_dict["parent_id"], "parent123")
        self.assertEqual(comment_dict["depth"], 1)
        self.assertEqual(comment_dict["reddit_id"], "t1_test")
        self.assertEqual(comment_dict["score"], 50)
        self.assertIn("id", comment_dict)


class TestRedditData(unittest.TestCase):
    """Test RedditData model."""
    
    def test_reddit_data_creation(self):
        """Test RedditData creation."""
        reddit_data = RedditData()
        self.assertIsNotNone(reddit_data.post)
        self.assertEqual(len(reddit_data.comments), 0)
        self.assertEqual(len(reddit_data.users), 0)
    
    def test_reddit_data_to_dict(self):
        """Test RedditData to_dict method."""
        reddit_data = RedditData()
        reddit_data.post.title = "Test Post"
        reddit_data.post.author = "test_author"
        
        comment = Comment(author="comment_author", content="Test comment")
        reddit_data.comments.append(comment)
        
        user = User(username="test_user")
        reddit_data.users.append(user)
        
        data_dict = reddit_data.to_dict()
        self.assertEqual(data_dict["post"]["title"], "Test Post")
        self.assertEqual(data_dict["post"]["author"], "test_author")
        self.assertEqual(len(data_dict["comments"]), 1)
        self.assertEqual(len(data_dict["users"]), 1)
    
    def test_reddit_data_to_json_ld(self):
        """Test RedditData to_json_ld method."""
        reddit_data = RedditData()
        reddit_data.post.title = "Test Post"
        reddit_data.post.author = "test_author"
        reddit_data.post.content = "Test content"
        reddit_data.post.reddit_id = "t3_test"
        reddit_data.post.subreddit = "test_sub"
        reddit_data.post.score = 100
        
        comment = Comment(
            author="comment_author",
            content="Test comment",
            reddit_id="t1_test",
            score=50
        )
        reddit_data.comments.append(comment)
        
        user1 = User(username="test_author")
        user2 = User(username="comment_author")
        reddit_data.users = [user1, user2]
        
        json_ld = reddit_data.to_json_ld()
        
        # Verify basic structure
        self.assertIn("@context", json_ld)
        self.assertIn("@graph", json_ld)
        self.assertEqual(json_ld["@context"], "https://schema.org")
        
        # Find the DataDownload item
        data_download = None
        for item in json_ld["@graph"]:
            if item.get("@type") == "DataDownload":
                data_download = item
                break
        
        self.assertIsNotNone(data_download)
        self.assertIn("hasPart", data_download)
        
        # Verify we have the expected items in hasPart
        has_part = data_download["hasPart"]
        post_items = [item for item in has_part if item.get("@type") == "SocialMediaPosting"]
        comment_items = [item for item in has_part if item.get("@type") == "Comment"]
        user_items = [item for item in has_part if item.get("@type") == "Person"]
        
        self.assertEqual(len(post_items), 1)
        self.assertEqual(len(comment_items), 1)
        self.assertEqual(len(user_items), 2)
        
        # Verify post content
        post_item = post_items[0]
        self.assertEqual(post_item["headline"], "Test Post")
        self.assertEqual(post_item["text"], "Test content")
        self.assertEqual(post_item["upvoteCount"], 100)


class TestUserStats(unittest.TestCase):
    """Test UserStats model."""
    
    def test_user_stats_creation(self):
        """Test UserStats creation."""
        stats = UserStats(
            user_id=1,
            username="test_user",
            post_count=5,
            comment_count=10,
            total_score=150,
            average_score=15.0
        )
        self.assertEqual(stats.user_id, 1)
        self.assertEqual(stats.username, "test_user")
        self.assertEqual(stats.post_count, 5)
        self.assertEqual(stats.comment_count, 10)
        self.assertEqual(stats.total_score, 150)
        self.assertEqual(stats.average_score, 15.0)
    
    def test_user_stats_to_dict(self):
        """Test UserStats to_dict method."""
        stats = UserStats(
            user_id=1,
            username="test_user",
            post_count=5,
            comment_count=10,
            total_score=150,
            average_score=15.0
        )
        stats_dict = stats.to_dict()
        self.assertEqual(stats_dict["user_id"], 1)
        self.assertEqual(stats_dict["username"], "test_user")
        self.assertEqual(stats_dict["post_count"], 5)
        self.assertEqual(stats_dict["comment_count"], 10)


class TestFactoryFunctions(unittest.TestCase):
    """Test factory functions."""
    
    def test_new_reddit_data(self):
        """Test new_reddit_data factory function."""
        reddit_data = new_reddit_data()
        self.assertIsInstance(reddit_data, RedditData)
        self.assertIsNotNone(reddit_data.post)
        self.assertEqual(len(reddit_data.comments), 0)
        self.assertEqual(len(reddit_data.users), 0)
    
    def test_new_comment(self):
        """Test new_comment factory function."""
        comment = new_comment("test_author", "Test content", "parent123", 1)
        self.assertIsInstance(comment, Comment)
        self.assertEqual(comment.author, "test_author")
        self.assertEqual(comment.content, "Test content")
        self.assertEqual(comment.parent_id, "parent123")
        self.assertEqual(comment.depth, 1)
        self.assertIsNotNone(comment.id)
    
    def test_new_user(self):
        """Test new_user factory function."""
        user = new_user("test_user")
        self.assertIsInstance(user, User)
        self.assertEqual(user.username, "test_user")


if __name__ == '__main__':
    unittest.main()