"""
Data models for Reddit data processing.
Corresponds to Go implementation in models/reddit.go
"""

from dataclasses import dataclass, field
from typing import List, Optional
import uuid
from datetime import datetime


@dataclass
class User:
    """Represents a Reddit user"""
    id: Optional[int] = None
    username: str = ""
    
    def to_dict(self) -> dict:
        result = {"username": self.username}
        if self.id is not None:
            result["id"] = self.id
        return result


@dataclass
class Post:
    """Represents a Reddit post"""
    title: str = ""
    author: str = ""
    author_id: Optional[int] = None
    content: str = ""
    reddit_id: Optional[str] = None
    subreddit: Optional[str] = None
    score: Optional[int] = None
    
    def to_dict(self) -> dict:
        result = {
            "title": self.title,
            "author": self.author,
            "content": self.content
        }
        if self.author_id is not None:
            result["author_id"] = self.author_id
        if self.reddit_id:
            result["reddit_id"] = self.reddit_id
        if self.subreddit:
            result["subreddit"] = self.subreddit
        if self.score is not None:
            result["score"] = self.score
        return result


@dataclass
class Comment:
    """Represents a Reddit comment with UUID tracking"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    author: str = ""
    author_id: Optional[int] = None
    content: str = ""
    parent_id: Optional[str] = None
    depth: int = 0
    reddit_id: Optional[str] = None
    post_id: Optional[int] = None
    score: Optional[int] = None
    
    def to_dict(self) -> dict:
        result = {
            "id": self.id,
            "author": self.author,
            "content": self.content,
            "depth": self.depth
        }
        if self.author_id is not None:
            result["author_id"] = self.author_id
        if self.parent_id:
            result["parent_id"] = self.parent_id
        if self.reddit_id:
            result["reddit_id"] = self.reddit_id
        if self.post_id is not None:
            result["post_id"] = self.post_id
        if self.score is not None:
            result["score"] = self.score
        return result


@dataclass
class RedditData:
    """Represents the extracted post and comments"""
    post: Post = field(default_factory=Post)
    comments: List[Comment] = field(default_factory=list)
    users: List[User] = field(default_factory=list)
    
    def to_dict(self) -> dict:
        return {
            "post": self.post.to_dict(),
            "comments": [c.to_dict() for c in self.comments],
            "users": [u.to_dict() for u in self.users]
        }
    
    def to_json_ld(self) -> dict:
        """
        Convert RedditData to JSON-LD format.
        Uses schema.org vocabulary for social media content.
        """
        # Build users as JSON-LD Person objects
        users_ld = []
        user_map = {}
        for user in self.users:
            user_id = f"user:{user.username}"
            user_ld = {
                "@type": "Person",
                "@id": user_id,
                "name": user.username
            }
            if user.id:
                user_ld["identifier"] = str(user.id)
            users_ld.append(user_ld)
            user_map[user.username] = user_id
        
        # Build comments as JSON-LD Comment objects
        comments_ld = []
        for comment in self.comments:
            comment_id = f"comment:{comment.reddit_id or comment.id}"
            comment_ld = {
                "@type": "Comment",
                "@id": comment_id,
                "text": comment.content,
                "author": user_map.get(comment.author, f"user:{comment.author}"),
                "datePublished": datetime.now().isoformat()
            }
            if comment.score:
                comment_ld["upvoteCount"] = comment.score
            if comment.parent_id:
                comment_ld["replyTo"] = {"@id": f"comment:{comment.parent_id}"}
            if comment.depth > 0:
                comment_ld["depth"] = comment.depth
            if comment.reddit_id:
                comment_ld["identifier"] = comment.reddit_id
            comments_ld.append(comment_ld)
        
        # Build post as JSON-LD SocialMediaPosting (subtype of SocialMediaPosting)
        post_id = f"post:{self.post.reddit_id or uuid.uuid4()}"
        post_ld = {
            "@type": "SocialMediaPosting",
            "@id": post_id,
            "headline": self.post.title,
            "text": self.post.content,
            "author": user_map.get(self.post.author, f"user:{self.post.author}"),
            "datePublished": datetime.now().isoformat()
        }
        
        if self.post.subreddit:
            post_ld["inLanguage"] = self.post.subreddit
            # Add subreddit as a ForumPosting context
            post_ld["isPartOf"] = {
                "@type": "ForumPosting",
                "name": self.post.subreddit
            }
        
        if self.post.score:
            post_ld["upvoteCount"] = self.post.score
        
        if self.post.reddit_id:
            post_ld["identifier"] = self.post.reddit_id
        
        # Build the complete JSON-LD graph
        json_ld = {
            "@context": "https://schema.org",
            "@graph": [
                {
                    "@type": "DataDownload",
                    "@id": "",
                    "name": "Reddit Data Extraction",
                    "dateCreated": datetime.now().isoformat(),
                    "hasPart": [
                        post_ld,
                        *comments_ld,
                        *users_ld
                    ]
                }
            ]
        }
        
        return json_ld


@dataclass
class UserStats:
    """Represents statistics for a user"""
    user_id: int = 0
    username: str = ""
    post_count: int = 0
    comment_count: int = 0
    total_score: int = 0
    average_score: float = 0.0
    last_active_at: Optional[str] = None
    joined_at: Optional[str] = None
    
    def to_dict(self) -> dict:
        return {
            "user_id": self.user_id,
            "username": self.username,
            "post_count": self.post_count,
            "comment_count": self.comment_count,
            "total_score": self.total_score,
            "average_score": self.average_score,
            "last_active_at": self.last_active_at,
            "joined_at": self.joined_at
        }


def new_reddit_data() -> RedditData:
    """Create a new RedditData instance"""
    return RedditData()


def new_comment(author: str, content: str, parent_id: str, depth: int) -> Comment:
    """Create a new Comment with generated UUID"""
    return Comment(
        author=author,
        content=content,
        parent_id=parent_id,
        depth=depth
    )


def new_user(username: str) -> User:
    """Create a new User"""
    return User(username=username)
