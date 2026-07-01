"""
Extraction logic for Reddit JSON data.
Corresponds to Go implementation in models/extractor.go
"""

from typing import List, Dict, Any
from models import RedditData, new_reddit_data, new_comment, new_user


def extract_post_and_comments(data: List[Dict[str, Any]]) -> RedditData:
    """
    Process Reddit JSON data and extract post info and comments.
    Corresponds to ExtractPostAndComments in Go.
    """
    reddit_data = new_reddit_data()
    
    # First pass: extract the post information
    extract_post(data, reddit_data)
    
    # Second pass: extract comments recursively
    extract_comments(data, reddit_data)
    
    # Collect unique users from post and comments
    extract_users(reddit_data)
    
    return reddit_data


def extract_post(data: List[Dict[str, Any]], reddit_data: RedditData) -> None:
    """
    Extract post information from Reddit JSON data.
    Corresponds to extractPost in Go.
    """
    for item in data:
        if item.get("kind") == "Listing":
            listing_data = item.get("data", {})
            children = listing_data.get("children", [])
            
            for child in children:
                if not isinstance(child, dict):
                    continue
                
                if child.get("kind") == "t3":
                    post_data = child.get("data", {})
                    
                    if "title" in post_data:
                        reddit_data.post.title = post_data["title"]
                    if "author" in post_data:
                        reddit_data.post.author = post_data["author"]
                    if "selftext" in post_data:
                        reddit_data.post.content = post_data["selftext"]
                    if "id" in post_data:
                        reddit_data.post.reddit_id = post_data["id"]
                    if "subreddit" in post_data:
                        reddit_data.post.subreddit = post_data["subreddit"]
                    if "score" in post_data:
                        reddit_data.post.score = int(post_data["score"])


def extract_users(reddit_data: RedditData) -> None:
    """
    Collect unique users from post and comments.
    Corresponds to extractUsers in Go.
    """
    user_map: Dict[str, bool] = {}
    
    # Add post author
    if reddit_data.post.author:
        user_map[reddit_data.post.author] = True
    
    # Add comment authors
    for comment in reddit_data.comments:
        if comment.author:
            user_map[comment.author] = True
    
    # Convert map to list of Users
    reddit_data.users = [new_user(username) for username in user_map.keys()]


def extract_comments(data: List[Dict[str, Any]], reddit_data: RedditData) -> None:
    """
    Process all listings for comments recursively.
    Corresponds to extractComments in Go.
    """
    for item in data:
        if item.get("kind") == "Listing":
            listing_data = item.get("data", {})
            children = listing_data.get("children", [])
            process_comments(children, "", 0, reddit_data)


def process_comments(
    children: List[Any], 
    parent_id: str, 
    depth: int, 
    reddit_data: RedditData
) -> None:
    """
    Recursively process comment children.
    Corresponds to processComments in Go.
    """
    for child in children:
        if not isinstance(child, dict):
            continue
        
        if child.get("kind") == "t1":
            comment_data = child.get("data", {})
            
            author = comment_data.get("author", "")
            content = comment_data.get("body", "")
            reddit_id = comment_data.get("id", "")
            score = comment_data.get("score", 0)
            
            comment = new_comment(author, content, parent_id, depth)
            comment.reddit_id = reddit_id
            comment.score = int(score) if score else 0
            reddit_data.comments.append(comment)
            
            # Process replies recursively
            replies = comment_data.get("replies", {})
            if isinstance(replies, dict):
                replies_data = replies.get("data", {})
                if isinstance(replies_data, dict):
                    replies_children = replies_data.get("children", [])
                    if isinstance(replies_children, list):
                        process_comments(
                            replies_children, 
                            comment.id, 
                            depth + 1, 
                            reddit_data
                        )
