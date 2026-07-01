#!/usr/bin/env python3
"""
Simple test script to verify the extractor functionality.
Run with: python test_extractor.py
"""

import json
from extractor import extract_post_and_comments
from models import RedditData


def main():
    # Load test data
    with open('test_reddit_data.json', 'r') as f:
        test_data = json.load(f)
    
    print("Testing extractor with sample Reddit data...")
    print("=" * 60)
    
    # Extract data
    reddit_data: RedditData = extract_post_and_comments(test_data)
    
    # Print extracted data
    print("\nExtracted Post:")
    print(f"  Title: {reddit_data.post.title}")
    print(f"  Author: {reddit_data.post.author}")
    print(f"  Content: {reddit_data.post.content}")
    print(f"  Reddit ID: {reddit_data.post.reddit_id}")
    print(f"  Subreddit: {reddit_data.post.subreddit}")
    print(f"  Score: {reddit_data.post.score}")
    
    print(f"\nExtracted {len(reddit_data.comments)} comments:")
    for i, comment in enumerate(reddit_data.comments, 1):
        print(f"  {i}. Author: {comment.author}, Content: {comment.content[:50]}...")
        print(f"     Depth: {comment.depth}, Reddit ID: {comment.reddit_id}, Score: {comment.score}")
    
    print(f"\nExtracted {len(reddit_data.users)} users:")
    for user in reddit_data.users:
        print(f"  - {user.username}")
    
    # Test JSON-LD conversion
    print("\n" + "=" * 60)
    print("JSON-LD Output:")
    print("=" * 60)
    json_ld = reddit_data.to_json_ld()
    print(json.dumps(json_ld, indent=2, ensure_ascii=False))
    
    # Test regular dict conversion
    print("\n" + "=" * 60)
    print("Regular JSON Output:")
    print("=" * 60)
    regular_json = reddit_data.to_dict()
    print(json.dumps(regular_json, indent=2, ensure_ascii=False))
    
    print("\n" + "=" * 60)
    print("All tests passed!")


if __name__ == '__main__':
    main()
