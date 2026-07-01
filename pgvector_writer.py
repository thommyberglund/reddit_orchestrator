"""
PgVector Writer Consumer for Reddit Orchestrator.

Consumes raw Reddit data from Kafka, extracts text content,
generates embeddings using sentence-transformers, and stores both
structured metadata and semantic vectors in PostgreSQL with pgvector extension.

This implements the semantic and metadata storage layer of the
multi-store architecture pattern.
"""

import json
import logging
import os
import sys
import uuid
from datetime import datetime, UTC
from typing import Optional, Dict, Any, List, Tuple

from confluent_kafka import Consumer, KafkaError
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add project directory to Python path for imports
project_dir = os.path.dirname(os.path.abspath(__file__))
if project_dir not in sys.path:
    sys.path.insert(0, project_dir)

# Try to import sentence-transformers for embeddings
try:
    from sentence_transformers import SentenceTransformer
    SENTENCE_TRANSFORMERS_AVAILABLE = True
except ImportError:
    SENTENCE_TRANSFORMERS_AVAILABLE = False
    logging.warning("sentence-transformers not installed. Embeddings will be None.")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PgVectorDB:
    """PostgreSQL with pgvector database handler."""
    
    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None
    ):
        self.host = host or os.getenv('PG_HOST', '192.168.1.1')
        self.port = port or int(os.getenv('PG_PORT', '5432'))
        self.database = database or os.getenv('PG_DATABASE', 'reddit_data')
        self.user = user or os.getenv('PG_USER', 'postgres')
        self.password = password or os.getenv('PG_PASSWORD', 'postgres')
        self.connection = None
        
    def connect(self):
        """Establish database connection."""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            # Enable pgvector extension
            with self.connection.cursor() as cursor:
                cursor.execute("CREATE EXTENSION IF NOT EXISTS vector")
                self.connection.commit()
            logger.info(f"Connected to PostgreSQL at {self.host}:{self.port}/{self.database}")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise
    
    def close(self):
        """Close database connection."""
        if self.connection:
            self.connection.close()
            logger.info("PostgreSQL connection closed")
    
    def initialize_tables(self):
        """Create tables if they don't exist."""
        with self.connection.cursor() as cursor:
            # Posts table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS reddit_posts (
                    id VARCHAR(36) PRIMARY KEY,
                    reddit_id VARCHAR(255) UNIQUE,
                    title TEXT,
                    content TEXT,
                    author VARCHAR(255),
                    subreddit VARCHAR(255),
                    score INTEGER,
                    created_at TIMESTAMP WITH TIME ZONE,
                    processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    metadata JSONB
                )
            """)
            
            # Comments table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS reddit_comments (
                    id VARCHAR(36) PRIMARY KEY,
                    reddit_id VARCHAR(255) UNIQUE,
                    post_id VARCHAR(36) REFERENCES reddit_posts(id),
                    content TEXT,
                    author VARCHAR(255),
                    score INTEGER,
                    depth INTEGER,
                    parent_id VARCHAR(255),
                    created_at TIMESTAMP WITH TIME ZONE,
                    processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    metadata JSONB
                )
            """)
            
            # Users table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS reddit_users (
                    id VARCHAR(36) PRIMARY KEY,
                    username VARCHAR(255) UNIQUE,
                    first_seen TIMESTAMP WITH TIME ZONE,
                    last_seen TIMESTAMP WITH TIME ZONE,
                    metadata JSONB
                )
            """)
            
            # Embeddings table for posts (using pgvector)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS post_embeddings (
                    id VARCHAR(36) PRIMARY KEY,
                    post_id VARCHAR(36) REFERENCES reddit_posts(id) ON DELETE CASCADE,
                    embedding vector(384),
                    model_name VARCHAR(255),
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Embeddings table for comments
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS comment_embeddings (
                    id VARCHAR(36) PRIMARY KEY,
                    comment_id VARCHAR(36) REFERENCES reddit_comments(id) ON DELETE CASCADE,
                    embedding vector(384),
                    model_name VARCHAR(255),
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Semantic search index for posts
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_post_embeddings 
                ON post_embeddings USING ivfflat (embedding vector_l2_ops) 
                WITH (lists = 100)
            """)
            
            # Semantic search index for comments
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_comment_embeddings 
                ON comment_embeddings USING ivfflat (embedding vector_l2_ops) 
                WITH (lists = 100)
            """)
            
            # Processing log table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS processing_log (
                    message_id VARCHAR(36) PRIMARY KEY,
                    source VARCHAR(255),
                    processing_type VARCHAR(255),
                    status VARCHAR(50),
                    error_message TEXT,
                    processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            self.connection.commit()
            logger.info("Database tables initialized")
    
    def store_post(self, post_data: Dict[str, Any], embeddings: Optional[List[float]] = None) -> str:
        """Store a post with optional embeddings.
        
        Returns the actual post ID (UUID) from the database, either newly
        inserted or existing.
        """
        post_id = str(uuid.uuid4())
        reddit_id = post_data.get("reddit_id") or post_data.get("id", "")
        actual_post_id = post_id
        
        with self.connection.cursor() as cursor:
            # Insert post
            cursor.execute("""
                INSERT INTO reddit_posts 
                (id, reddit_id, title, content, author, subreddit, score, created_at, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (reddit_id) DO NOTHING
            """, (
                post_id,
                reddit_id,
                post_data.get("title", ""),
                post_data.get("content", ""),
                post_data.get("author", ""),
                post_data.get("subreddit", ""),
                post_data.get("score", 0),
                post_data.get("created_at", datetime.now(UTC)),
                json.dumps(post_data.get("metadata", {}))
            ))
            
            # If no row was inserted, the post already exists - fetch its ID
            if cursor.rowcount == 0 and reddit_id:
                cursor.execute("SELECT id FROM reddit_posts WHERE reddit_id = %s", (reddit_id,))
                result = cursor.fetchone()
                if result:
                    actual_post_id = result[0]
            
            # Only insert embeddings if we have a valid post_id
            if embeddings and len(embeddings) > 0:
                embedding_id = str(uuid.uuid4())
                cursor.execute("""
                    INSERT INTO post_embeddings 
                    (id, post_id, embedding, model_name)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                """, (
                    embedding_id,
                    actual_post_id,
                    embeddings,
                    "all-MiniLM-L6-v2"
                ))
            
            self.connection.commit()
            return actual_post_id
    
    def store_comment(self, comment_data: Dict[str, Any], embeddings: Optional[List[float]] = None) -> str:
        """Store a comment with optional embeddings.
        
        Returns the actual comment ID (UUID) from the database, either newly
        inserted or existing.
        """
        comment_id = str(uuid.uuid4())
        reddit_id = comment_data.get("reddit_id") or comment_data.get("id", "")
        actual_comment_id = comment_id
        
        with self.connection.cursor() as cursor:
            # Insert comment
            cursor.execute("""
                INSERT INTO reddit_comments 
                (id, reddit_id, post_id, content, author, score, depth, parent_id, created_at, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (reddit_id) DO NOTHING
            """, (
                comment_id,
                reddit_id,
                comment_data.get("post_id", ""),
                comment_data.get("content", ""),
                comment_data.get("author", ""),
                comment_data.get("score", 0),
                comment_data.get("depth", 0),
                comment_data.get("parent_id", ""),
                comment_data.get("created_at", datetime.now(UTC)),
                json.dumps(comment_data.get("metadata", {}))
            ))
            
            # If no row was inserted, the comment already exists - fetch its ID
            if cursor.rowcount == 0 and reddit_id:
                cursor.execute("SELECT id FROM reddit_comments WHERE reddit_id = %s", (reddit_id,))
                result = cursor.fetchone()
                if result:
                    actual_comment_id = result[0]
            
            # Only insert embeddings if we have a valid comment_id
            if embeddings and len(embeddings) > 0:
                embedding_id = str(uuid.uuid4())
                cursor.execute("""
                    INSERT INTO comment_embeddings 
                    (id, comment_id, embedding, model_name)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                """, (
                    embedding_id,
                    actual_comment_id,
                    embeddings,
                    "all-MiniLM-L6-v2"
                ))
            
            self.connection.commit()
            return actual_comment_id
    
    def store_user(self, user_data: Dict[str, Any]) -> str:
        """Store a user."""
        user_id = str(uuid.uuid4())
        username = user_data.get("username", "")
        
        with self.connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO reddit_users 
                (id, username, first_seen, last_seen, metadata)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (username) DO UPDATE 
                SET last_seen = EXCLUDED.last_seen,
                    metadata = EXCLUDED.metadata
            """, (
                user_id,
                username,
                datetime.now(UTC),
                datetime.now(UTC),
                json.dumps(user_data.get("metadata", {}))
            ))
            self.connection.commit()
            return user_id
    
    def log_processing(self, message_id: str, status: str, error: Optional[str] = None):
        """Log processing status."""
        with self.connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO processing_log 
                (message_id, source, processing_type, status, error_message)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (message_id) DO UPDATE 
                SET status = EXCLUDED.status,
                    error_message = EXCLUDED.error_message,
                    processed_at = CURRENT_TIMESTAMP
            """, (
                message_id,
                "kafka",
                "pgvector-writer",
                status,
                error
            ))
            self.connection.commit()


class EmbeddingGenerator:
    """Generates embeddings from text using sentence-transformers."""
    
    def __init__(self, model_name: str = "all-MiniLM-L6-v2"):
        self.model_name = model_name
        self.model = None
        if SENTENCE_TRANSFORMERS_AVAILABLE:
            self.load_model()
    
    def load_model(self):
        """Load the embedding model."""
        try:
            logger.info(f"Loading embedding model: {self.model_name}")
            self.model = SentenceTransformer(self.model_name)
            logger.info(f"Embedding model loaded successfully")
        except Exception as e:
            logger.error(f"Failed to load embedding model: {e}")
            self.model = None
    
    def generate(self, text: str) -> Optional[List[float]]:
        """Generate embeddings for text."""
        if not self.model or not text or not text.strip():
            return None
        try:
            # Truncate very long texts
            max_length = 512
            if len(text) > max_length:
                text = text[:max_length]
            embeddings = self.model.encode(text, convert_to_tensor=False)
            return embeddings.tolist()
        except Exception as e:
            logger.error(f"Failed to generate embeddings: {e}")
            return None


class PgVectorWriterConsumer:
    """Kafka consumer that writes to PostgreSQL with pgvector."""
    
    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        topic: Optional[str] = None,
        group_id: Optional[str] = None,
        auto_offset_reset: Optional[str] = None,
        pg_host: Optional[str] = None,
        pg_port: Optional[int] = None,
        pg_database: Optional[str] = None,
        pg_user: Optional[str] = None,
        pg_password: Optional[str] = None
    ):
        self.bootstrap_servers = bootstrap_servers or os.getenv('KAFKA_BOOTSTRAP_SERVERS', '192.168.1.1:9092')
        self.topic = topic or os.getenv('KAFKA_TOPIC', 'reddit-data')
        self.group_id = group_id or os.getenv('KAFKA_CONSUMER_GROUP', 'pgvector-writer-group')
        self.auto_offset_reset = auto_offset_reset or os.getenv('KAFKA_AUTO_OFFSET_RESET', 'earliest')
        
        self.pg_db = PgVectorDB(pg_host, pg_port, pg_database, pg_user, pg_password)
        self.embedding_generator = EmbeddingGenerator()
        self.consumer = None
        self._running = False
    
    def _initialize_consumer(self):
        """Initialize Kafka consumer."""
        config = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': self.auto_offset_reset,
            'enable.auto.commit': True,
            'session.timeout.ms': 6000
        }
        
        # Add SASL authentication if configured
        if os.getenv('KAFKA_USERNAME') and os.getenv('KAFKA_PASSWORD'):
            config['security.protocol'] = os.getenv('KAFKA_SECURITY_PROTOCOL', 'SASL_PLAINTEXT')
            config['sasl.mechanisms'] = os.getenv('KAFKA_SASL_MECHANISM', 'PLAIN')
            config['sasl.username'] = os.getenv('KAFKA_USERNAME')
            config['sasl.password'] = os.getenv('KAFKA_PASSWORD')
        
        try:
            from confluent_kafka import Consumer
            self.consumer = Consumer(config)
            logger.info(f"Kafka consumer initialized for topic: {self.topic}, group: {self.group_id}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka consumer: {e}")
            raise
    
    def start(self):
        """Start consuming messages."""
        self._running = True
        self._initialize_consumer()
        self.pg_db.connect()
        self.pg_db.initialize_tables()
        
        self.consumer.subscribe([self.topic])
        logger.info(f"PgVector Writer started, consuming from topic: {self.topic}")
        
        try:
            while self._running:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        continue
                
                self._process_message(msg)
                self.consumer.commit(msg)
        
        except KeyboardInterrupt:
            logger.info("PgVector Writer interrupted by user")
        except Exception as e:
            logger.error(f"Error in PgVector Writer: {e}")
        finally:
            self.stop()
    
    def _process_message(self, msg):
        """Process a single Kafka message."""
        try:
            message_value = msg.value().decode('utf-8')
            data = json.loads(message_value)
            message_id = data.get("id") or str(uuid.uuid4())
            
            logger.info(f"Processing message: {message_id}")
            logger.debug(f"Raw message data keys: {list(data.keys())}")
            if "data" in data:
                logger.debug(f"Message has 'data' wrapper with keys: {list(data['data'].keys())}")
            
            # Extract text and generate embeddings
            text_content = self._extract_text(data)
            logger.debug(f"Extracted text length: {len(text_content)} chars")
            embeddings = self.embedding_generator.generate(text_content) if text_content else None
            if embeddings:
                logger.debug(f"Generated embedding vector of length: {len(embeddings)}")
            else:
                logger.debug("No embeddings generated (no text or model not available)")
            
            # Store in PostgreSQL
            self._store_data(data, embeddings)
            
            # Log successful processing
            self.pg_db.log_processing(message_id, "completed")
            logger.info(f"Successfully processed message: {message_id}")
        
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode message as JSON: {e}")
            self.pg_db.log_processing("unknown", "failed", str(e))
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            self.pg_db.log_processing("unknown", "failed", str(e))
    
    def _extract_text(self, data: Dict[str, Any]) -> str:
        """Extract text content from data for embedding generation."""
        parts = []
        
        # Handle message wrapper from Kafka producer
        actual_data = data.get("data", data)
        logger.debug(f"Extracting text from {'wrapped' if 'data' in data else 'direct'} data")
        
        # Extract from JSON-LD format
        if "@graph" in actual_data:
            logger.debug(f"Found @graph with {len(actual_data['@graph'])} items")
            for item in actual_data["@graph"]:
                item_type = item.get("@type", "")
                
                # Handle DataDownload container
                if item_type == "DataDownload":
                    has_part = item.get("hasPart", [])
                    for part_item in has_part:
                        part_type = part_item.get("@type", "")
                        if part_type == "SocialMediaPosting":
                            if part_item.get("headline"):
                                parts.append(part_item["headline"])
                            if part_item.get("text"):
                                parts.append(part_item["text"])
                        elif part_type == "Comment":
                            if part_item.get("text"):
                                parts.append(part_item["text"])
                
                elif item_type == "SocialMediaPosting":
                    if item.get("headline"):
                        parts.append(item["headline"])
                    if item.get("text"):
                        parts.append(item["text"])
                elif item_type == "Comment":
                    if item.get("text"):
                        parts.append(item["text"])
            logger.debug(f"Extracted {len(parts)} text parts from @graph")
        
        # Extract from regular format
        elif "post" in actual_data:
            if actual_data["post"].get("title"):
                parts.append(actual_data["post"]["title"])
            if actual_data["post"].get("content"):
                parts.append(actual_data["post"]["content"])
            for comment in actual_data.get("comments", []):
                if comment.get("content"):
                    parts.append(comment["content"])
            logger.debug(f"Extracted {len(parts)} text parts from regular format")
        
        # Fallback: try the original data (in case it wasn't wrapped)
        if not parts:
            if "@graph" in data:
                logger.debug("Falling back to root-level @graph")
                for item in data["@graph"]:
                    if item.get("@type") == "SocialMediaPosting":
                        if item.get("headline"):
                            parts.append(item["headline"])
                        if item.get("text"):
                            parts.append(item["text"])
                    elif item.get("@type") == "Comment":
                        if item.get("text"):
                            parts.append(item["text"])
            elif "post" in data:
                logger.debug("Falling back to root-level post")
                if data["post"].get("title"):
                    parts.append(data["post"]["title"])
                if data["post"].get("content"):
                    parts.append(data["post"]["content"])
                for comment in data.get("comments", []):
                    if comment.get("content"):
                        parts.append(comment["content"])
        
        # Final fallback: try to extract any text fields
        if not parts and isinstance(data, dict):
            logger.debug("Falling back to generic text extraction")
            for value in data.values():
                if isinstance(value, str) and len(value) > 10:
                    parts.append(value)
        
        logger.debug(f"Total text parts extracted: {len(parts)}")
        return " ".join(parts)[:2000]  # Limit text length
    
    def _store_data(self, data: Dict[str, Any], embeddings: Optional[List[float]]):
        """Store data in PostgreSQL."""
        try:
            # Debug: show actual message structure
            logger.info(f"MESSAGE STRUCTURE - Root keys: {list(data.keys())}")
            
            # Handle message wrapper from Kafka producer
            actual_data = data.get("data", data)
            if "data" in data:
                logger.info(f"MESSAGE STRUCTURE - Wrapped data keys: {list(actual_data.keys())}")
                # Show first level of wrapped data structure
                for key, value in actual_data.items():
                    if isinstance(value, dict):
                        logger.info(f"MESSAGE STRUCTURE - '{key}' is dict with keys: {list(value.keys())[:5]}")
                    elif isinstance(value, list) and len(value) > 0:
                        logger.info(f"MESSAGE STRUCTURE - '{key}' is list with {len(value)} items")
                        if len(value) > 0:
                            logger.info(f"MESSAGE STRUCTURE - first item type: {type(value[0])}, keys: {list(value[0].keys()) if isinstance(value[0], dict) else 'not dict'}")
            else:
                logger.info(f"MESSAGE STRUCTURE - Direct data keys: {list(data.keys())}")
            
            logger.debug(f"Storing data from {'wrapped' if 'data' in data else 'direct'} data")
            
            posts_stored = 0
            comments_stored = 0
            users_stored = 0
            
            # Store from JSON-LD format
            if "@graph" in actual_data:
                logger.info(f"MESSAGE STRUCTURE - @graph has {len(actual_data['@graph'])} items")
                # Log all @type values found
                type_counts = {}
                for item in actual_data["@graph"]:
                    item_type = item.get("@type", "NO_TYPE")
                    type_counts[item_type] = type_counts.get(item_type, 0) + 1
                logger.info(f"MESSAGE STRUCTURE - @type distribution: {type_counts}")
                
                for item in actual_data["@graph"]:
                    item_type = item.get("@type", "")
                    
                    # Handle DataDownload container (common in Reddit JSON-LD)
                    if item_type == "DataDownload":
                        has_part = item.get("hasPart", [])
                        
                        # Collect posts first, then comments
                        posts_to_store = []
                        comments_to_store = []
                        users_to_store = []
                        
                        for part_item in has_part:
                            part_type = part_item.get("@type", "")
                            
                            if part_type == "SocialMediaPosting":
                                posts_to_store.append(part_item)
                            elif part_type == "Comment":
                                comments_to_store.append(part_item)
                            elif part_type == "Person":
                                users_to_store.append(part_item)
                        
                        # Store posts first and get their UUIDs
                        post_uuid = ""
                        if posts_to_store:
                            # For now, assume single post per DataDownload
                            # Store all posts but use the first one for comments
                            for post_item in posts_to_store:
                                post_data = {
                                    "reddit_id": post_item.get("identifier") or post_item.get("@id", ""),
                                    "title": post_item.get("headline", ""),
                                    "content": post_item.get("text", ""),
                                    "author": post_item.get("author", ""),
                                    "score": post_item.get("upvoteCount", 0),
                                    "subreddit": post_item.get("inLanguage", ""),
                                    "metadata": post_item
                                }
                                post_uuid = self.pg_db.store_post(post_data, embeddings)
                                posts_stored += 1
                                logger.debug(f"Stored post: {post_data.get('reddit_id')} -> UUID: {post_uuid}")
                        
                        # Store comments with the post UUID
                        for comment_item in comments_to_store:
                            comment_data = {
                                "reddit_id": comment_item.get("identifier") or comment_item.get("@id", ""),
                                "post_id": post_uuid,  # All comments in same DataDownload belong to the post
                                "content": comment_item.get("text", ""),
                                "author": comment_item.get("author", ""),
                                "score": comment_item.get("upvoteCount", 0),
                                "depth": comment_item.get("depth", 0),
                                "parent_id": comment_item.get("replyTo", {}).get("@id", "") if comment_item.get("replyTo") else "",
                                "metadata": comment_item
                            }
                            self.pg_db.store_comment(comment_data, embeddings)
                            comments_stored += 1
                            logger.debug(f"Stored comment: {comment_data.get('reddit_id')} -> Post UUID: {post_uuid}")
                        
                        # Store users
                        for user_item in users_to_store:
                            user_data = {
                                "username": user_item.get("name", ""),
                                "metadata": user_item
                            }
                            self.pg_db.store_user(user_data)
                            users_stored += 1
                            logger.debug(f"Stored user: {user_data.get('username')}")
                        
                        # Also process top-level items (not in hasPart)
                        continue
                    
                    if item_type == "SocialMediaPosting":
                        post_data = {
                            "reddit_id": item.get("identifier") or item.get("@id", ""),
                            "title": item.get("headline", ""),
                            "content": item.get("text", ""),
                            "author": item.get("author", ""),
                            "score": item.get("upvoteCount", 0),
                            "subreddit": item.get("inLanguage", ""),
                            "metadata": item
                        }
                        self.pg_db.store_post(post_data, embeddings)
                        posts_stored += 1
                        logger.debug(f"Stored post: {post_data.get('reddit_id')}")
                        
                    elif item_type == "Comment":
                        # For top-level comments, try to extract post_id
                        post_id_ref = ""
                        if item.get("isPartOf"):
                            post_id_ref = item["isPartOf"].get("@id", "") if isinstance(item["isPartOf"], dict) else item["isPartOf"]
                        elif item.get("inReplyTo"):
                            post_id_ref = item["inReplyTo"].get("@id", "") if isinstance(item["inReplyTo"], dict) else item["inReplyTo"]
                        
                        comment_data = {
                            "reddit_id": item.get("identifier") or item.get("@id", ""),
                            "post_id": post_id_ref,
                            "content": item.get("text", ""),
                            "author": item.get("author", ""),
                            "score": item.get("upvoteCount", 0),
                            "depth": item.get("depth", 0),
                            "parent_id": item.get("replyTo", {}).get("@id", "") if item.get("replyTo") else "",
                            "metadata": item
                        }
                        self.pg_db.store_comment(comment_data, embeddings)
                        comments_stored += 1
                        logger.debug(f"Stored comment: {comment_data.get('reddit_id')}")
                        
                    elif item_type == "Person":
                        user_data = {
                            "username": item.get("name", ""),
                            "metadata": item
                        }
                        self.pg_db.store_user(user_data)
                        users_stored += 1
                        logger.debug(f"Stored user: {user_data.get('username')}")
                
                logger.debug(f"Stored {posts_stored} posts, {comments_stored} comments, {users_stored} users from @graph")
            
            # Store from regular format
            elif "post" in actual_data:
                self.pg_db.store_post(actual_data["post"])
                posts_stored += 1
                logger.debug(f"Stored post: {actual_data['post'].get('reddit_id', 'unknown')}")
                for comment in actual_data.get("comments", []):
                    self.pg_db.store_comment(comment)
                    comments_stored += 1
                    logger.debug(f"Stored comment: {comment.get('reddit_id', 'unknown')}")
                for user in actual_data.get("users", []):
                    self.pg_db.store_user(user)
                    users_stored += 1
                    logger.debug(f"Stored user: {user.get('username', 'unknown')}")
                logger.debug(f"Stored {posts_stored} posts, {comments_stored} comments, {users_stored} users from regular format")
            
            # Fallback: try the original data (in case it wasn't wrapped)
            if "@graph" not in actual_data and "post" not in actual_data:
                logger.debug("Falling back to root-level data")
                if "@graph" in data:
                    for item in data["@graph"]:
                        if item.get("@type") == "SocialMediaPosting":
                            post_data = {
                                "reddit_id": item.get("identifier") or item.get("@id", ""),
                                "title": item.get("headline", ""),
                                "content": item.get("text", ""),
                                "author": item.get("author", ""),
                                "score": item.get("upvoteCount", 0),
                                "subreddit": item.get("inLanguage", ""),
                                "metadata": item
                            }
                            self.pg_db.store_post(post_data, embeddings)
                            posts_stored += 1
                            
                        elif item.get("@type") == "Comment":
                            # Extract post_id for fallback comments
                            post_id_ref = ""
                            if item.get("isPartOf"):
                                post_id_ref = item["isPartOf"].get("@id", "") if isinstance(item["isPartOf"], dict) else item["isPartOf"]
                            elif item.get("inReplyTo"):
                                post_id_ref = item["inReplyTo"].get("@id", "") if isinstance(item["inReplyTo"], dict) else item["inReplyTo"]
                            
                            comment_data = {
                                "reddit_id": item.get("identifier") or item.get("@id", ""),
                                "post_id": post_id_ref,
                                "content": item.get("text", ""),
                                "author": item.get("author", ""),
                                "score": item.get("upvoteCount", 0),
                                "depth": item.get("depth", 0),
                                "parent_id": item.get("replyTo", {}).get("@id", "") if item.get("replyTo") else "",
                                "metadata": item
                            }
                            self.pg_db.store_comment(comment_data, embeddings)
                            comments_stored += 1
                elif "post" in data:
                    self.pg_db.store_post(data["post"])
                    posts_stored += 1
                    for comment in data.get("comments", []):
                        self.pg_db.store_comment(comment)
                        comments_stored += 1
                    for user in data.get("users", []):
                        self.pg_db.store_user(user)
                        users_stored += 1
                
                logger.debug(f"Stored {posts_stored} posts, {comments_stored} comments, {users_stored} users from fallback")
            
            if posts_stored == 0 and comments_stored == 0 and users_stored == 0:
                logger.warning(f"No data stored from message - no posts, comments, or users found")
        
        except Exception as e:
            logger.error(f"Failed to store data in PostgreSQL: {e}")
            raise
    
    def stop(self):
        """Stop the consumer."""
        self._running = False
        if self.consumer:
            try:
                self.consumer.close()
                logger.info("Kafka consumer closed")
            except Exception as e:
                logger.error(f"Error closing Kafka consumer: {e}")
        if self.pg_db:
            self.pg_db.close()


def start_pgvector_writer(
    bootstrap_servers: Optional[str] = None,
    topic: Optional[str] = None,
    group_id: Optional[str] = None,
    auto_offset_reset: Optional[str] = None,
    pg_host: Optional[str] = None,
    pg_port: Optional[int] = None,
    pg_database: Optional[str] = None,
    pg_user: Optional[str] = None,
    pg_password: Optional[str] = None
):
    """
    Start the PgVector Writer consumer.
    
    All configuration is read from environment variables if not provided as arguments.
    """
    consumer = PgVectorWriterConsumer(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        pg_host=pg_host,
        pg_port=pg_port,
        pg_database=pg_database,
        pg_user=pg_user,
        pg_password=pg_password
    )
    
    return consumer


if __name__ == "__main__":
    print("Starting PgVector Writer Consumer...")
    print("Press Ctrl+C to stop\n")
    
    try:
        consumer = start_pgvector_writer()
        consumer.start()
    except KeyboardInterrupt:
        print("\nStopping PgVector Writer...")
    except Exception as e:
        print(f"Error: {e}")
