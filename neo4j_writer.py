"""
Neo4j Writer Consumer for Reddit Orchestrator.

Consumes raw Reddit data from Kafka, identifies entities and relationships,
and builds a Knowledge Graph in Neo4j.

This implements the graph/knowledge layer of the multi-store architecture pattern.
"""

import json
import logging
import os
import re
import sys
import uuid
from datetime import datetime, UTC
from typing import Optional, Dict, Any, List, Tuple

from confluent_kafka import Consumer, KafkaError
from neo4j import GraphDatabase
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add project directory to Python path for imports
project_dir = os.path.dirname(os.path.abspath(__file__))
if project_dir not in sys.path:
    sys.path.insert(0, project_dir)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class Neo4jConnection:
    """Neo4j database connection handler."""
    
    def __init__(
        self,
        uri: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None
    ):
        self.uri = uri or os.getenv('NEO4J_URI', 'bolt://192.168.1.1:7687')
        self.user = user or os.getenv('NEO4J_USER', 'neo4j')
        self.password = password or os.getenv('NEO4J_PASSWORD', 'neo4j')
        self.driver = None
    
    def connect(self):
        """Establish Neo4j connection."""
        try:
            self.driver = GraphDatabase.driver(
                self.uri,
                auth=(self.user, self.password)
            )
            # Verify connection
            with self.driver.session() as session:
                result = session.run("RETURN 1")
                result.consume()
            logger.info(f"Connected to Neo4j at {self.uri}")
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j: {e}")
            raise
    
    def close(self):
        """Close Neo4j connection."""
        if self.driver:
            self.driver.close()
            logger.info("Neo4j connection closed")
    
    def execute_query(self, query: str, parameters: Optional[Dict] = None):
        """Execute a Cypher query."""
        if not self.driver:
            self.connect()
        
        with self.driver.session() as session:
            result = session.run(query, parameters or {})
            return result
    
    def execute_write_transaction(self, query: str, parameters: Optional[Dict] = None):
        """Execute a write transaction."""
        if not self.driver:
            self.connect()
        
        with self.driver.session() as session:
            # For neo4j driver 5.x, use execute_write instead of write_transaction
            result = session.execute_write(
                lambda tx: tx.run(query, parameters or {}).consume()
            )
            return result


class EntityExtractor:
    """Extracts entities and relationships from Reddit data."""
    
    def __init__(self):
        # Common patterns for extracting entities
        self.user_pattern = re.compile(r'@(\w+)')
        self.subreddit_pattern = re.compile(r'r/(\w+)')
        self.url_pattern = re.compile(r'https?://\S+')
        self.hashtag_pattern = re.compile(r'#(\w+)')
    
    def extract_from_jsonld(self, jsonld_data: Dict[str, Any]) -> Tuple[List[Dict], List[Dict]]:
        """
        Extract entities and relationships from JSON-LD formatted data.
        
        Returns:
            Tuple of (entities, relationships)
        """
        entities = []
        relationships = []
        
        if "@graph" not in jsonld_data:
            return entities, relationships
        
        post_id = None
        users_map = {}
        
        for item in jsonld_data["@graph"]:
            item_type = item.get("@type", "")
            item_id = item.get("@id", str(uuid.uuid4()))
            
            if item_type == "DataDownload":
                # Root container - extract items from hasPart
                has_part = item.get("hasPart", [])
                for part_item in has_part:
                    part_type = part_item.get("@type", "")
                    part_id = part_item.get("@id", str(uuid.uuid4()))
                    
                    if part_type == "SocialMediaPosting":
                        post_id = part_id
                        post_entity = {
                            "id": post_id,
                            "type": "Post",
                            "title": part_item.get("headline", ""),
                            "content": part_item.get("text", ""),
                            "score": part_item.get("upvoteCount", 0),
                            "subreddit": part_item.get("inLanguage", ""),
                            "created_at": part_item.get("datePublished", datetime.now(UTC).isoformat())
                        }
                        entities.append(post_entity)
                        
                        # Extract subreddit as entity
                        subreddit = part_item.get("inLanguage") or part_item.get("isPartOf", {}).get("name", "")
                        if subreddit:
                            subreddit_entity = {
                                "id": f"subreddit:{subreddit}",
                                "type": "Subreddit",
                                "name": subreddit
                            }
                            entities.append(subreddit_entity)
                            
                            # Create POSTED_IN relationship
                            relationships.append({
                                "source": post_id,
                                "target": f"subreddit:{subreddit}",
                                "type": "POSTED_IN",
                                "properties": {"timestamp": post_entity.get("created_at", "")}
                            })
                    
                    elif part_type == "Comment":
                        # Extract comment entity
                        comment_id = part_id
                        comment_entity = {
                            "id": comment_id,
                            "type": "Comment",
                            "content": part_item.get("text", ""),
                            "score": part_item.get("upvoteCount", 0),
                            "depth": part_item.get("depth", 0),
                            "created_at": part_item.get("datePublished", datetime.now(UTC).isoformat())
                        }
                        entities.append(comment_entity)
                        
                        # Link to parent (post or comment)
                        reply_to = part_item.get("replyTo", {}).get("@id", "")
                        if reply_to:
                            relationships.append({
                                "source": comment_id,
                                "target": reply_to,
                                "type": "REPLIES_TO",
                                "properties": {}
                            })
                    
                    elif part_type == "Person":
                        # Extract user entity
                        username = part_item.get("name", "")
                        if username:
                            user_id = f"user:{username}"
                            user_entity = {
                                "id": user_id,
                                "type": "User",
                                "username": username
                            }
                            users_map[username] = user_id
                            entities.append(user_entity)
                
                # Continue to process top-level items as well
                continue
            elif item_type == "SocialMediaPosting":
                # Extract post entity
                post_id = item_id
                post_entity = {
                    "id": post_id,
                    "type": "Post",
                    "title": item.get("headline", ""),
                    "content": item.get("text", ""),
                    "score": item.get("upvoteCount", 0),
                    "subreddit": item.get("inLanguage", ""),
                    "created_at": item.get("datePublished", datetime.now(UTC).isoformat())
                }
                entities.append(post_entity)
                
                # Extract subreddit as entity
                subreddit = item.get("inLanguage") or item.get("isPartOf", {}).get("name", "")
                if subreddit:
                    subreddit_entity = {
                        "id": f"subreddit:{subreddit}",
                        "type": "Subreddit",
                        "name": subreddit
                    }
                    entities.append(subreddit_entity)
                    
                    # Create POSTED_IN relationship
                    relationships.append({
                        "source": post_id,
                        "target": f"subreddit:{subreddit}",
                        "type": "POSTED_IN",
                        "properties": {"timestamp": post_entity.get("created_at", "")}
                    })
            
            elif item_type == "Comment":
                # Extract comment entity
                comment_id = item_id
                comment_entity = {
                    "id": comment_id,
                    "type": "Comment",
                    "content": item.get("text", ""),
                    "score": item.get("upvoteCount", 0),
                    "depth": item.get("depth", 0),
                    "created_at": item.get("datePublished", datetime.now(UTC).isoformat())
                }
                entities.append(comment_entity)
                
                # Link to parent (post or comment)
                reply_to = item.get("replyTo", {}).get("@id", "")
                if reply_to:
                    if "post:" in reply_to:
                        relationships.append({
                            "source": comment_id,
                            "target": reply_to,
                            "type": "REPLIES_TO",
                            "properties": {}
                        })
                    else:
                        relationships.append({
                            "source": comment_id,
                            "target": reply_to,
                            "type": "REPLIES_TO",
                            "properties": {}
                        })
            
            elif item_type == "Person":
                # Extract user entity
                username = item.get("name", "")
                if username:
                    user_id = f"user:{username}"
                    user_entity = {
                        "id": user_id,
                        "type": "User",
                        "username": username
                    }
                    users_map[username] = user_id
                    entities.append(user_entity)
        
        # Create AUTHORED relationships (link users to their posts/comments)
        for item in jsonld_data["@graph"]:
            item_type = item.get("@type", "")
            item_id = item.get("@id", "")
            author_ref = item.get("author", "")
            
            # Check if this is a DataDownload with hasPart
            if item_type == "DataDownload":
                has_part = item.get("hasPart", [])
                for part_item in has_part:
                    part_type = part_item.get("@type", "")
                    part_id = part_item.get("@id", "")
                    part_author = part_item.get("author", "")
                    
                    if part_author and part_type in ["SocialMediaPosting", "Comment"]:
                        username = part_author.replace("user:", "") if "user:" in part_author else part_author
                        # Auto-create user entity if not already in map
                        if username not in users_map:
                            user_id = f"user:{username}"
                            user_entity = {
                                "id": user_id,
                                "type": "User",
                                "username": username
                            }
                            users_map[username] = user_id
                            entities.append(user_entity)
                        relationships.append({
                            "source": users_map[username],
                            "target": part_id,
                            "type": "AUTHORED",
                            "properties": {"timestamp": part_item.get("datePublished", "")}
                        })
            elif author_ref and item_type in ["SocialMediaPosting", "Comment"]:
                username = author_ref.replace("user:", "") if "user:" in author_ref else author_ref
                # Auto-create user entity if not already in map
                if username not in users_map:
                    user_id = f"user:{username}"
                    user_entity = {
                        "id": user_id,
                        "type": "User",
                        "username": username
                    }
                    users_map[username] = user_id
                    entities.append(user_entity)
                relationships.append({
                    "source": users_map[username],
                    "target": item_id,
                    "type": "AUTHORED",
                    "properties": {"timestamp": item.get("datePublished", "")}
                })
        
        return entities, relationships
    
    def extract_from_regular(self, data: Dict[str, Any]) -> Tuple[List[Dict], List[Dict]]:
        """
        Extract entities and relationships from regular format data.
        
        Returns:
            Tuple of (entities, relationships)
        """
        entities = []
        relationships = []
        
        # Extract post
        if "post" in data:
            post = data["post"]
            post_id = f"post:{post.get('reddit_id', str(uuid.uuid4()))}"
            post_entity = {
                "id": post_id,
                "type": "Post",
                "title": post.get("title", ""),
                "content": post.get("content", ""),
                "score": post.get("score", 0),
                "subreddit": post.get("subreddit", "")
            }
            entities.append(post_entity)
            
            # Extract subreddit
            subreddit = post.get("subreddit", "")
            if subreddit:
                subreddit_entity = {
                    "id": f"subreddit:{subreddit}",
                    "type": "Subreddit",
                    "name": subreddit
                }
                entities.append(subreddit_entity)
                relationships.append({
                    "source": post_id,
                    "target": f"subreddit:{subreddit}",
                    "type": "POSTED_IN",
                    "properties": {}
                })
            
            # Extract post author
            post_author = post.get("author", "")
            if post_author:
                user_id = f"user:{post_author}"
                user_entity = {
                    "id": user_id,
                    "type": "User",
                    "username": post_author
                }
                entities.append(user_entity)
                relationships.append({
                    "source": user_id,
                    "target": post_id,
                    "type": "AUTHORED",
                    "properties": {}
                })
            
            # Process comments
            for comment in data.get("comments", []):
                comment_id = f"comment:{comment.get('reddit_id', str(uuid.uuid4()))}"
                comment_entity = {
                    "id": comment_id,
                    "type": "Comment",
                    "content": comment.get("content", ""),
                    "score": comment.get("score", 0),
                    "depth": comment.get("depth", 0)
                }
                entities.append(comment_entity)
                
                # Link comment to post
                relationships.append({
                    "source": comment_id,
                    "target": post_id,
                    "type": "REPLIES_TO",
                    "properties": {}
                })
                
                # Extract comment author
                comment_author = comment.get("author", "")
                if comment_author:
                    user_id = f"user:{comment_author}"
                    # Check if user already exists
                    if not any(e.get("id") == user_id for e in entities):
                        user_entity = {
                            "id": user_id,
                            "type": "User",
                            "username": comment_author
                        }
                        entities.append(user_entity)
                    relationships.append({
                        "source": user_id,
                        "target": comment_id,
                        "type": "AUTHORED",
                        "properties": {}
                    })
                
                # Link to parent comment if exists
                parent_id = comment.get("parent_id", "")
                if parent_id and parent_id != post.get("reddit_id", ""):
                    relationships.append({
                        "source": comment_id,
                        "target": f"comment:{parent_id}",
                        "type": "REPLIES_TO",
                        "properties": {}
                    })
        
        return entities, relationships


class KnowledgeGraphBuilder:
    """Builds and maintains the Knowledge Graph in Neo4j."""
    
    def __init__(self, neo4j_connection: Neo4jConnection):
        self.neo4j = neo4j_connection
        self.entity_extractor = EntityExtractor()
    
    def create_constraints(self):
        """Create uniqueness constraints for better performance."""
        constraints = [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Post) REQUIRE p.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Comment) REQUIRE c.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (u:User) REQUIRE u.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (s:Subreddit) REQUIRE s.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (u:User) REQUIRE u.username IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (s:Subreddit) REQUIRE s.name IS UNIQUE"
        ]
        
        for constraint in constraints:
            try:
                self.neo4j.execute_write_transaction(constraint)
                logger.info(f"Created constraint: {constraint[:50]}...")
            except Exception as e:
                logger.warning(f"Failed to create constraint: {e}")
    
    def create_indexes(self):
        """Create indexes for faster queries."""
        indexes = [
            "CREATE INDEX IF NOT EXISTS FOR (p:Post) ON (p.title)",
            "CREATE INDEX IF NOT EXISTS FOR (p:Post) ON (p.score)",
            "CREATE INDEX IF NOT EXISTS FOR (c:Comment) ON (c.content)",
            "CREATE INDEX IF NOT EXISTS FOR (u:User) ON (u.username)",
            "CREATE INDEX IF NOT EXISTS FOR (s:Subreddit) ON (s.name)"
        ]
        
        for index in indexes:
            try:
                self.neo4j.execute_write_transaction(index)
                logger.info(f"Created index: {index[:50]}...")
            except Exception as e:
                logger.warning(f"Failed to create index: {e}")
    
    def initialize_graph(self):
        """Initialize the Knowledge Graph with constraints and indexes."""
        logger.info("Initializing Knowledge Graph...")
        self.create_constraints()
        self.create_indexes()
        logger.info("Knowledge Graph initialized")
    
    def add_entities_and_relationships(
        self, 
        entities: List[Dict[str, Any]], 
        relationships: List[Dict[str, Any]]
    ):
        """
        Add entities and relationships to the Knowledge Graph.
        
        Uses MERGE to avoid duplicates.
        """
        if not entities and not relationships:
            return
        
        # Batch process entities by type for efficiency
        posts = [e for e in entities if e.get("type") == "Post"]
        comments = [e for e in entities if e.get("type") == "Comment"]
        users = [e for e in entities if e.get("type") == "User"]
        subreddits = [e for e in entities if e.get("type") == "Subreddit"]
        
        # Build Cypher query for batch insert
        query_parts = []
        params = {}
        
        # Process posts
        for i, post in enumerate(posts):
            param_name = f"post_{i}"
            params[param_name] = post
            query_parts.append(f"""
                MERGE (p{i}:Post {{id: ${param_name}.id}})
                ON CREATE SET 
                    p{i}.title = ${param_name}.title,
                    p{i}.content = ${param_name}.content,
                    p{i}.score = ${param_name}.score,
                    p{i}.created_at = ${param_name}.created_at
                ON MATCH SET 
                    p{i}.title = COALESCE(p{i}.title, ${param_name}.title),
                    p{i}.content = COALESCE(p{i}.content, ${param_name}.content),
                    p{i}.score = COALESCE(p{i}.score, ${param_name}.score)
            """)
        
        # Process comments
        for i, comment in enumerate(comments):
            param_name = f"comment_{i}"
            params[param_name] = comment
            query_parts.append(f"""
                MERGE (c{i}:Comment {{id: ${param_name}.id}})
                ON CREATE SET 
                    c{i}.content = ${param_name}.content,
                    c{i}.score = ${param_name}.score,
                    c{i}.depth = ${param_name}.depth
            """)
        
        # Process users
        for i, user in enumerate(users):
            param_name = f"user_{i}"
            params[param_name] = user
            query_parts.append(f"""
                MERGE (u{i}:User {{id: ${param_name}.id}})
                ON CREATE SET 
                    u{i}.username = ${param_name}.username
            """)
        
        # Process subreddits
        for i, subreddit in enumerate(subreddits):
            param_name = f"subreddit_{i}"
            params[param_name] = subreddit
            query_parts.append(f"""
                MERGE (s{i}:Subreddit {{id: ${param_name}.id}})
                ON CREATE SET 
                    s{i}.name = ${param_name}.name
            """)
        
        # Process relationships
        for i, rel in enumerate(relationships):
            source_id = rel.get("source", "")
            target_id = rel.get("target", "")
            rel_type = rel.get("type", "")
            
            # Find the variable names for source and target
            source_var = self._find_variable_for_id(source_id, entities)
            target_var = self._find_variable_for_id(target_id, entities)
            
            if source_var and target_var:
                query_parts.append(f"""
                    MATCH (source:{self._get_label_for_id(source_id, entities)} {{id: $rel_{i}_source}})
                    MATCH (target:{self._get_label_for_id(target_id, entities)} {{id: $rel_{i}_target}})
                    MERGE (source)-[r{i}:{rel_type}]->(target)
                """)
                params[f"rel_{i}_source"] = source_id
                params[f"rel_{i}_target"] = target_id
        
        # Combine all parts into a single transaction
        full_query = "\n".join(query_parts)
        
        try:
            self.neo4j.execute_write_transaction(full_query, params)
            logger.info(f"Successfully added {len(entities)} entities and {len(relationships)} relationships")
        except Exception as e:
            logger.error(f"Failed to add entities/relationships: {e}")
            # Fallback: add entities and relationships separately
            self._add_entities_and_relationships_fallback(entities, relationships)
    
    def _find_variable_for_id(self, id: str, entities: List[Dict]) -> Optional[str]:
        """Find the variable name for a given entity ID."""
        # This is a simplified approach; in practice, we'd need to track variable names
        # For now, we'll just use a generic approach
        for entity in entities:
            if entity.get("id") == id:
                entity_type = entity.get("type", "").lower()
                return f"{entity_type}_{id.replace(':', '_').replace('-', '_')}"
        return None
    
    def _get_label_for_id(self, id: str, entities: List[Dict]) -> str:
        """Get the Neo4j label for a given entity ID."""
        for entity in entities:
            if entity.get("id") == id:
                return entity.get("type", "Node")
        return "Node"
    
    def _add_entities_and_relationships_fallback(
        self, 
        entities: List[Dict[str, Any]], 
        relationships: List[Dict[str, Any]]
    ):
        """Fallback method to add entities and relationships one by one."""
        for entity in entities:
            self._add_entity_fallback(entity)
        
        for rel in relationships:
            self._add_relationship_fallback(rel)
    
    def _add_entity_fallback(self, entity: Dict[str, Any]):
        """Add a single entity (fallback)."""
        entity_type = entity.get("type", "Node")
        entity_id = entity.get("id", "")
        
        # Build properties
        props = {}
        for key, value in entity.items():
            if key != "id" and key != "type":
                props[key] = value
        
        query = f"""
            MERGE (n:{entity_type} {{id: $id}})
            ON CREATE SET n += $props
        """
        params = {"id": entity_id, "props": props}
        
        try:
            self.neo4j.execute_write_transaction(query, params)
        except Exception as e:
            logger.error(f"Failed to add entity {entity_id}: {e}")
    
    def _add_relationship_fallback(self, relationship: Dict[str, Any]):
        """Add a single relationship (fallback)."""
        source_id = relationship.get("source", "")
        target_id = relationship.get("target", "")
        rel_type = relationship.get("type", "RELATED_TO")
        
        # Determine labels from IDs
        source_label = self._infer_label_from_id(source_id)
        target_label = self._infer_label_from_id(target_id)
        
        query = f"""
            MATCH (source:{source_label} {{id: $source_id}})
            MATCH (target:{target_label} {{id: $target_id}})
            MERGE (source)-[r:{rel_type}]->(target)
        """
        params = {"source_id": source_id, "target_id": target_id}
        
        try:
            self.neo4j.execute_write_transaction(query, params)
        except Exception as e:
            logger.error(f"Failed to add relationship {source_id}->{rel_type}->{target_id}: {e}")
    
    def _infer_label_from_id(self, id: str) -> str:
        """Infer Neo4j label from entity ID prefix."""
        if id.startswith("post:"):
            return "Post"
        elif id.startswith("comment:"):
            return "Comment"
        elif id.startswith("user:"):
            return "User"
        elif id.startswith("subreddit:"):
            return "Subreddit"
        return "Node"


class Neo4jWriterConsumer:
    """Kafka consumer that builds Knowledge Graph in Neo4j."""
    
    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        topic: Optional[str] = None,
        group_id: Optional[str] = None,
        auto_offset_reset: Optional[str] = None,
        neo4j_uri: Optional[str] = None,
        neo4j_user: Optional[str] = None,
        neo4j_password: Optional[str] = None
    ):
        self.bootstrap_servers = bootstrap_servers or os.getenv('KAFKA_BOOTSTRAP_SERVERS', '192.168.1.1:9092')
        self.topic = topic or os.getenv('KAFKA_TOPIC', 'reddit-data')
        self.group_id = group_id or os.getenv('KAFKA_CONSUMER_GROUP', 'neo4j-writer-group')
        self.auto_offset_reset = auto_offset_reset or os.getenv('KAFKA_AUTO_OFFSET_RESET', 'earliest')
        
        self.neo4j_conn = Neo4jConnection(neo4j_uri, neo4j_user, neo4j_password)
        self.graph_builder = None
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
        self.neo4j_conn.connect()
        self.graph_builder = KnowledgeGraphBuilder(self.neo4j_conn)
        self.graph_builder.initialize_graph()
        
        self.consumer.subscribe([self.topic])
        logger.info(f"Neo4j Writer started, consuming from topic: {self.topic}")
        
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
            logger.info("Neo4j Writer interrupted by user")
        except Exception as e:
            logger.error(f"Error in Neo4j Writer: {e}")
        finally:
            self.stop()
    
    def _process_message(self, msg):
        """Process a single Kafka message."""
        try:
            message_value = msg.value().decode('utf-8')
            data = json.loads(message_value)
            message_id = data.get("id") or str(uuid.uuid4())
            
            logger.info(f"Processing message: {message_id}")
            
            # Extract entities and relationships
            entities, relationships = self._extract_entities_and_relationships(data)
            
            if entities or relationships:
                # Add to Knowledge Graph
                self.graph_builder.add_entities_and_relationships(entities, relationships)
                logger.info(f"Added {len(entities)} entities and {len(relationships)} relationships to Knowledge Graph")
            else:
                logger.warning(f"No entities/relationships extracted from message: {message_id}")
        
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode message as JSON: {e}")
        except Exception as e:
            logger.error(f"Error processing message: {e}")
    
    def _extract_entities_and_relationships(
        self, 
        data: Dict[str, Any]
    ) -> Tuple[List[Dict], List[Dict]]:
        """Extract entities and relationships from data."""
        extractor = EntityExtractor()
        
        # Handle message wrapper from Kafka producer
        # The actual data might be in a "data" field
        actual_data = data.get("data", data)
        
        # Try JSON-LD format first
        if "@graph" in actual_data:
            return extractor.extract_from_jsonld(actual_data)
        # Try regular format
        elif "post" in actual_data or "comments" in actual_data:
            return extractor.extract_from_regular(actual_data)
        
        # Also try the original data (in case it's not wrapped)
        if "@graph" in data:
            return extractor.extract_from_jsonld(data)
        elif "post" in data or "comments" in data:
            return extractor.extract_from_regular(data)
        
        # Generic extraction
        return self._generic_extraction(actual_data)
    
    def _generic_extraction(self, data: Dict[str, Any]) -> Tuple[List[Dict], List[Dict]]:
        """Generic entity extraction for unknown formats."""
        entities = []
        relationships = []
        
        # Try to extract basic entities
        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, dict):
                    if "author" in value and "content" in value:
                        # Looks like a post or comment
                        entity_id = f"{key}:{value.get('id', str(uuid.uuid4()))}"
                        entity = {
                            "id": entity_id,
                            "type": key.capitalize(),
                            **value
                        }
                        entities.append(entity)
                elif isinstance(value, list):
                    for i, item in enumerate(value):
                        if isinstance(item, dict):
                            if "author" in item and "content" in item:
                                entity_id = f"{key}_{i}:{item.get('id', str(uuid.uuid4()))}"
                                entity = {
                                    "id": entity_id,
                                    "type": key.capitalize(),
                                    **item
                                }
                                entities.append(entity)
        
        return entities, relationships
    
    def stop(self):
        """Stop the consumer."""
        self._running = False
        if self.consumer:
            try:
                self.consumer.close()
                logger.info("Kafka consumer closed")
            except Exception as e:
                logger.error(f"Error closing Kafka consumer: {e}")
        if self.neo4j_conn:
            self.neo4j_conn.close()


def start_neo4j_writer(
    bootstrap_servers: Optional[str] = None,
    topic: Optional[str] = None,
    group_id: Optional[str] = None,
    auto_offset_reset: Optional[str] = None,
    neo4j_uri: Optional[str] = None,
    neo4j_user: Optional[str] = None,
    neo4j_password: Optional[str] = None
):
    """
    Start the Neo4j Writer consumer.
    
    All configuration is read from environment variables if not provided as arguments.
    """
    consumer = Neo4jWriterConsumer(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        neo4j_uri=neo4j_uri,
        neo4j_user=neo4j_user,
        neo4j_password=neo4j_password
    )
    
    return consumer


if __name__ == "__main__":
    print("Starting Neo4j Writer Consumer...")
    print("Press Ctrl+C to stop\n")
    
    try:
        consumer = start_neo4j_writer()
        consumer.start()
    except KeyboardInterrupt:
        print("\nStopping Neo4j Writer...")
    except Exception as e:
        print(f"Error: {e}")
