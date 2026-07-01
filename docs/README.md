# Reddit Orchestrator - Architecture Documentation

This directory contains Mermaid diagrams documenting the Reddit Orchestrator architecture.

## Files

| File | Description | Diagram Type |
|------|-------------|--------------|
| [architecture.mmd](./architecture.mmd) | System architecture showing all components, external dependencies, storage layers, and event-driven data flow | Flowchart |
| [sequence.mmd](./sequence.mmd) | Component interaction sequence showing the end-to-end asynchronous data processing flow | Sequence Diagram |
| [class.mmd](./class.mmd) | Data models, core components, and database schemas with their relationships | Class Diagram |

## Diagram Types

### 1. System Architecture (architecture.mmd)

Shows the complete multi-store architecture including:

- **External systems**: Reddit API, Kafka Cluster, MinIO Storage, PostgreSQL + pgvector, Neo4j
- **Application components**: Flask API, Extractor, Models, Kafka Producer
- **Event-driven writers**: MinIO Writer, PgVector Writer, Neo4j Writer
- **Storage layers**: 
  - MinIO (Raw Storage) with posts and comments buckets
  - PostgreSQL (Semantic DB) with tables and vector indexes
  - Neo4j (Knowledge Graph) with nodes and relationships
- **Docker containers**: All 4 services (app, minio-writer, pgvector-writer, neo4j-writer)
- **Color-coded by category**: external, application, storage, docker, writers

### 2. Sequence Diagram (sequence.mmd)

Illustrates the asynchronous, event-driven interaction:

1. **Synchronous part**: Client → Flask → Kafka (fast response)
2. **Asynchronous part**: Three parallel paths from Kafka:
   - MinIO Writer → MinIO (raw JSON storage)
   - PgVector Writer → PostgreSQL (metadata + embeddings)
   - Neo4j Writer → Neo4j (knowledge graph)

Key difference from old architecture: Flask returns **immediately** after sending to Kafka, while all writers process messages **in parallel** in the background.

### 3. Class Diagram (class.mmd)

Documents the complete structure:

- **Core Models**: `RedditData`, `Post`, `Comment`, `User`
- **Core Components**: `Extractor`, `KafkaProducer`
- **MinIO Writer**: `KafkaConsumer`, `MinIOStorage`
- **PgVector Writer**: `PgVectorWriterConsumer`, `PgVectorDB`, `EmbeddingGenerator`
- **Neo4j Writer**: `Neo4jWriterConsumer`, `Neo4jConnection`, `EntityExtractor`, `KnowledgeGraphBuilder`
- **PostgreSQL Schema**: Tables for posts, comments, embeddings with vector types
- **Neo4j Schema**: Node types (Post, Comment, User, Subreddit) and relationships (AUTHORED, POSTED_IN, REPLIES_TO)

## Architecture Evolution

### Original (Synchronous)
```
Client → Flask → Extractor → Models → Kafka → MinIO Consumer → MinIO
```
- All processing in one blocking HTTP request
- Client waits for entire pipeline to complete
- Tight coupling between components
- Single path, no parallelism

### New (Event-Driven Multi-Store)
```
Client → Flask → Kafka → Response (FAST)
              ↓
          ┌─── MinIO Writer → MinIO
          ├─── PgVector Writer → PostgreSQL + pgvector
          └─── Neo4j Writer → Neo4j
```
- Flask returns immediately after Kafka send
- Three independent writers consume from same topic
- Parallel processing, decoupled components
- Multiple storage backends for different use cases

## Viewing the Diagrams

### Option 1: GitHub/GitLab
Mermaid diagrams are natively supported by GitHub and GitLab. Simply open the `.mmd` files in your repository viewer.

### Option 2: VS Code
Install the [Mermaid extension](https://marketplace.visualstudio.com/items?itemName=bierner.markdown-mermaid) for VS Code to preview diagrams directly.

### Option 3: Mermaid Live Editor
Copy the contents of any `.mmd` file and paste it into the [Mermaid Live Editor](https://mermaid.live/) to view and edit interactively.

### Option 4: Documentation Tools
Tools like `docsify`, `MkDocs` with Mermaid plugins, or `Obsidian` can render these diagrams directly.

## Usage in Markdown

To include these diagrams in your documentation, use:

```markdown
```mermaid
%%{include: docs/architecture.mmd}
```
```

Or copy the contents directly into your markdown files.

## Storage Layers Overview

The multi-store architecture uses three complementary storage systems:

### 1. MinIO (Raw Storage)
- **Purpose**: Long-term archiving of raw data
- **Data**: Original JSON files, logs
- **Use case**: Audit trail, replay, backup
- **Writer**: `kafka_consumer.py`

### 2. PostgreSQL + pgvector (Semantic Database)
- **Purpose**: Semantic search and structured metadata
- **Data**: Posts, comments, users, vector embeddings
- **Use case**: Similarity search, filtering, analytics
- **Writer**: `pgvector_writer.py`
- **Key features**: 
  - `pgvector` extension for vector similarity search
  - IVFFlat indexes for fast nearest-neighbor queries
  - JSONB for flexible metadata storage

### 3. Neo4j (Knowledge Graph)
- **Purpose**: Relationship queries and graph analytics
- **Data**: Nodes (entities) and relationships
- **Use case**: Traversal queries, recommendation, pattern detection
- **Writer**: `neo4j_writer.py`
- **Key features**:
  - Node types: Post, Comment, User, Subreddit
  - Relationship types: AUTHORED, POSTED_IN, REPLIES_TO
  - MERGE for idempotent writes
  - Constraints and indexes for performance

## Data Flow Details

### Step 1: Ingestion
```
Client POST /api/reddit → Flask API
```
- Flask receives Reddit JSON data
- Validates and parses the request

### Step 2: Processing (Synchronous)
```
Flask → Extractor → Models → Kafka Producer → Kafka
```
- Extractor parses Reddit JSON structure
- Models convert to structured data + JSON-LD
- Kafka Producer sends to `reddit-data` topic
- **Flask returns HTTP 200 immediately** (non-blocking)

### Step 3: Distribution (Asynchronous)
```
Kafka Topic → MinIO Writer
Kafka Topic → PgVector Writer
Kafka Topic → Neo4j Writer
```
- All three writers consume from the same Kafka topic
- Each writer processes messages independently
- Messages are consumed in parallel

### Step 4: Storage
```
MinIO Writer → MinIO (raw JSON)
PgVector Writer → PostgreSQL (metadata + vectors)
Neo4j Writer → Neo4j (nodes + relationships)
```
- Each writer stores data in its optimized format
- All three storage systems contain related but differently-structured data

## Benefits of Multi-Store Architecture

| Benefit | Description |
|---------|-------------|
| **Decoupling** | Each component operates independently |
| **Scalability** | Scale each writer based on its load |
| **Resilience** | Failure in one writer doesn't affect others |
| **Flexibility** | Easy to add new storage backends |
| **Performance** | Parallel processing of data |
| **Specialization** | Each storage optimized for its use case |
| **Redundancy** | Data preserved in multiple formats |

## Trade-offs

| Consideration | Impact |
|---------------|--------|
| **Eventual Consistency** | Data may be out-of-sync temporarily between stores |
| **Complexity** | More moving parts to manage |
| **Resource Usage** | Higher memory/compute for parallel processing |
| **Debugging** | Need to trace data across multiple systems |
| **Schema Evolution** | Changes must be coordinated across all stores |
