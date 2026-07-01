"""
Flask API application for Reddit data processing.
Corresponds to Go implementation in main.go and handlers/reddit.go

This application:
1. Receives Reddit JSON data via HTTP POST
2. Extracts post and comment information
3. Converts to JSON-LD format
4. Sends to Kafka topic
"""

import json
import logging
import os
import signal
import sys
from typing import Optional
from flask import Flask, request, jsonify, Response
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add project directory to Python path for imports
project_dir = os.path.dirname(os.path.abspath(__file__))
if project_dir not in sys.path:
    sys.path.insert(0, project_dir)

from models import RedditData
from extractor import extract_post_and_comments
from kafka_producer import get_kafka_producer, KafkaProducer


# Initialize Flask app
app = Flask(__name__)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global Kafka producer
kafka_producer: Optional[KafkaProducer] = None


def get_producer() -> Optional[KafkaProducer]:
    """Get or initialize the Kafka producer."""
    global kafka_producer
    if kafka_producer is None:
        try:
            kafka_producer = get_kafka_producer()
            logger.info("Kafka producer initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            return None
    return kafka_producer


@app.route('/')
def hello_handler() -> Response:
    """
    Root endpoint - corresponds to HelloHandler in Go.
    """
    return jsonify({
        "message": "Reddit Handler API - Python Implementation",
        "status": "running",
        "endpoints": {
            "/api/reddit": "POST - Process Reddit JSON data",
            "/health": "GET - Health check"
        }
    })


@app.route('/api/reddit', methods=['POST'])
def reddit_handler() -> Response:
    """
    Process Reddit JSON data - corresponds to RedditHandler in Go.
    
    This endpoint:
    1. Accepts Reddit JSON data in the request body
    2. Extracts post and comment information
    3. Converts to JSON-LD format
    4. Sends to Kafka topic
    5. Returns the extracted data as JSON
    """
    if request.method != 'POST':
        return jsonify({"error": "Method not allowed"}), 405
    
    logger.info("Received POST request to /api/reddit")
    
    try:
        # Decode JSON data from request
        data = request.get_json(force=True)
        if not data or not isinstance(data, list):
            logger.error("Invalid JSON data: expected array")
            return jsonify({"error": "Invalid JSON format: expected array"}), 400
        
        logger.info("JSON decoded successfully, processing...")
        
        # Extract post and comments
        reddit_data: RedditData = extract_post_and_comments(data)
        
        logger.info(f"Extraction complete: post='{reddit_data.post.title}', "
                   f"author='{reddit_data.post.author}', "
                   f"comments={len(reddit_data.comments)}, "
                   f"users={len(reddit_data.users)}")
        
        # Convert to JSON-LD format
        json_ld_data = reddit_data.to_json_ld()
        
        # Send to Kafka
        producer = get_producer()
        if producer:
            logger.info("Sending to Kafka...")
            success = producer.produce_json_ld(json_ld_data)
            if success:
                logger.info("Successfully sent to Kafka topic")
            else:
                logger.warning("Failed to send to Kafka")
                # Continue processing even if Kafka fails
        else:
            logger.warning("Kafka producer not available, skipping Kafka send")
        
        # Return the regular JSON response
        response_data = reddit_data.to_dict()
        return jsonify(response_data)
        
    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode JSON: {e}")
        return jsonify({"error": f"Invalid JSON: {str(e)}"}), 400
    except Exception as e:
        logger.error(f"Internal server error: {e}", exc_info=True)
        return jsonify({"error": f"Internal server error: {str(e)}"}), 500


@app.route('/health', methods=['GET'])
def health_handler() -> Response:
    """Health check endpoint."""
    producer_status = "available" if get_producer() is not None else "unavailable"
    
    return jsonify({
        "status": "healthy",
        "kafka_producer": producer_status
    })


def graceful_shutdown(signum: int, frame: any) -> None:
    """Handle graceful shutdown."""
    logger.info(f"Received signal {signum}, shutting down...")
    
    global kafka_producer
    if kafka_producer:
        try:
            kafka_producer.close()
        except Exception as e:
            logger.error(f"Error closing Kafka producer: {e}")
    
    logger.info("Shutdown complete")
    sys.exit(0)


def main():
    """Main entry point."""
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)
    
    # Initialize Kafka producer on startup
    get_producer()
    
    # Get configuration from environment variables
    host = os.getenv('HOST', '0.0.0.0')
    port = int(os.getenv('PORT', '8080'))
    debug = os.getenv('FLASK_DEBUG', 'False').lower() == 'true'
    
    # Run Flask app
    logger.info(f"Starting Flask server on {host}:{port}")
    app.run(host=host, port=port, debug=debug)


if __name__ == '__main__':
    main()
