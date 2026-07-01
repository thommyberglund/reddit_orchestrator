# Python implementation Dockerfile using uv
FROM ghcr.io/astral-sh/uv:python3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV UV_NO_CACHE_DIR 1

# Set working directory
WORKDIR /app

# Install system dependencies for confluent-kafka
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy pyproject.toml
COPY pyproject.toml .

# Install Python dependencies using uv
RUN uv pip install --system -r pyproject.toml

# Copy application code
COPY . .

# Expose port
EXPOSE 8080

# Set default command
CMD ["python", "-m", "flask", "run", "--host=0.0.0.0", "--port=8080"]
