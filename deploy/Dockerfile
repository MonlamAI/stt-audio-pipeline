# Dockerfile for STT Audio Processing Workers
FROM python:3.10-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    git \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install additional dependencies for workers
RUN pip install --no-cache-dir \
    minio \
    boto3 \
    gdown \
    requests

# Copy application code
COPY . .

# Set Python path
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Default command (overridden by docker-compose)
CMD ["python", "-m", "workers.download_worker"]
