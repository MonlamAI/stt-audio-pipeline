#!/bin/bash
# Setup script for AWS SQS queues

set -e

echo "Creating SQS queues for STT audio processing..."

# Create download jobs queue
echo "Creating stt-download-jobs queue..."
aws sqs create-queue \
    --queue-name stt-download-jobs \
    --attributes '{
        "VisibilityTimeout": "600",
        "MessageRetentionPeriod": "1209600",
        "ReceiveMessageWaitTimeSeconds": "20"
    }'

# Create split jobs queue
echo "Creating stt-split-jobs queue..."
aws sqs create-queue \
    --queue-name stt-split-jobs \
    --attributes '{
        "VisibilityTimeout": "900",
        "MessageRetentionPeriod": "1209600",
        "ReceiveMessageWaitTimeSeconds": "20"
    }'

# Get queue URLs
echo ""
echo "Queue URLs (add these to your .env file):"
echo "=========================================="

DOWNLOAD_URL=$(aws sqs get-queue-url --queue-name stt-download-jobs --query 'QueueUrl' --output text)
echo "SQS_DOWNLOAD_QUEUE_URL=$DOWNLOAD_URL"

SPLIT_URL=$(aws sqs get-queue-url --queue-name stt-split-jobs --query 'QueueUrl' --output text)
echo "SQS_SPLIT_QUEUE_URL=$SPLIT_URL"

echo ""
echo "SQS queues created successfully!"
echo ""
echo "Optional: Create dead letter queues for failed jobs"
echo "aws sqs create-queue --queue-name stt-download-jobs-dlq"
echo "aws sqs create-queue --queue-name stt-split-jobs-dlq"
