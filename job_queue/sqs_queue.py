"""
AWS SQS Job Queue module for audio processing pipeline.
Handles job coordination between download and split workers.
Queues are auto-created if they don't exist.
"""

import boto3
import json
import os
from typing import List, Dict, Optional

# Initialize SQS client
sqs = boto3.client(
    'sqs',
    region_name=os.environ.get('AWS_REGION', 'us-east-1'),
    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY')
)

# Queue names (used for auto-creation)
DOWNLOAD_QUEUE_NAME = os.environ.get('SQS_DOWNLOAD_QUEUE_NAME', 'stt-download-jobs')
SPLIT_QUEUE_NAME = os.environ.get('SQS_SPLIT_QUEUE_NAME', 'stt-split-jobs')

# Cache for queue URLs
_queue_urls = {}


def get_or_create_queue(queue_name: str) -> str:
    """
    Get queue URL, creating the queue if it doesn't exist.
    
    Args:
        queue_name: Name of the SQS queue
        
    Returns:
        Queue URL
    """
    # Check cache first
    if queue_name in _queue_urls:
        return _queue_urls[queue_name]
    
    try:
        # Try to get existing queue
        response = sqs.get_queue_url(QueueName=queue_name)
        queue_url = response['QueueUrl']
        print(f"Found existing queue: {queue_name}")
    except sqs.exceptions.QueueDoesNotExist:
        # Create queue if it doesn't exist
        print(f"Creating queue: {queue_name}")
        response = sqs.create_queue(
            QueueName=queue_name,
            Attributes={
                'VisibilityTimeout': '900',  # 15 minutes
                'MessageRetentionPeriod': '1209600',  # 14 days
                'ReceiveMessageWaitTimeSeconds': '20',  # Long polling
            }
        )
        queue_url = response['QueueUrl']
        print(f"Created queue: {queue_url}")
    
    # Cache the URL
    _queue_urls[queue_name] = queue_url
    return queue_url


def get_download_queue_url() -> str:
    """Get download queue URL (auto-creates if needed)."""
    # Check environment first for explicit URL
    env_url = os.environ.get('SQS_DOWNLOAD_QUEUE_URL')
    if env_url:
        return env_url
    return get_or_create_queue(DOWNLOAD_QUEUE_NAME)


def get_split_queue_url() -> str:
    """Get split queue URL (auto-creates if needed)."""
    # Check environment first for explicit URL
    env_url = os.environ.get('SQS_SPLIT_QUEUE_URL')
    if env_url:
        return env_url
    return get_or_create_queue(SPLIT_QUEUE_NAME)


# Lazy-initialized queue URLs (for backward compatibility)
# These will be populated on first use
DOWNLOAD_QUEUE_URL = None
SPLIT_QUEUE_URL = None


def _ensure_queue_urls():
    """Initialize queue URLs if not already set."""
    global DOWNLOAD_QUEUE_URL, SPLIT_QUEUE_URL
    if DOWNLOAD_QUEUE_URL is None:
        DOWNLOAD_QUEUE_URL = get_download_queue_url()
    if SPLIT_QUEUE_URL is None:
        SPLIT_QUEUE_URL = get_split_queue_url()


def enqueue_download(
    job_id: str,
    source_type: str,
    source_url: str,
    output_path: str,
    metadata: Optional[Dict] = None
):
    """
    Add a download job to SQS download queue.
    
    Args:
        job_id: Unique identifier for this job
        source_type: Type of source ('youtube', 'gdrive', 's3', 'url')
        source_url: URL or path to download from
        output_path: MinIO path for output (e.g., 'raw-audio/job123.wav')
        metadata: Optional additional metadata
    """
    queue_url = get_download_queue_url()
    
    message = {
        'job_id': job_id,
        'source_type': source_type,
        'source_url': source_url,
        'output_path': output_path,
        'metadata': metadata or {}
    }
    
    response = sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(message),
        MessageGroupId=job_id if _is_fifo_queue(queue_url) else None,
        MessageDeduplicationId=job_id if _is_fifo_queue(queue_url) else None
    )
    
    print(f"Enqueued download job {job_id}: {source_type} -> {output_path}")
    return response.get('MessageId')


def enqueue_split(
    job_id: str,
    raw_audio_path: str,
    output_prefix: str,
    metadata: Optional[Dict] = None
):
    """
    Add a split job to SQS split queue.
    
    Args:
        job_id: Unique identifier for this job
        raw_audio_path: MinIO path to raw audio file
        output_prefix: Prefix for output segment files
        metadata: Optional additional metadata
    """
    queue_url = get_split_queue_url()
    
    message = {
        'job_id': job_id,
        'raw_audio_path': raw_audio_path,
        'output_prefix': output_prefix,
        'metadata': metadata or {}
    }
    
    response = sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(message),
        MessageGroupId=job_id if _is_fifo_queue(queue_url) else None,
        MessageDeduplicationId=job_id if _is_fifo_queue(queue_url) else None
    )
    
    print(f"Enqueued split job {job_id}: {raw_audio_path} -> {output_prefix}_*")
    return response.get('MessageId')


def receive_messages(
    queue_url: str,
    max_messages: int = 10,
    wait_time_seconds: int = 20,
    visibility_timeout: int = 300
) -> List[Dict]:
    """
    Receive messages from SQS queue with long polling.
    
    Args:
        queue_url: SQS queue URL
        max_messages: Maximum number of messages to receive (1-10)
        wait_time_seconds: Long polling wait time (0-20 seconds)
        visibility_timeout: Time before message becomes visible again (seconds)
        
    Returns:
        List of message dictionaries with 'Body', 'ReceiptHandle', etc.
    """
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=min(max_messages, 10),
        WaitTimeSeconds=wait_time_seconds,
        VisibilityTimeout=visibility_timeout,
        AttributeNames=['All'],
        MessageAttributeNames=['All']
    )
    
    return response.get('Messages', [])


def delete_message(queue_url: str, receipt_handle: str):
    """
    Delete a processed message from queue.
    
    Args:
        queue_url: SQS queue URL
        receipt_handle: Receipt handle from received message
    """
    sqs.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle
    )


def change_message_visibility(
    queue_url: str,
    receipt_handle: str,
    visibility_timeout: int
):
    """
    Change visibility timeout of a message (extend processing time).
    
    Args:
        queue_url: SQS queue URL
        receipt_handle: Receipt handle from received message
        visibility_timeout: New visibility timeout in seconds
    """
    sqs.change_message_visibility(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle,
        VisibilityTimeout=visibility_timeout
    )


def get_queue_stats(queue_url: str) -> Dict:
    """
    Get queue statistics (message counts).
    
    Args:
        queue_url: SQS queue URL
        
    Returns:
        Dict with message counts
    """
    response = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=[
            'ApproximateNumberOfMessages',
            'ApproximateNumberOfMessagesNotVisible',
            'ApproximateNumberOfMessagesDelayed'
        ]
    )
    
    attrs = response.get('Attributes', {})
    return {
        'pending': int(attrs.get('ApproximateNumberOfMessages', 0)),
        'in_flight': int(attrs.get('ApproximateNumberOfMessagesNotVisible', 0)),
        'delayed': int(attrs.get('ApproximateNumberOfMessagesDelayed', 0))
    }


def purge_queue(queue_url: str):
    """
    Delete all messages from a queue (use with caution).
    
    Args:
        queue_url: SQS queue URL
    """
    sqs.purge_queue(QueueUrl=queue_url)
    print(f"Purged queue: {queue_url}")


def _is_fifo_queue(queue_url: str) -> bool:
    """Check if queue is a FIFO queue based on URL."""
    return queue_url and queue_url.endswith('.fifo')


def send_batch(queue_url: str, messages: List[Dict]) -> Dict:
    """
    Send multiple messages in a batch (up to 10).
    
    Args:
        queue_url: SQS queue URL
        messages: List of message dictionaries with 'Id' and 'MessageBody'
        
    Returns:
        Response with successful and failed message IDs
    """
    entries = []
    for i, msg in enumerate(messages[:10]):
        entry = {
            'Id': msg.get('Id', str(i)),
            'MessageBody': msg.get('MessageBody', json.dumps(msg))
        }
        if _is_fifo_queue(queue_url):
            entry['MessageGroupId'] = msg.get('MessageGroupId', 'default')
            entry['MessageDeduplicationId'] = msg.get('Id', str(i))
        entries.append(entry)
    
    response = sqs.send_message_batch(
        QueueUrl=queue_url,
        Entries=entries
    )
    
    return {
        'successful': [s['Id'] for s in response.get('Successful', [])],
        'failed': [f['Id'] for f in response.get('Failed', [])]
    }
