"""
Queue module for job scheduling with AWS SQS.
Queues are auto-created if they don't exist.
"""

from .sqs_queue import (
    enqueue_download,
    enqueue_split,
    receive_messages,
    delete_message,
    get_queue_stats,
    get_download_queue_url,
    get_split_queue_url,
)

__all__ = [
    'enqueue_download',
    'enqueue_split',
    'receive_messages',
    'delete_message',
    'get_queue_stats',
    'get_download_queue_url',
    'get_split_queue_url',
]
