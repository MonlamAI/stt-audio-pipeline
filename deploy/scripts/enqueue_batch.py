#!/usr/bin/env python3
"""
Enqueue batch of audio files from MinIO to SQS for processing.
Usage: python enqueue_batch.py --limit 1000 --collection amdo
"""

import os
import sys
import json
import argparse
import uuid
from datetime import datetime, timezone

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import boto3
from botocore.config import Config
from dotenv import load_dotenv

# Load environment
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))


def get_minio_client():
    """Get MinIO/S3 client for source bucket."""
    endpoint = os.environ.get("MINIO_ENDPOINT", "s3.monlam.ai")
    secure = os.environ.get("MINIO_SECURE", "true").lower() == "true"
    verify_ssl = os.environ.get("MINIO_VERIFY_SSL", "false").lower() == "true"
    
    protocol = "https" if secure else "http"
    endpoint_url = f"{protocol}://{endpoint}"
    
    return boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY"),
        verify=verify_ssl,
        config=Config(signature_version='s3v4')
    )


def get_sqs_client():
    """Get SQS client."""
    return boto3.client(
        'sqs',
        region_name=os.environ.get('AWS_REGION', 'us-east-1'),
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY')
    )


def list_audio_files(minio_client, bucket, collection=None, limit=1000):
    """List audio files from MinIO bucket."""
    prefix = f"{collection}/" if collection else ""
    
    files = []
    paginator = minio_client.get_paginator('list_objects_v2')
    
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            # Filter for audio files
            if key.lower().endswith(('.mp3', '.wav', '.m4a', '.flac', '.ogg')):
                files.append({
                    'key': key,
                    'size': obj['Size'],
                    'last_modified': obj['LastModified'].isoformat()
                })
            
            if len(files) >= limit:
                return files
    
    return files


def enqueue_jobs(sqs_client, queue_url, files, source_bucket, output_bucket,
                 batch_id=None, source_type='minio'):
    """Enqueue split jobs to SQS."""
    batch_id = batch_id or datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')
    
    enqueued = 0
    failed = 0
    
    # Send in batches of 10 (SQS limit)
    for i in range(0, len(files), 10):
        batch = files[i:i+10]
        entries = []
        
        for j, file in enumerate(batch):
            job_id = f"{batch_id}-{enqueued + j:05d}"
            
            # For S3 source, use original_key (without raw-audio/ prefix)
            source_key = file.get('original_key', file['key'])
            
            # Extract collection and filename from key
            parts = source_key.split('/')
            collection = parts[0] if len(parts) > 1 else 'unknown'
            filename = os.path.splitext(parts[-1])[0]
            
            message = {
                'job_id': job_id,
                'source_type': source_type,  # 'minio' or 's3'
                'source_bucket': source_bucket if source_type == 'minio' else output_bucket,
                'source_key': file['key'] if source_type == 's3' else source_key,
                'output_bucket': output_bucket,
                'output_prefix': f"{collection}/{filename}",
                'max_duration': 30,
                'metadata': {
                    'collection': collection,
                    'original_filename': parts[-1],
                    'source_size': file['size'],
                    'batch_id': batch_id,
                    'submitted_at': datetime.now(timezone.utc).isoformat()
                }
            }
            
            entries.append({
                'Id': str(j),
                'MessageBody': json.dumps(message)
            })
        
        try:
            response = sqs_client.send_message_batch(
                QueueUrl=queue_url,
                Entries=entries
            )
            
            enqueued += len(response.get('Successful', []))
            failed += len(response.get('Failed', []))
            
            if response.get('Failed'):
                for f in response['Failed']:
                    print(f"  Failed: {f.get('Message')}")
                    
        except Exception as e:
            print(f"Error sending batch: {e}")
            failed += len(entries)
    
    return enqueued, failed


def get_s3_source_client():
    """Get AWS S3 client for listing source files (when using --source s3)."""
    return boto3.client(
        's3',
        region_name=os.environ.get('AWS_REGION', 'us-east-1'),
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY')
    )


def list_s3_audio_files(s3_client, bucket, collection=None, limit=1000):
    """List audio files from AWS S3 bucket (raw-audio/ prefix)."""
    prefix = f"raw-audio/{collection}/" if collection else "raw-audio/"
    
    files = []
    paginator = s3_client.get_paginator('list_objects_v2')
    
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.lower().endswith(('.mp3', '.wav', '.m4a', '.flac', '.ogg')):
                # Strip "raw-audio/" prefix for the source_key
                files.append({
                    'key': key,
                    'original_key': key[len('raw-audio/'):],  # amdo/file.mp3
                    'size': obj['Size'],
                    'last_modified': obj['LastModified'].isoformat()
                })
            
            if len(files) >= limit:
                return files
    
    return files


def main():
    parser = argparse.ArgumentParser(description='Enqueue audio files for processing')
    parser.add_argument('--limit', type=int, default=1000, help='Maximum number of files to enqueue')
    parser.add_argument('--collection', type=str, default=None, help='Collection to process (amdo, kham, utsang)')
    parser.add_argument('--source-bucket', type=str, default='audio', help='Source bucket name')
    parser.add_argument('--source', type=str, default='minio', choices=['minio', 's3'],
                        help='Source storage: minio (default) or s3 (after sync)')
    parser.add_argument('--output-bucket', type=str, default=None, help='Output S3 bucket')
    parser.add_argument('--queue-name', type=str, default=None, help='SQS queue name')
    parser.add_argument('--dry-run', action='store_true', help='List files without enqueuing')
    args = parser.parse_args()
    
    # Get configuration
    output_bucket = args.output_bucket or os.environ.get('AWS_S3_BUCKET', 'monlamai-stt-raw-audio')
    queue_name = args.queue_name or os.environ.get('SQS_SPLIT_QUEUE_NAME', 'stt-split-jobs')
    
    print("=" * 60)
    print("STT AUDIO PROCESSING - BATCH ENQUEUE")
    print("=" * 60)
    print(f"\nConfiguration:")
    print(f"  Source: {args.source}")
    print(f"  Source bucket: {args.source_bucket if args.source == 'minio' else output_bucket}")
    print(f"  Output bucket: {output_bucket}")
    print(f"  Collection: {args.collection or 'all'}")
    print(f"  Limit: {args.limit}")
    print(f"  Queue: {queue_name}")
    print(f"  Dry run: {args.dry_run}")
    
    # List files from source
    if args.source == 's3':
        print(f"\nListing files from S3 (raw-audio/ prefix)...")
        s3_client = get_s3_source_client()
        files = list_s3_audio_files(s3_client, output_bucket, args.collection, args.limit)
    else:
        print(f"\nListing files from MinIO...")
        minio_client = get_minio_client()
        files = list_audio_files(minio_client, args.source_bucket, args.collection, args.limit)
    
    print(f"Found {len(files)} audio files")
    
    if not files:
        print("No files found!")
        return
    
    # Show sample
    print(f"\nSample files:")
    for f in files[:5]:
        size_mb = f['size'] / (1024 * 1024)
        print(f"  - {f['key']} ({size_mb:.2f} MB)")
    if len(files) > 5:
        print(f"  ... and {len(files) - 5} more")
    
    # Calculate totals
    total_size = sum(f['size'] for f in files)
    print(f"\nTotal size: {total_size / (1024**3):.2f} GB")
    
    if args.dry_run:
        print("\nDry run - no jobs enqueued")
        return
    
    # Get SQS queue URL
    print(f"\nConnecting to SQS...")
    sqs_client = get_sqs_client()
    
    try:
        response = sqs_client.get_queue_url(QueueName=queue_name)
        queue_url = response['QueueUrl']
    except sqs_client.exceptions.QueueDoesNotExist:
        print(f"Queue {queue_name} does not exist. Creating...")
        response = sqs_client.create_queue(
            QueueName=queue_name,
            Attributes={
                'VisibilityTimeout': '900',
                'MessageRetentionPeriod': '1209600',
                'ReceiveMessageWaitTimeSeconds': '20',
            }
        )
        queue_url = response['QueueUrl']
    
    print(f"Queue URL: {queue_url}")
    
    # Enqueue jobs
    print(f"\nEnqueuing {len(files)} jobs...")
    batch_id = datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')
    enqueued, failed = enqueue_jobs(sqs_client, queue_url, files, args.source_bucket,
                                    output_bucket, batch_id, source_type=args.source)
    
    print(f"\n" + "=" * 60)
    print("ENQUEUE COMPLETE")
    print("=" * 60)
    print(f"\nResults:")
    print(f"  Batch ID: {batch_id}")
    print(f"  Enqueued: {enqueued}")
    print(f"  Failed: {failed}")
    
    # Show queue stats
    attrs = sqs_client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
    )['Attributes']
    
    print(f"\nQueue status:")
    print(f"  Pending: {attrs.get('ApproximateNumberOfMessages', 0)}")
    print(f"  In-flight: {attrs.get('ApproximateNumberOfMessagesNotVisible', 0)}")


if __name__ == '__main__':
    main()
