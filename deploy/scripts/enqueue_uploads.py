#!/usr/bin/env python3
"""
Enqueue MinIO-to-S3 upload jobs.
Lists audio files in MinIO, checks which are already in S3 (raw-audio/ prefix),
and enqueues only the missing ones to the stt-upload-jobs SQS queue.

Usage:
    python enqueue_uploads.py --dry-run                  # check count first
    python enqueue_uploads.py                            # enqueue all
    python enqueue_uploads.py --collection amdo --limit 5000
"""

import os
import sys
import json
import argparse
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


def get_aws_clients():
    """Get AWS S3 and SQS clients."""
    region = os.environ.get('AWS_REGION', 'us-east-1')
    s3 = boto3.client(
        's3',
        region_name=region,
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY')
    )
    sqs = boto3.client(
        'sqs',
        region_name=region,
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY')
    )
    return s3, sqs


def list_minio_audio_files(minio_client, bucket, collection=None, limit=None):
    """List all audio files from MinIO bucket."""
    prefix = f"{collection}/" if collection else ""

    files = []
    paginator = minio_client.get_paginator('list_objects_v2')

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.lower().endswith(('.mp3', '.wav', '.m4a', '.flac', '.ogg')):
                files.append({
                    'key': key,
                    'size': obj['Size'],
                })

            if limit and len(files) >= limit:
                return files

    return files


def list_s3_existing_keys(s3_client, bucket, prefix='raw-audio/'):
    """List all keys already present in S3 under raw-audio/."""
    existing = set()
    paginator = s3_client.get_paginator('list_objects_v2')

    print(f"  Scanning s3://{bucket}/{prefix} for existing files...")
    count = 0

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            # Strip the raw-audio/ prefix so keys match MinIO keys
            key = obj['Key'][len(prefix):]
            existing.add(key)
            count += 1
            if count % 10000 == 0:
                print(f"    ... scanned {count} existing files")

    print(f"  Found {len(existing)} existing files in S3")
    return existing


def enqueue_upload_jobs(sqs_client, queue_url, files, source_bucket):
    """Enqueue upload jobs to SQS in batches of 10."""
    enqueued = 0
    failed = 0

    for i in range(0, len(files), 10):
        batch = files[i:i + 10]
        entries = []

        for j, file in enumerate(batch):
            message = {
                'source_bucket': source_bucket,
                'source_key': file['key'],
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

        # Progress
        total = enqueued + failed
        if total % 1000 < 10:
            print(f"  Enqueued {enqueued} / {total} ...")

    return enqueued, failed


def main():
    parser = argparse.ArgumentParser(description='Enqueue MinIO-to-S3 upload jobs')
    parser.add_argument('--limit', type=int, default=None,
                        help='Maximum number of files to enqueue (default: all)')
    parser.add_argument('--collection', type=str, default=None,
                        help='Collection to process (amdo, kham, utsang)')
    parser.add_argument('--source-bucket', type=str, default='audio',
                        help='MinIO source bucket name')
    parser.add_argument('--output-bucket', type=str, default=None,
                        help='S3 output bucket')
    parser.add_argument('--queue-name', type=str, default=None,
                        help='SQS queue name')
    parser.add_argument('--dry-run', action='store_true',
                        help='List files without enqueuing')
    args = parser.parse_args()

    output_bucket = args.output_bucket or os.environ.get('AWS_S3_BUCKET', 'monlamai-stt-raw-audio')
    queue_name = args.queue_name or os.environ.get('SQS_UPLOAD_QUEUE_NAME', 'stt-upload-jobs')

    print("=" * 60)
    print("MINIO-TO-S3 UPLOAD - BATCH ENQUEUE")
    print("=" * 60)
    print(f"\nConfiguration:")
    print(f"  MinIO bucket:  {args.source_bucket}")
    print(f"  S3 bucket:     {output_bucket}")
    print(f"  Collection:    {args.collection or 'all'}")
    print(f"  Limit:         {args.limit or 'none'}")
    print(f"  Queue:         {queue_name}")
    print(f"  Dry run:       {args.dry_run}")

    # List files from MinIO
    print(f"\nListing files from MinIO...")
    minio_client = get_minio_client()
    minio_files = list_minio_audio_files(minio_client, args.source_bucket,
                                         args.collection, args.limit)
    print(f"Found {len(minio_files)} audio files in MinIO")

    if not minio_files:
        print("No files found!")
        return

    # Check which files already exist in S3
    print(f"\nChecking S3 for existing files...")
    s3_client, sqs_client = get_aws_clients()
    existing_keys = list_s3_existing_keys(s3_client, output_bucket)

    # Filter to only missing files
    missing_files = [f for f in minio_files if f['key'] not in existing_keys]
    already_synced = len(minio_files) - len(missing_files)

    print(f"\nSync status:")
    print(f"  Total in MinIO:   {len(minio_files)}")
    print(f"  Already in S3:    {already_synced}")
    print(f"  Missing (to sync): {len(missing_files)}")

    if not missing_files:
        print("\nAll files already synced to S3!")
        return

    # Show sample
    print(f"\nSample missing files:")
    for f in missing_files[:5]:
        size_mb = f['size'] / (1024 * 1024)
        print(f"  - {f['key']} ({size_mb:.2f} MB)")
    if len(missing_files) > 5:
        print(f"  ... and {len(missing_files) - 5} more")

    # Calculate totals
    total_size = sum(f['size'] for f in missing_files)
    print(f"\nTotal to transfer: {total_size / (1024**3):.2f} GB")

    if args.dry_run:
        print("\nDry run - no jobs enqueued")
        return

    # Get SQS queue URL
    print(f"\nConnecting to SQS...")
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
    print(f"\nEnqueuing {len(missing_files)} upload jobs...")
    enqueued, failed = enqueue_upload_jobs(sqs_client, queue_url, missing_files,
                                           args.source_bucket)

    print(f"\n" + "=" * 60)
    print("ENQUEUE COMPLETE")
    print("=" * 60)
    print(f"\nResults:")
    print(f"  Enqueued: {enqueued}")
    print(f"  Failed:   {failed}")

    # Show queue stats
    attrs = sqs_client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
    )['Attributes']

    print(f"\nQueue status:")
    print(f"  Pending:   {attrs.get('ApproximateNumberOfMessages', 0)}")
    print(f"  In-flight: {attrs.get('ApproximateNumberOfMessagesNotVisible', 0)}")


if __name__ == '__main__':
    main()
