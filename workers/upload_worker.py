"""
Upload Worker - Transfers audio files from MinIO to AWS S3.
Polls SQS for upload jobs, downloads from MinIO using get_object (avoids HEAD/403),
and uploads to S3 under the raw-audio/ prefix.

On success: deletes SQS message.
On failure: message returns to queue after visibility timeout and gets retried.
"""

import json
import os
import sys
import time
import tempfile
from pathlib import Path
from datetime import datetime, timezone

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import boto3
from botocore.config import Config
from dotenv import load_dotenv

# Load environment
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))
load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'deploy', '.env'))

from job_queue.sqs_queue import (
    receive_messages,
    delete_message,
    change_message_visibility,
)


# Upload queue name
UPLOAD_QUEUE_NAME = os.environ.get('SQS_UPLOAD_QUEUE_NAME', 'stt-upload-jobs')


def get_minio_client():
    """Get MinIO/S3 client for source downloads."""
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


def get_s3_client():
    """Get AWS S3 client for uploads."""
    return boto3.client(
        's3',
        region_name=os.environ.get('AWS_REGION', 'us-east-1'),
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY')
    )


def get_sqs_client():
    """Get SQS client."""
    return boto3.client(
        'sqs',
        region_name=os.environ.get('AWS_REGION', 'us-east-1'),
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY')
    )


def get_upload_queue_url():
    """Get upload queue URL (auto-creates if needed)."""
    env_url = os.environ.get('SQS_UPLOAD_QUEUE_URL')
    if env_url:
        return env_url

    from job_queue.sqs_queue import get_or_create_queue
    return get_or_create_queue(UPLOAD_QUEUE_NAME)


def download_from_minio(minio_client, bucket, key, local_path):
    """
    Download file from MinIO using get_object (GET request).
    Avoids HEAD request which some MinIO/Cloudflare configs block with 403.
    """
    os.makedirs(os.path.dirname(local_path) or '.', exist_ok=True)

    response = minio_client.get_object(Bucket=bucket, Key=key)
    size = 0
    with open(local_path, 'wb') as f:
        for chunk in response['Body'].iter_chunks(chunk_size=1024 * 1024):
            f.write(chunk)
            size += len(chunk)

    return size


def upload_to_s3(s3_client, local_path, bucket, key):
    """Upload file to AWS S3."""
    s3_client.upload_file(local_path, bucket, key)


def process_upload(job: dict, minio_client, s3_client):
    """
    Download from MinIO and upload to S3.

    Args:
        job: Job dictionary with keys:
            - source_bucket: MinIO bucket name (e.g. 'audio')
            - source_key: MinIO object key (e.g. 'amdo/file.mp3')
        minio_client: MinIO boto3 client
        s3_client: AWS S3 boto3 client
    """
    source_bucket = job['source_bucket']
    source_key = job['source_key']
    output_bucket = os.environ.get('AWS_S3_BUCKET', 'monlamai-stt-raw-audio')
    s3_key = f"raw-audio/{source_key}"

    print(f"  Source: minio://{source_bucket}/{source_key}")
    print(f"  Dest:   s3://{output_bucket}/{s3_key}")

    # Check if already uploaded (idempotency)
    try:
        response = s3_client.list_objects_v2(
            Bucket=output_bucket, Prefix=s3_key, MaxKeys=1
        )
        if response.get('Contents'):
            for obj in response['Contents']:
                if obj['Key'] == s3_key:
                    print(f"  Already exists in S3, skipping")
                    return
    except Exception as e:
        print(f"  Warning: Could not check existing output: {e}")

    with tempfile.TemporaryDirectory() as tmpdir:
        # Download from MinIO
        ext = os.path.splitext(source_key)[1] or '.mp3'
        local_path = os.path.join(tmpdir, f'transfer{ext}')

        print(f"  Downloading from MinIO...")
        size = download_from_minio(minio_client, source_bucket, source_key, local_path)
        size_mb = size / (1024 * 1024)
        print(f"  Downloaded: {size_mb:.2f} MB")

        # Upload to S3
        print(f"  Uploading to S3...")
        upload_to_s3(s3_client, local_path, output_bucket, s3_key)
        print(f"  Uploaded successfully")


def run_upload_worker():
    """
    Main worker loop - continuously poll SQS and process upload jobs.
    """
    print("Starting upload worker...")
    print(f"  Queue: {UPLOAD_QUEUE_NAME}")
    print(f"  MinIO endpoint: {os.environ.get('MINIO_ENDPOINT', 's3.monlam.ai')}")
    print(f"  S3 bucket: {os.environ.get('AWS_S3_BUCKET', 'monlamai-stt-raw-audio')}")

    # Get queue URL
    queue_url = get_upload_queue_url()
    print(f"  Queue URL: {queue_url}")

    # Initialize clients once
    minio_client = get_minio_client()
    s3_client = get_s3_client()

    consecutive_empty = 0
    jobs_processed = 0
    start_time = time.time()

    while True:
        try:
            messages = receive_messages(
                queue_url,
                max_messages=5,
                wait_time_seconds=20,
                visibility_timeout=900  # 15 minutes
            )

            if not messages:
                consecutive_empty += 1
                if consecutive_empty % 10 == 0:
                    elapsed = time.time() - start_time
                    print(f"No messages ({consecutive_empty} empty polls, "
                          f"{jobs_processed} jobs done in {elapsed/60:.1f}m)")
                continue

            consecutive_empty = 0

            for msg in messages:
                job = json.loads(msg['Body'])
                source_key = job.get('source_key', 'unknown')

                print(f"\n{'='*50}")
                print(f"Processing upload: {source_key}")

                try:
                    # Extend visibility for large files
                    change_message_visibility(queue_url, msg['ReceiptHandle'], 1800)

                    process_upload(job, minio_client, s3_client)
                    delete_message(queue_url, msg['ReceiptHandle'])
                    jobs_processed += 1
                    print(f"  SUCCESS (total: {jobs_processed})")

                except Exception as e:
                    print(f"  FAILED - {e}")
                    import traceback
                    traceback.print_exc()
                    # Message will return to queue after visibility timeout

        except KeyboardInterrupt:
            print(f"\nShutting down upload worker... ({jobs_processed} jobs completed)")
            break

        except Exception as e:
            print(f"Worker error: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(5)  # Back off on errors


if __name__ == '__main__':
    run_upload_worker()
