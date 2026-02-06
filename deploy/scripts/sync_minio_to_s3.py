#!/usr/bin/env python3
"""
Sync audio files from MinIO (on-premise) to AWS S3.
Run this from a machine with direct access to MinIO for best speed.

Usage:
  python sync_minio_to_s3.py                          # sync all
  python sync_minio_to_s3.py --collection amdo         # sync one collection
  python sync_minio_to_s3.py --workers 20              # 20 parallel transfers
  python sync_minio_to_s3.py --dry-run                 # just count files
"""

import os
import sys
import argparse
import time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import boto3
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

# Thread-safe counters
_lock = threading.Lock()
_stats = {
    'transferred': 0,
    'skipped': 0,
    'failed': 0,
    'bytes_transferred': 0,
}


def get_minio_client():
    """Get MinIO client."""
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
        config=Config(
            signature_version='s3v4',
            max_pool_connections=50,
            retries={'max_attempts': 3}
        )
    )


def get_s3_client():
    """Get AWS S3 client."""
    return boto3.client(
        's3',
        region_name=os.environ.get('AWS_REGION', 'us-east-1'),
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
        config=Config(
            max_pool_connections=50,
            retries={'max_attempts': 3}
        )
    )


def list_minio_files(minio_client, bucket, prefix=''):
    """List all audio files in MinIO."""
    files = []
    paginator = minio_client.get_paginator('list_objects_v2')

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.lower().endswith(('.mp3', '.wav', '.m4a', '.flac', '.ogg')):
                files.append({'key': key, 'size': obj['Size']})

    return files


def list_s3_existing(s3_client, bucket, prefix=''):
    """List existing files in S3 (for skip check)."""
    existing = set()
    paginator = s3_client.get_paginator('list_objects_v2')

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            existing.add(obj['Key'])

    return existing


def transfer_file(minio_client, s3_client, source_bucket, dest_bucket, file_info):
    """Download from MinIO and upload to S3."""
    key = file_info['key']
    size = file_info['size']

    try:
        # Download from MinIO using GET (no HEAD - avoids 403)
        response = minio_client.get_object(Bucket=source_bucket, Key=key)
        body = response['Body'].read()

        # Upload to S3 (same key structure: raw-audio/amdo/file.mp3)
        dest_key = f"raw-audio/{key}"
        s3_client.put_object(
            Bucket=dest_bucket,
            Key=dest_key,
            Body=body,
            ContentType='audio/mpeg'
        )

        with _lock:
            _stats['transferred'] += 1
            _stats['bytes_transferred'] += size

        return True

    except Exception as e:
        with _lock:
            _stats['failed'] += 1
        print(f"  FAILED: {key} - {e}")
        return False


def print_progress(total, start_time):
    """Print current progress."""
    with _lock:
        done = _stats['transferred'] + _stats['skipped']
        failed = _stats['failed']
        bytes_done = _stats['bytes_transferred']

    elapsed = time.time() - start_time
    rate = _stats['transferred'] / max(elapsed / 60, 0.01)
    speed_mb = bytes_done / (1024 * 1024) / max(elapsed, 0.01)
    remaining = (total - done - failed)
    eta_min = remaining / max(rate, 0.01)

    print(
        f"\r  Progress: {done}/{total} "
        f"({_stats['transferred']} new, {_stats['skipped']} skipped, {failed} failed) "
        f"| {rate:.1f} files/min | {speed_mb:.1f} MB/s "
        f"| ETA: {eta_min:.0f} min   ",
        end='', flush=True
    )


def main():
    parser = argparse.ArgumentParser(description='Sync MinIO audio files to AWS S3')
    parser.add_argument('--collection', type=str, default=None,
                        help='Collection to sync (amdo, kham, utsang)')
    parser.add_argument('--source-bucket', type=str, default='audio',
                        help='MinIO source bucket')
    parser.add_argument('--dest-bucket', type=str, default=None,
                        help='S3 destination bucket')
    parser.add_argument('--workers', type=int, default=10,
                        help='Number of parallel transfers')
    parser.add_argument('--limit', type=int, default=None,
                        help='Limit number of files to transfer')
    parser.add_argument('--dry-run', action='store_true',
                        help='Count files without transferring')
    args = parser.parse_args()

    dest_bucket = args.dest_bucket or os.environ.get('AWS_S3_BUCKET', 'monlamai-stt-raw-audio')
    prefix = f"{args.collection}/" if args.collection else ""

    print("=" * 60)
    print("MINIO → S3 SYNC")
    print("=" * 60)
    print(f"  Source: minio://{args.source_bucket}/{prefix}")
    print(f"  Dest:   s3://{dest_bucket}/raw-audio/{prefix}")
    print(f"  Workers: {args.workers}")

    # List MinIO files
    print(f"\nListing MinIO files...")
    minio_client = get_minio_client()
    files = list_minio_files(minio_client, args.source_bucket, prefix)

    if args.limit:
        files = files[:args.limit]

    total_size_gb = sum(f['size'] for f in files) / (1024 ** 3)
    print(f"  Found {len(files):,} files ({total_size_gb:.1f} GB)")

    if not files:
        print("No files to transfer!")
        return

    if args.dry_run:
        # Show breakdown by collection
        collections = {}
        for f in files:
            col = f['key'].split('/')[0] if '/' in f['key'] else 'root'
            if col not in collections:
                collections[col] = {'count': 0, 'size': 0}
            collections[col]['count'] += 1
            collections[col]['size'] += f['size']

        print(f"\n  Breakdown:")
        for col, info in sorted(collections.items()):
            gb = info['size'] / (1024 ** 3)
            print(f"    {col}: {info['count']:,} files ({gb:.1f} GB)")
        print(f"\n  Dry run - no files transferred")
        return

    # Check which files already exist in S3
    print(f"\nChecking existing files in S3...")
    s3_client = get_s3_client()
    existing = list_s3_existing(s3_client, dest_bucket, f"raw-audio/{prefix}")
    print(f"  Found {len(existing):,} already in S3")

    # Filter out already transferred
    to_transfer = []
    for f in files:
        dest_key = f"raw-audio/{f['key']}"
        if dest_key in existing:
            _stats['skipped'] += 1
        else:
            to_transfer.append(f)

    print(f"  Skipping {_stats['skipped']:,}, transferring {len(to_transfer):,}")

    if not to_transfer:
        print("\nAll files already synced!")
        return

    # Transfer files
    print(f"\nTransferring {len(to_transfer):,} files with {args.workers} workers...")
    start_time = time.time()
    total = len(files)

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {}
        for f in to_transfer:
            # Each thread gets its own clients for thread safety
            future = executor.submit(
                transfer_file,
                get_minio_client(),
                get_s3_client(),
                args.source_bucket,
                dest_bucket,
                f
            )
            futures[future] = f

        for future in as_completed(futures):
            future.result()  # raises exceptions
            print_progress(total, start_time)

    elapsed = time.time() - start_time
    print(f"\n\n{'=' * 60}")
    print(f"SYNC COMPLETE")
    print(f"{'=' * 60}")
    print(f"  Transferred: {_stats['transferred']:,}")
    print(f"  Skipped:     {_stats['skipped']:,}")
    print(f"  Failed:      {_stats['failed']:,}")
    print(f"  Data:        {_stats['bytes_transferred'] / (1024**3):.1f} GB")
    print(f"  Time:        {elapsed / 60:.1f} min")
    print(f"  Speed:       {_stats['bytes_transferred'] / (1024**2) / max(elapsed, 1):.1f} MB/s")


if __name__ == '__main__':
    main()
