#!/usr/bin/env python3
"""
Upload audio files from local MinIO to AWS S3.
Run this directly on the MinIO server or any machine with direct MinIO access.

Setup:
  pip install boto3

Usage:
  python upload_to_s3.py                              # upload all collections
  python upload_to_s3.py --collection amdo            # one collection
  python upload_to_s3.py --workers 30                 # 30 parallel uploads
  python upload_to_s3.py --limit 100                  # test with 100 files
  python upload_to_s3.py --dry-run                    # just count files
  python upload_to_s3.py --minio-endpoint 192.168.1.10:9000  # direct IP

Environment variables (or edit the defaults below):
  MINIO_ENDPOINT       MinIO endpoint (default: s3.monlam.ai)
  MINIO_ACCESS_KEY     MinIO access key (default: monlam)
  MINIO_SECRET_KEY     MinIO secret key
  MINIO_SECURE         Use HTTPS (default: true)
  AWS_ACCESS_KEY_ID    AWS access key
  AWS_SECRET_ACCESS_KEY AWS secret key
  AWS_REGION           AWS region (default: us-east-1)
"""

import os
import sys
import time
import argparse
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import boto3
    from botocore.config import Config
except ImportError:
    print("ERROR: boto3 not installed. Run: pip install boto3")
    sys.exit(1)

# ============================================================
# CONFIGURATION - Edit these if not using environment variables
# ============================================================
DEFAULTS = {
    'MINIO_ENDPOINT': 's3.monlam.ai',
    'MINIO_ACCESS_KEY': 'monlam',
    'MINIO_SECRET_KEY': '',
    'MINIO_SECURE': 'true',
    'MINIO_VERIFY_SSL': 'false',
    'MINIO_SOURCE_BUCKET': 'audio',
    'AWS_ACCESS_KEY_ID': '',
    'AWS_SECRET_ACCESS_KEY': '',
    'AWS_REGION': 'us-east-1',
    'AWS_S3_BUCKET': 'monlamai-stt-raw-audio',
}


def env(key):
    """Get config from environment or defaults."""
    return os.environ.get(key, DEFAULTS.get(key, ''))


# ============================================================
# Thread-safe progress tracking
# ============================================================
_lock = threading.Lock()
_stats = {
    'uploaded': 0,
    'skipped': 0,
    'failed': 0,
    'bytes': 0,
    'errors': [],
}


def get_minio_client(endpoint_override=None):
    """Create MinIO client."""
    endpoint = endpoint_override or env('MINIO_ENDPOINT')
    secure = env('MINIO_SECURE').lower() == 'true'
    verify_ssl = env('MINIO_VERIFY_SSL').lower() == 'true'

    protocol = "https" if secure else "http"
    if not endpoint.startswith("http"):
        endpoint_url = f"{protocol}://{endpoint}"
    else:
        endpoint_url = endpoint

    return boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=env('MINIO_ACCESS_KEY'),
        aws_secret_access_key=env('MINIO_SECRET_KEY'),
        verify=verify_ssl,
        config=Config(
            signature_version='s3v4',
            max_pool_connections=50,
            retries={'max_attempts': 5, 'mode': 'adaptive'},
            connect_timeout=30,
            read_timeout=120,
        )
    )


def get_s3_client():
    """Create AWS S3 client."""
    return boto3.client(
        's3',
        region_name=env('AWS_REGION'),
        aws_access_key_id=env('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=env('AWS_SECRET_ACCESS_KEY'),
        config=Config(
            max_pool_connections=50,
            retries={'max_attempts': 3},
        )
    )


def list_minio_files(minio_client, bucket, prefix=''):
    """List all audio files in MinIO."""
    files = []
    paginator = minio_client.get_paginator('list_objects_v2')
    extensions = ('.mp3', '.wav', '.m4a', '.flac', '.ogg')

    print(f"  Scanning minio://{bucket}/{prefix}...")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['Key'].lower().endswith(extensions):
                files.append({'key': obj['Key'], 'size': obj['Size']})
        if files and len(files) % 10000 == 0:
            print(f"    Found {len(files):,} so far...")

    return files


def list_s3_existing(s3_client, bucket, prefix='raw-audio/'):
    """List files already in S3."""
    existing = set()
    paginator = s3_client.get_paginator('list_objects_v2')

    print(f"  Scanning s3://{bucket}/{prefix}...")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            # Store without the raw-audio/ prefix to match MinIO keys
            existing.add(obj['Key'][len('raw-audio/'):])
        if existing and len(existing) % 10000 == 0:
            print(f"    Found {len(existing):,} so far...")

    return existing


def upload_one_file(file_info, source_bucket, dest_bucket, minio_endpoint=None):
    """Download from MinIO and upload to S3. Returns True on success."""
    key = file_info['key']
    dest_key = f"raw-audio/{key}"

    try:
        # Create fresh clients per thread for safety
        minio = get_minio_client(minio_endpoint)
        s3 = get_s3_client()

        # Download from MinIO using GET (avoids HEAD/403 issue)
        response = minio.get_object(Bucket=source_bucket, Key=key)
        body = response['Body'].read()

        # Upload to S3
        s3.put_object(Bucket=dest_bucket, Key=dest_key, Body=body)

        with _lock:
            _stats['uploaded'] += 1
            _stats['bytes'] += len(body)

        return True

    except Exception as e:
        with _lock:
            _stats['failed'] += 1
            if len(_stats['errors']) < 20:
                _stats['errors'].append(f"{key}: {str(e)[:100]}")
        return False


def print_progress(total, start_time):
    """Print progress line."""
    with _lock:
        uploaded = _stats['uploaded']
        skipped = _stats['skipped']
        failed = _stats['failed']
        bytes_done = _stats['bytes']

    done = uploaded + skipped + failed
    elapsed = max(time.time() - start_time, 0.1)
    rate = uploaded / (elapsed / 60) if uploaded > 0 else 0
    speed_mb = bytes_done / (1024 * 1024) / elapsed if bytes_done > 0 else 0
    remaining = total - done
    eta_min = remaining / rate if rate > 0 else 0
    eta_hr = eta_min / 60

    pct = done / total * 100 if total > 0 else 0

    print(
        f"\r  [{pct:5.1f}%] {uploaded:,} uploaded | {skipped:,} skipped | {failed:,} failed | "
        f"{rate:.1f} files/min | {speed_mb:.1f} MB/s | "
        f"ETA: {eta_hr:.1f}h   ",
        end='', flush=True
    )


def main():
    parser = argparse.ArgumentParser(
        description='Upload audio files from MinIO to AWS S3',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python upload_to_s3.py --dry-run                    # count files first
  python upload_to_s3.py --collection amdo --limit 10 # test with 10
  python upload_to_s3.py --workers 30                 # parallel uploads
  python upload_to_s3.py --minio-endpoint 192.168.1.10:9000  # direct IP
        """
    )
    parser.add_argument('--collection', type=str, default=None,
                        help='Collection to upload (amdo, kham, utsang, or all)')
    parser.add_argument('--source-bucket', type=str, default=None,
                        help='MinIO source bucket (default: audio)')
    parser.add_argument('--dest-bucket', type=str, default=None,
                        help='S3 destination bucket')
    parser.add_argument('--workers', type=int, default=20,
                        help='Number of parallel upload threads (default: 20)')
    parser.add_argument('--limit', type=int, default=None,
                        help='Limit number of files to upload')
    parser.add_argument('--minio-endpoint', type=str, default=None,
                        help='Override MinIO endpoint (e.g. 192.168.1.10:9000)')
    parser.add_argument('--minio-no-ssl', action='store_true',
                        help='Use HTTP instead of HTTPS for MinIO')
    parser.add_argument('--dry-run', action='store_true',
                        help='Count files without uploading')
    args = parser.parse_args()

    source_bucket = args.source_bucket or env('MINIO_SOURCE_BUCKET') or 'audio'
    dest_bucket = args.dest_bucket or env('AWS_S3_BUCKET')
    prefix = f"{args.collection}/" if args.collection else ""

    # Handle endpoint override
    minio_endpoint = args.minio_endpoint
    if minio_endpoint:
        if args.minio_no_ssl:
            if not minio_endpoint.startswith('http'):
                minio_endpoint = f"http://{minio_endpoint}"
        else:
            if not minio_endpoint.startswith('http'):
                minio_endpoint = f"https://{minio_endpoint}"

    print("=" * 60)
    print("MINIO -> S3 UPLOAD")
    print("=" * 60)
    print(f"  MinIO:   {minio_endpoint or env('MINIO_ENDPOINT')}")
    print(f"  Source:  minio://{source_bucket}/{prefix or '*'}")
    print(f"  Dest:    s3://{dest_bucket}/raw-audio/")
    print(f"  Workers: {args.workers}")
    if args.limit:
        print(f"  Limit:   {args.limit}")

    # List MinIO files
    print(f"\nStep 1: Listing MinIO files...")
    minio_client = get_minio_client(minio_endpoint)
    all_files = list_minio_files(minio_client, source_bucket, prefix)
    total_size_gb = sum(f['size'] for f in all_files) / (1024 ** 3)
    print(f"  Found {len(all_files):,} audio files ({total_size_gb:.1f} GB)")

    if not all_files:
        print("\nNo audio files found!")
        return

    # Check S3 for existing
    print(f"\nStep 2: Checking S3 for already-uploaded files...")
    s3_client = get_s3_client()
    existing = list_s3_existing(s3_client, dest_bucket, f"raw-audio/{prefix}")
    print(f"  Already in S3: {len(existing):,}")

    # Filter
    to_upload = [f for f in all_files if f['key'] not in existing]
    _stats['skipped'] = len(all_files) - len(to_upload)
    upload_size_gb = sum(f['size'] for f in to_upload) / (1024 ** 3)
    print(f"  To upload: {len(to_upload):,} files ({upload_size_gb:.1f} GB)")

    if args.limit:
        to_upload = to_upload[:args.limit]
        upload_size_gb = sum(f['size'] for f in to_upload) / (1024 ** 3)
        print(f"  Limited to: {len(to_upload):,} files ({upload_size_gb:.1f} GB)")

    if not to_upload:
        print("\nAll files already uploaded!")
        return

    if args.dry_run:
        # Show breakdown
        collections = {}
        for f in to_upload:
            col = f['key'].split('/')[0] if '/' in f['key'] else 'root'
            if col not in collections:
                collections[col] = {'count': 0, 'size': 0}
            collections[col]['count'] += 1
            collections[col]['size'] += f['size']

        print(f"\n  Breakdown:")
        for col, info in sorted(collections.items()):
            print(f"    {col}: {info['count']:,} files ({info['size']/(1024**3):.1f} GB)")

        # Estimate
        rate_per_worker = 1.1  # files/min from test
        rate = rate_per_worker * args.workers
        hours = len(to_upload) / rate / 60
        print(f"\n  Estimated time: {hours:.0f} hours ({hours/24:.1f} days) with {args.workers} workers")
        print(f"  Dry run - nothing uploaded")
        return

    # Upload
    print(f"\nStep 3: Uploading {len(to_upload):,} files with {args.workers} workers...")
    print(f"  (Ctrl+C to stop - progress is saved, re-run to resume)\n")
    start_time = time.time()
    total = len(all_files)

    try:
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {}
            for f in to_upload:
                future = executor.submit(
                    upload_one_file, f, source_bucket, dest_bucket, minio_endpoint
                )
                futures[future] = f

            last_print = 0
            for future in as_completed(futures):
                future.result()
                now = time.time()
                if now - last_print > 2:  # Update every 2 sec
                    print_progress(total, start_time)
                    last_print = now

        print_progress(total, start_time)

    except KeyboardInterrupt:
        print("\n\n  Interrupted! Progress saved. Re-run to continue.")

    # Final report
    elapsed = time.time() - start_time
    elapsed_min = elapsed / 60

    print(f"\n\n{'=' * 60}")
    print("UPLOAD COMPLETE")
    print(f"{'=' * 60}")
    print(f"  Uploaded:  {_stats['uploaded']:,}")
    print(f"  Skipped:   {_stats['skipped']:,} (already in S3)")
    print(f"  Failed:    {_stats['failed']:,}")
    print(f"  Data:      {_stats['bytes'] / (1024**3):.2f} GB")
    print(f"  Time:      {elapsed_min:.1f} min")
    if _stats['uploaded'] > 0:
        rate = _stats['uploaded'] / elapsed_min
        speed = _stats['bytes'] / (1024**2) / elapsed
        print(f"  Rate:      {rate:.1f} files/min | {speed:.1f} MB/s")

    if _stats['errors']:
        print(f"\n  Errors ({len(_stats['errors'])}):")
        for err in _stats['errors'][:10]:
            print(f"    - {err}")
        if len(_stats['errors']) > 10:
            print(f"    ... and {len(_stats['errors']) - 10} more")

    if _stats['failed'] > 0:
        print(f"\n  Re-run this script to retry failed files.")


if __name__ == '__main__':
    main()
