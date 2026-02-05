"""
Job Scheduler - Creates jobs from catalog and enqueues them to SQS.
Supports reading from MinIO buckets, catalogs, or spreadsheets.
For Monlam audio data, schedules split jobs directly (files already in MinIO).
"""

import json
import os
import sys
import argparse
from pathlib import Path
from typing import List, Dict, Optional, Generator

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from job_queue.sqs_queue import (
    enqueue_download, 
    enqueue_split, 
    get_queue_stats,
    get_download_queue_url,
    get_split_queue_url,
)
from util.minio_utils import read_json, write_json, list_objects, object_exists, get_minio_client


def load_catalog_from_minio(bucket: str = 'config', object_name: str = 'catalog.json') -> List[Dict]:
    """
    Load job catalog from MinIO.
    
    Expected catalog format:
    {
        "jobs": [
            {
                "job_id": "unique_id",
                "source_type": "youtube|gdrive|url|s3",
                "source_url": "https://..."
            },
            ...
        ]
    }
    
    Args:
        bucket: MinIO bucket containing catalog
        object_name: Object key for catalog JSON
        
    Returns:
        List of job dictionaries
    """
    try:
        catalog = read_json(bucket, object_name)
        return catalog.get('jobs', [])
    except Exception as e:
        print(f"Failed to load catalog from minio://{bucket}/{object_name}: {e}")
        return []


def list_audio_files_from_bucket(
    bucket: str = 'audio',
    collections: List[str] = None,
    extensions: List[str] = None
) -> Generator[Dict, None, None]:
    """
    List audio files directly from MinIO bucket (for Monlam audio data).
    
    Args:
        bucket: Source bucket name (default: 'audio')
        collections: List of collection prefixes to include (e.g., ['amdo', 'kham'])
                    If None, lists all files
        extensions: File extensions to include (default: ['.mp3', '.wav'])
        
    Yields:
        Dict with job_id, source_path, collection info
    """
    if extensions is None:
        extensions = ['.mp3', '.wav', '.m4a', '.ogg', '.flac']
    
    client = get_minio_client()
    
    # If specific collections, iterate through each
    if collections:
        prefixes = [f"{c}/" for c in collections]
    else:
        # List all top-level prefixes (collections)
        prefixes = ['']
    
    for prefix in prefixes:
        print(f"Scanning bucket '{bucket}' with prefix '{prefix}'...")
        
        try:
            objects = client.list_objects(bucket, prefix=prefix, recursive=True)
            
            for obj in objects:
                # Skip directories
                if obj.is_dir:
                    continue
                
                # Check extension
                key = obj.object_name
                if not any(key.lower().endswith(ext) for ext in extensions):
                    continue
                
                # Extract collection from path
                parts = key.split('/')
                collection = parts[0] if len(parts) > 1 else 'root'
                
                # Generate job_id from filename (without extension)
                filename = os.path.basename(key)
                job_id = os.path.splitext(filename)[0]
                
                # Clean up job_id (remove special chars that cause issues)
                job_id = job_id.replace('-', '_').replace(' ', '_')
                
                yield {
                    'job_id': f"{collection}_{job_id}",
                    'source_bucket': bucket,
                    'source_key': key,
                    'collection': collection,
                    'filename': filename,
                    'size': obj.size
                }
                
        except Exception as e:
            print(f"Error listing objects with prefix '{prefix}': {e}")


def schedule_split_jobs_from_bucket(
    source_bucket: str = 'audio',
    collections: List[str] = None,
    segments_bucket: str = 'segments',
    skip_existing: bool = True,
    limit: int = None,
    dry_run: bool = False
):
    """
    Schedule split jobs directly for files already in MinIO.
    This is the main function for processing Monlam audio data.
    
    Args:
        source_bucket: Bucket containing source audio files
        collections: Collections to process (None = all)
        segments_bucket: Bucket for output segments
        skip_existing: Skip files that already have segments
        limit: Maximum number of jobs to schedule (None = no limit)
        dry_run: If True, only print what would be done
    """
    total = 0
    enqueued = 0
    skipped = 0
    
    print(f"\nScheduling split jobs from '{source_bucket}'...")
    if collections:
        print(f"Collections: {', '.join(collections)}")
    else:
        print("Collections: ALL")
    
    if dry_run:
        print("DRY RUN - no jobs will be enqueued")
    
    for file_info in list_audio_files_from_bucket(source_bucket, collections):
        total += 1
        
        if limit and enqueued >= limit:
            print(f"\nReached limit of {limit} jobs")
            break
        
        job_id = file_info['job_id']
        source_key = file_info['source_key']
        
        # Check if already processed
        if skip_existing:
            existing = list_objects(segments_bucket, prefix=f"{job_id}_")
            if existing:
                if total % 1000 == 0:
                    print(f"  [{total}] Skipping {job_id} (segments exist)")
                skipped += 1
                continue
        
        if dry_run:
            print(f"  Would enqueue: {job_id} ({source_key})")
        else:
            # Enqueue split job (skip download, file already in MinIO)
            enqueue_split(
                job_id=job_id,
                raw_audio_path=f"{source_bucket}/{source_key}",
                output_prefix=job_id,
                metadata={
                    'collection': file_info['collection'],
                    'original_filename': file_info['filename'],
                    'size': file_info['size']
                }
            )
        
        enqueued += 1
        
        if total % 1000 == 0:
            print(f"  Progress: {total} scanned, {enqueued} enqueued, {skipped} skipped")
    
    print(f"\nScheduling complete:")
    print(f"  Total scanned: {total}")
    print(f"  Enqueued: {enqueued}")
    print(f"  Skipped: {skipped}")


def load_catalog_from_file(filepath: str) -> List[Dict]:
    """
    Load job catalog from local JSON file.
    
    Args:
        filepath: Path to catalog JSON file
        
    Returns:
        List of job dictionaries
    """
    with open(filepath, 'r') as f:
        catalog = json.load(f)
    return catalog.get('jobs', [])


def load_catalog_from_spreadsheet(sheet_id: str, id_col: str, link_col: str, 
                                   from_id: int = None, to_id: int = None) -> List[Dict]:
    """
    Load job catalog from Google Spreadsheet.
    
    Args:
        sheet_id: Google Sheets ID
        id_col: Column name for job ID
        link_col: Column name for source URL
        from_id: Start ID for filtering
        to_id: End ID for filtering
        
    Returns:
        List of job dictionaries
    """
    # Import here to avoid dependency if not used
    sys.path.insert(0, str(Path(__file__).parent.parent / 'util'))
    from google_utils import read_spreadsheet
    
    df = read_spreadsheet(sheet_id)
    jobs = []
    
    for _, row in df.iterrows():
        job_id = str(row[id_col])
        source_url = row[link_col]
        
        # Apply ID range filter if specified
        if from_id is not None or to_id is not None:
            try:
                id_num = int(row.get('Sr.no', row.get('ID', 0)))
                if from_id is not None and id_num < from_id:
                    continue
                if to_id is not None and id_num > to_id:
                    continue
            except (ValueError, TypeError):
                pass
        
        # Skip empty URLs
        if not source_url or str(source_url).lower() == 'nan':
            continue
        
        # Detect source type from URL
        source_type = detect_source_type(source_url)
        
        jobs.append({
            'job_id': job_id,
            'source_type': source_type,
            'source_url': source_url
        })
    
    return jobs


def detect_source_type(url: str) -> str:
    """Detect source type from URL."""
    url_lower = url.lower()
    
    if 'youtube.com' in url_lower or 'youtu.be' in url_lower:
        return 'youtube'
    elif 'drive.google.com' in url_lower:
        return 'gdrive'
    elif url_lower.startswith('s3://'):
        return 's3'
    else:
        return 'url'


def schedule_jobs(jobs: List[Dict], skip_existing: bool = True, batch_size: int = 100):
    """
    Enqueue jobs to SQS download queue.
    
    Args:
        jobs: List of job dictionaries
        skip_existing: Skip jobs that already have raw audio in MinIO
        batch_size: Number of jobs to enqueue in each batch
    """
    total = len(jobs)
    enqueued = 0
    skipped = 0
    
    print(f"Scheduling {total} jobs...")
    
    for i, job in enumerate(jobs):
        job_id = job['job_id']
        
        # Check if already processed
        if skip_existing:
            raw_audio_path = f"{job_id}.wav"
            if object_exists('raw-audio', raw_audio_path):
                print(f"  [{i+1}/{total}] Skipping {job_id} (raw audio exists)")
                skipped += 1
                continue
            
            # Also check if segments exist
            segments = list_objects('segments', prefix=f"{job_id}_")
            if segments:
                print(f"  [{i+1}/{total}] Skipping {job_id} (segments exist)")
                skipped += 1
                continue
        
        # Enqueue download job
        output_path = f"raw-audio/{job_id}.wav"
        enqueue_download(
            job_id=job_id,
            source_type=job['source_type'],
            source_url=job['source_url'],
            output_path=output_path,
            metadata=job.get('metadata', {})
        )
        
        enqueued += 1
        
        if (i + 1) % 100 == 0:
            print(f"  Progress: {i+1}/{total} (enqueued: {enqueued}, skipped: {skipped})")
    
    print(f"\nScheduling complete:")
    print(f"  Total jobs: {total}")
    print(f"  Enqueued: {enqueued}")
    print(f"  Skipped: {skipped}")


def get_status():
    """Print current queue status."""
    print("\nQueue Status:")
    print("-" * 40)
    
    try:
        download_url = get_download_queue_url()
        stats = get_queue_stats(download_url)
        print(f"Download Queue ({download_url.split('/')[-1]}):")
        print(f"  Pending: {stats['pending']}")
        print(f"  In-flight: {stats['in_flight']}")
        print(f"  Delayed: {stats['delayed']}")
    except Exception as e:
        print(f"Download Queue: Error - {e}")
    
    try:
        split_url = get_split_queue_url()
        stats = get_queue_stats(split_url)
        print(f"Split Queue ({split_url.split('/')[-1]}):")
        print(f"  Pending: {stats['pending']}")
        print(f"  In-flight: {stats['in_flight']}")
        print(f"  Delayed: {stats['delayed']}")
    except Exception as e:
        print(f"Split Queue: Error - {e}")


def create_sample_catalog():
    """Create a sample catalog file for reference."""
    sample = {
        "jobs": [
            {
                "job_id": "sample_001",
                "source_type": "youtube",
                "source_url": "https://www.youtube.com/watch?v=example1"
            },
            {
                "job_id": "sample_002",
                "source_type": "gdrive",
                "source_url": "https://drive.google.com/file/d/abc123/view"
            },
            {
                "job_id": "sample_003",
                "source_type": "url",
                "source_url": "https://example.com/audio.wav"
            }
        ]
    }
    
    return sample


def main():
    parser = argparse.ArgumentParser(description='Schedule audio processing jobs')
    
    # Source options
    parser.add_argument('--source', choices=['minio', 'file', 'spreadsheet', 'bucket'],
                        default='bucket', help='Job source (default: bucket for Monlam audio)')
    
    # Bucket source options (for Monlam audio)
    parser.add_argument('--bucket', default='audio',
                        help='Source bucket containing audio files (default: audio)')
    parser.add_argument('--collections', type=str,
                        help='Comma-separated collections to process (e.g., amdo,kham,utsang)')
    parser.add_argument('--segments-bucket', default='segments',
                        help='Output bucket for segments (default: segments)')
    
    # Catalog source options
    parser.add_argument('--catalog', default='config/catalog.json',
                        help='Catalog path (MinIO path or local file)')
    parser.add_argument('--sheet-id', help='Google Sheet ID (for spreadsheet source)')
    parser.add_argument('--id-col', default='ID', help='ID column name')
    parser.add_argument('--link-col', default='youtube link', help='Link column name')
    parser.add_argument('--from-id', type=int, help='Start ID for filtering')
    parser.add_argument('--to-id', type=int, help='End ID for filtering')
    
    # Common options
    parser.add_argument('--no-skip', action='store_true',
                        help='Do not skip existing jobs')
    parser.add_argument('--limit', type=int,
                        help='Maximum number of jobs to schedule')
    parser.add_argument('--dry-run', action='store_true',
                        help='Print what would be done without actually scheduling')
    parser.add_argument('--status', action='store_true',
                        help='Show queue status and exit')
    parser.add_argument('--create-sample', action='store_true',
                        help='Create sample catalog and exit')
    
    args = parser.parse_args()
    
    if args.status:
        get_status()
        return
    
    if args.create_sample:
        sample = create_sample_catalog()
        print(json.dumps(sample, indent=2))
        return
    
    # Parse collections
    collections = None
    if args.collections:
        collections = [c.strip() for c in args.collections.split(',')]
    
    # Route to appropriate handler based on source
    if args.source == 'bucket':
        # Direct bucket processing (for Monlam audio data)
        schedule_split_jobs_from_bucket(
            source_bucket=args.bucket,
            collections=collections,
            segments_bucket=args.segments_bucket,
            skip_existing=not args.no_skip,
            limit=args.limit,
            dry_run=args.dry_run
        )
        
    elif args.source == 'minio':
        parts = args.catalog.split('/', 1)
        bucket = parts[0]
        object_name = parts[1] if len(parts) > 1 else 'catalog.json'
        jobs = load_catalog_from_minio(bucket, object_name)
        if jobs:
            print(f"Loaded {len(jobs)} jobs from catalog")
            schedule_jobs(jobs, skip_existing=not args.no_skip)
        else:
            print("No jobs found in catalog")
            return
        
    elif args.source == 'file':
        jobs = load_catalog_from_file(args.catalog)
        if jobs:
            print(f"Loaded {len(jobs)} jobs from file")
            schedule_jobs(jobs, skip_existing=not args.no_skip)
        else:
            print("No jobs found in file")
            return
        
    elif args.source == 'spreadsheet':
        if not args.sheet_id:
            print("Error: --sheet-id required for spreadsheet source")
            return
        jobs = load_catalog_from_spreadsheet(
            args.sheet_id,
            args.id_col,
            args.link_col,
            args.from_id,
            args.to_id
        )
        if jobs:
            print(f"Loaded {len(jobs)} jobs from spreadsheet")
            schedule_jobs(jobs, skip_existing=not args.no_skip)
        else:
            print("No jobs found in spreadsheet")
            return
    
    # Show final status
    get_status()


if __name__ == '__main__':
    main()
