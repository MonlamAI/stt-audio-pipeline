"""
Download Worker - Stateless job processor for downloading audio files.
Pulls jobs from SQS, downloads audio, saves to MinIO, enqueues split job.
"""

import json
import os
import sys
import time
import subprocess
import tempfile
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from job_queue.sqs_queue import (
    receive_messages,
    delete_message,
    enqueue_split,
    change_message_visibility,
    get_download_queue_url,
)
from util.minio_utils import upload_file, object_exists


def process_download(job: dict):
    """
    Download audio from source and save to MinIO.
    
    Args:
        job: Job dictionary with keys:
            - job_id: Unique job identifier
            - source_type: 'youtube', 'gdrive', 's3', 'url'
            - source_url: URL or path to download from
            - output_path: MinIO path (bucket/key)
    """
    job_id = job['job_id']
    source_type = job['source_type']
    source_url = job['source_url']
    output_path = job.get('output_path', f"raw-audio/{job_id}.wav")
    
    # Parse bucket and object name from output_path
    parts = output_path.split('/', 1)
    bucket = parts[0] if len(parts) > 1 else 'raw-audio'
    object_name = parts[1] if len(parts) > 1 else f"{job_id}.wav"
    
    # Check if already processed (idempotency)
    if object_exists(bucket, object_name):
        print(f"Job {job_id}: Already exists in MinIO, skipping download")
        # Still enqueue split job in case it wasn't processed
        enqueue_split(job_id, output_path, job_id)
        return
    
    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = os.path.join(tmpdir, f"{job_id}.wav")
        
        # Download based on source type
        if source_type == 'youtube':
            download_youtube(source_url, local_path)
        elif source_type == 'gdrive':
            download_gdrive(source_url, local_path)
        elif source_type == 'url':
            download_url(source_url, local_path)
        elif source_type == 's3':
            download_s3(source_url, local_path)
        else:
            raise ValueError(f"Unknown source type: {source_type}")
        
        # Verify file exists and has content
        if not os.path.exists(local_path) or os.path.getsize(local_path) == 0:
            raise RuntimeError(f"Download failed: {local_path} is empty or missing")
        
        # Upload to MinIO
        upload_file(local_path, bucket, object_name)
        
        # Enqueue split job
        enqueue_split(job_id, output_path, job_id)
        
        print(f"Job {job_id}: Download complete, split job enqueued")


def download_youtube(url: str, output_path: str):
    """Download audio from YouTube using yt-dlp."""
    print(f"Downloading from YouTube: {url}")
    
    # Output template without extension (yt-dlp adds it)
    output_template = output_path.replace('.wav', '')
    
    cmd = [
        'yt-dlp',
        '--extract-audio',
        '--audio-format', 'wav',
        '--audio-quality', '0',
        '--postprocessor-args', '-ar 16000 -ac 1',
        '--no-playlist',
        '-o', output_template + '.%(ext)s',
        url
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        raise RuntimeError(f"yt-dlp failed: {result.stderr}")
    
    # yt-dlp may create file with different name, find and rename
    output_dir = os.path.dirname(output_path)
    for f in os.listdir(output_dir or '.'):
        if f.startswith(os.path.basename(output_template)):
            actual_path = os.path.join(output_dir or '.', f)
            if actual_path != output_path:
                os.rename(actual_path, output_path)
            break


def download_gdrive(url: str, output_path: str):
    """Download audio from Google Drive."""
    print(f"Downloading from Google Drive: {url}")
    
    # Extract file ID from URL
    if 'drive.google.com' in url:
        if '/d/' in url:
            file_id = url.split('/d/')[1].split('/')[0]
        elif 'id=' in url:
            file_id = url.split('id=')[1].split('&')[0]
        else:
            file_id = url
    else:
        file_id = url
    
    # Use gdown for Google Drive downloads
    import gdown
    gdown.download(id=file_id, output=output_path, quiet=False)
    
    # Convert to 16kHz mono WAV if needed
    convert_to_wav_16k(output_path, output_path)


def download_url(url: str, output_path: str):
    """Download audio from direct URL."""
    print(f"Downloading from URL: {url}")
    
    import requests
    
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    # Save to temp file first
    temp_path = output_path + '.tmp'
    with open(temp_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    
    # Convert to 16kHz mono WAV
    convert_to_wav_16k(temp_path, output_path)
    
    # Clean up temp file
    if os.path.exists(temp_path):
        os.remove(temp_path)


def download_s3(s3_path: str, output_path: str):
    """Download audio from S3/MinIO."""
    print(f"Downloading from S3: {s3_path}")
    
    # Parse s3://bucket/key format
    if s3_path.startswith('s3://'):
        s3_path = s3_path[5:]
    
    parts = s3_path.split('/', 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ''
    
    from util.minio_utils import download_file
    download_file(bucket, key, output_path)
    
    # Convert to 16kHz mono WAV if needed
    convert_to_wav_16k(output_path, output_path)


def convert_to_wav_16k(input_path: str, output_path: str):
    """Convert audio file to 16kHz mono WAV using ffmpeg."""
    temp_output = output_path + '.conv.wav'
    
    cmd = [
        'ffmpeg', '-y',
        '-i', input_path,
        '-ac', '1',
        '-ar', '16000',
        '-acodec', 'pcm_s16le',
        temp_output
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode == 0:
        # Replace original with converted
        os.replace(temp_output, output_path)
    else:
        # Clean up on failure
        if os.path.exists(temp_output):
            os.remove(temp_output)
        # If conversion fails, assume input is already correct format
        print(f"FFmpeg conversion note: {result.stderr}")


def run_download_worker():
    """
    Main worker loop - continuously poll SQS and process download jobs.
    """
    print("Starting download worker...")
    
    # Get queue URL (auto-creates if needed)
    queue_url = get_download_queue_url()
    print(f"Queue URL: {queue_url}")
    
    consecutive_empty = 0
    
    while True:
        try:
            messages = receive_messages(
                queue_url,
                max_messages=1,  # Process one at a time for reliability
                wait_time_seconds=20,
                visibility_timeout=600  # 10 minutes for download
            )
            
            if not messages:
                consecutive_empty += 1
                if consecutive_empty % 10 == 0:
                    print(f"No messages received ({consecutive_empty} empty polls)")
                continue
            
            consecutive_empty = 0
            
            for msg in messages:
                job = json.loads(msg['Body'])
                job_id = job.get('job_id', 'unknown')
                
                print(f"\n{'='*50}")
                print(f"Processing download job: {job_id}")
                print(f"Source: {job.get('source_type')} - {job.get('source_url')}")
                
                try:
                    process_download(job)
                    delete_message(queue_url, msg['ReceiptHandle'])
                    print(f"Job {job_id}: SUCCESS")
                    
                except Exception as e:
                    print(f"Job {job_id}: FAILED - {e}")
                    # Message will return to queue after visibility timeout
                    # Could implement dead letter queue for repeated failures
                    
        except KeyboardInterrupt:
            print("\nShutting down download worker...")
            break
            
        except Exception as e:
            print(f"Worker error: {e}")
            time.sleep(5)  # Back off on errors


if __name__ == '__main__':
    run_download_worker()
