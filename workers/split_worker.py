"""
Split Worker - Stateless job processor for splitting audio files.
Pulls jobs from SQS, downloads from MinIO, splits audio, uploads segments to AWS S3.

Supports two job formats:
1. New format: source_bucket, source_key, output_bucket, output_prefix
2. Legacy format: raw_audio_path (bucket/key combined)
"""

import json
import os
import sys
import time
import tempfile
import subprocess
from pathlib import Path
from datetime import datetime, timezone

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import boto3
from botocore.config import Config
from pydub import AudioSegment

from job_queue.sqs_queue import (
    receive_messages,
    delete_message,
    change_message_visibility,
    get_split_queue_url,
)
from util.minio_utils import download_file

# Global VAD model (loaded once per worker)
_vad_model = None
_vad_utils = None

MAX_DURATION_SEC = 30
MIN_DURATION_SEC = 1


def get_vad_model():
    """Load Silero VAD model (cached)."""
    global _vad_model, _vad_utils
    
    if _vad_model is None:
        import ssl
        import certifi
        ssl._create_default_https_context = lambda: ssl.create_default_context(cafile=certifi.where())
        
        import torch
        torch.set_grad_enabled(False)
        
        print("Loading Silero VAD model...")
        _vad_model, utils = torch.hub.load(
            repo_or_dir='snakers4/silero-vad',
            model='silero_vad',
            force_reload=False,
            onnx=False,
            verbose=False,
            trust_repo=True
        )
        _vad_utils = utils
        print("VAD model loaded!")
    
    return _vad_model, _vad_utils


def get_s3_client():
    """Get AWS S3 client for output."""
    return boto3.client(
        's3',
        region_name=os.environ.get('AWS_REGION', 'us-east-1'),
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY')
    )


def chop_to_max_duration(segments, max_duration=MAX_DURATION_SEC):
    """Recursively chop segments that exceed max_duration into halves."""
    result = []
    for seg in segments:
        duration = seg['end'] - seg['start']
        if duration <= max_duration:
            result.append(seg)
        else:
            mid = (seg['start'] + seg['end']) / 2
            result.extend(chop_to_max_duration([
                {'start': seg['start'], 'end': mid},
                {'start': mid, 'end': seg['end']}
            ], max_duration))
    return result


def split_audio_with_vad(wav_path, max_duration=MAX_DURATION_SEC):
    """
    Split audio using Silero VAD with max duration enforcement.
    
    Returns list of segments: [{'start': float, 'end': float, 'duration': float}, ...]
    """
    model, utils = get_vad_model()
    get_speech_timestamps, _, read_audio, _, _ = utils
    
    # Read audio
    wav = read_audio(wav_path, sampling_rate=16000)
    
    # Detect speech
    speech_timestamps = get_speech_timestamps(
        wav, model,
        threshold=0.3,
        min_speech_duration_ms=2000,
        min_silence_duration_ms=800,
        speech_pad_ms=300,
        return_seconds=True
    )
    
    # Apply max duration chopping
    final_segments = chop_to_max_duration(speech_timestamps, max_duration)
    
    # Filter short segments and add duration
    result = []
    for seg in final_segments:
        duration = seg['end'] - seg['start']
        if duration >= MIN_DURATION_SEC:
            result.append({
                'start': seg['start'],
                'end': seg['end'],
                'duration': duration
            })
    
    return result


def process_split(job: dict):
    """
    Download raw audio from MinIO, split into segments, upload to AWS S3.
    
    Args:
        job: Job dictionary with keys:
            - job_id: Unique job identifier
            - source_bucket: MinIO bucket name
            - source_key: MinIO object key
            - output_bucket: AWS S3 bucket for output
            - output_prefix: Prefix for output segment files
            - max_duration: Max segment duration (default 30)
            - metadata: Optional metadata
    """
    job_id = job['job_id']
    
    # Support both new and legacy job formats
    if 'source_bucket' in job:
        source_bucket = job['source_bucket']
        source_key = job['source_key']
    else:
        # Legacy format: raw_audio_path = "bucket/key"
        raw_audio_path = job.get('raw_audio_path', '')
        parts = raw_audio_path.split('/', 1)
        source_bucket = parts[0] if len(parts) > 1 else 'audio'
        source_key = parts[1] if len(parts) > 1 else raw_audio_path
    
    output_bucket = job.get('output_bucket', os.environ.get('AWS_S3_BUCKET', 'monlamai-stt-raw-audio'))
    output_prefix = job.get('output_prefix', job_id)
    max_duration = job.get('max_duration', MAX_DURATION_SEC)
    metadata = job.get('metadata', {})
    
    print(f"Job {job_id}:")
    print(f"  Source: minio://{source_bucket}/{source_key}")
    print(f"  Output: s3://{output_bucket}/{output_prefix}/")
    
    # Get S3 client for output
    s3_client = get_s3_client()
    
    # Check if already processed (idempotency)
    try:
        response = s3_client.list_objects_v2(
            Bucket=output_bucket,
            Prefix=f"{output_prefix}/",
            MaxKeys=1
        )
        if response.get('Contents'):
            print(f"  Already processed, skipping")
            return
    except Exception as e:
        print(f"  Warning: Could not check existing output: {e}")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        # Download source audio
        source_ext = os.path.splitext(source_key)[1] or '.mp3'
        local_input = os.path.join(tmpdir, f'input{source_ext}')
        
        source_type = job.get('source_type', 'minio')
        
        if source_type == 's3':
            # Download from AWS S3 (after sync_minio_to_s3)
            print(f"  Downloading from S3...")
            s3_client.download_file(source_bucket, source_key, local_input)
        else:
            # Download from MinIO (original path)
            print(f"  Downloading from MinIO...")
            download_file(source_bucket, source_key, local_input)
        
        if not os.path.exists(local_input) or os.path.getsize(local_input) == 0:
            raise RuntimeError(f"Download failed: file is empty or missing")
        
        file_size_mb = os.path.getsize(local_input) / (1024 * 1024)
        print(f"  Downloaded: {file_size_mb:.2f} MB")
        
        # Convert to WAV if needed
        wav_path = os.path.join(tmpdir, 'input.wav')
        if source_ext.lower() != '.wav':
            print(f"  Converting to WAV...")
            convert_to_wav(local_input, wav_path)
        else:
            wav_path = local_input
        
        # Split with VAD
        print(f"  Splitting with VAD (max {max_duration}s)...")
        segments = split_audio_with_vad(wav_path, max_duration)
        
        if not segments:
            print(f"  No speech segments found")
            # Still create metadata to mark as processed
            s3_client.put_object(
                Bucket=output_bucket,
                Key=f"{output_prefix}/metadata.json",
                Body=json.dumps({
                    'job_id': job_id,
                    'source_bucket': source_bucket,
                    'source_key': source_key,
                    'processed_at': datetime.now(timezone.utc).isoformat(),
                    'total_segments': 0,
                    'status': 'no_speech'
                }),
                ContentType='application/json'
            )
            return
        
        print(f"  Found {len(segments)} segments")
        
        # Load audio for segmentation
        audio = AudioSegment.from_wav(wav_path)
        
        # Export and upload segments
        segment_dir = os.path.join(tmpdir, 'segments')
        os.makedirs(segment_dir, exist_ok=True)
        
        uploaded = []
        
        # Prepare all segments first
        segment_files = []
        for i, seg in enumerate(segments):
            start_ms = int(seg['start'] * 1000)
            end_ms = int(seg['end'] * 1000)
            segment_audio = audio[start_ms:end_ms]
            
            local_path = os.path.join(segment_dir, f'segment_{i+1:03d}.wav')
            segment_audio.export(local_path, format='wav')
            
            segment_files.append({
                'local_path': local_path,
                's3_key': f"{output_prefix}/segment_{i+1:03d}.wav",
                'info': {
                    'filename': f'segment_{i+1:03d}.wav',
                    'start_sec': round(seg['start'], 2),
                    'end_sec': round(seg['end'], 2),
                    'duration_sec': round(seg['duration'], 2)
                }
            })
        
        # Parallel upload using ThreadPoolExecutor
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        def upload_segment(seg_info):
            s3_client.upload_file(seg_info['local_path'], output_bucket, seg_info['s3_key'])
            return seg_info['info']
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(upload_segment, sf): sf for sf in segment_files}
            for future in as_completed(futures):
                uploaded.append(future.result())
        
        # Upload metadata
        meta = {
            'job_id': job_id,
            'source_bucket': source_bucket,
            'source_key': source_key,
            'output_bucket': output_bucket,
            'output_prefix': output_prefix,
            'processed_at': datetime.now(timezone.utc).isoformat(),
            'total_segments': len(segments),
            'max_segment_duration': max_duration,
            'segments': uploaded,
            'metadata': metadata
        }
        s3_client.put_object(
            Bucket=output_bucket,
            Key=f"{output_prefix}/metadata.json",
            Body=json.dumps(meta, indent=2),
            ContentType='application/json'
        )
        
        print(f"  Uploaded {len(uploaded)} segments + metadata")


def convert_to_wav(input_path: str, output_path: str):
    """Convert audio file to 16kHz mono WAV using ffmpeg."""
    import subprocess
    
    cmd = [
        'ffmpeg', '-y',
        '-i', input_path,
        '-ac', '1',           # Mono
        '-ar', '16000',       # 16kHz
        '-acodec', 'pcm_s16le',  # PCM 16-bit
        output_path
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        raise RuntimeError(f"FFmpeg conversion failed: {result.stderr}")


def run_split_worker():
    """
    Main worker loop - continuously poll SQS and process split jobs.
    """
    print("Starting split worker...")
    
    # Get queue URL (auto-creates if needed)
    queue_url = get_split_queue_url()
    print(f"Queue URL: {queue_url}")
    
    consecutive_empty = 0
    
    while True:
        try:
            messages = receive_messages(
                queue_url,
                max_messages=5,  # Process multiple for speed
                wait_time_seconds=20,
                visibility_timeout=900  # 15 minutes for large files
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
                print(f"Processing split job: {job_id}")
                print(f"Source: {job.get('raw_audio_path')}")
                
                try:
                    # Extend visibility for large files
                    change_message_visibility(queue_url, msg['ReceiptHandle'], 1800)
                    
                    process_split(job)
                    delete_message(queue_url, msg['ReceiptHandle'])
                    print(f"Job {job_id}: SUCCESS")
                    
                except Exception as e:
                    print(f"Job {job_id}: FAILED - {e}")
                    import traceback
                    traceback.print_exc()
                    # Message will return to queue after visibility timeout
                    
        except KeyboardInterrupt:
            print("\nShutting down split worker...")
            break
            
        except Exception as e:
            print(f"Worker error: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(5)  # Back off on errors


if __name__ == '__main__':
    run_split_worker()
