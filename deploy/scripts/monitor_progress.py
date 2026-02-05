#!/usr/bin/env python3
"""
Monitor STT processing pipeline progress.
Shows SQS queue depth, ASG instance count, and S3 output stats.
Usage: python monitor_progress.py --interval 30
"""

import os
import sys
import time
import argparse
from datetime import datetime, timezone

import boto3
from dotenv import load_dotenv

# Load environment
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))


def get_queue_stats(sqs_client, queue_name):
    """Get SQS queue statistics."""
    try:
        response = sqs_client.get_queue_url(QueueName=queue_name)
        queue_url = response['QueueUrl']
        
        attrs = sqs_client.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=[
                'ApproximateNumberOfMessages',
                'ApproximateNumberOfMessagesNotVisible',
                'ApproximateNumberOfMessagesDelayed'
            ]
        )['Attributes']
        
        return {
            'pending': int(attrs.get('ApproximateNumberOfMessages', 0)),
            'in_flight': int(attrs.get('ApproximateNumberOfMessagesNotVisible', 0)),
            'delayed': int(attrs.get('ApproximateNumberOfMessagesDelayed', 0))
        }
    except Exception as e:
        return {'error': str(e)}


def get_asg_stats(asg_client, asg_name):
    """Get Auto Scaling Group statistics."""
    try:
        response = asg_client.describe_auto_scaling_groups(
            AutoScalingGroupNames=[asg_name]
        )
        
        if not response['AutoScalingGroups']:
            return {'error': 'ASG not found'}
        
        asg = response['AutoScalingGroups'][0]
        
        instances = asg.get('Instances', [])
        running = sum(1 for i in instances if i['LifecycleState'] == 'InService')
        pending = sum(1 for i in instances if i['LifecycleState'] == 'Pending')
        terminating = sum(1 for i in instances if i['LifecycleState'] == 'Terminating')
        
        return {
            'desired': asg['DesiredCapacity'],
            'running': running,
            'pending': pending,
            'terminating': terminating,
            'min': asg['MinSize'],
            'max': asg['MaxSize']
        }
    except Exception as e:
        return {'error': str(e)}


def get_s3_stats(s3_client, bucket, prefix=None):
    """Get S3 bucket output statistics."""
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        
        total_objects = 0
        total_size = 0
        collections = set()
        
        params = {'Bucket': bucket}
        if prefix:
            params['Prefix'] = prefix
        
        for page in paginator.paginate(**params):
            for obj in page.get('Contents', []):
                total_objects += 1
                total_size += obj['Size']
                
                # Extract collection from key
                parts = obj['Key'].split('/')
                if len(parts) > 1:
                    collections.add(parts[0])
        
        return {
            'objects': total_objects,
            'size_gb': total_size / (1024**3),
            'collections': len(collections)
        }
    except Exception as e:
        return {'error': str(e)}


def format_duration(seconds):
    """Format seconds as HH:MM:SS."""
    hours, remainder = divmod(int(seconds), 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def main():
    parser = argparse.ArgumentParser(description='Monitor STT processing pipeline')
    parser.add_argument('--interval', type=int, default=30, help='Refresh interval in seconds')
    parser.add_argument('--queue-name', type=str, default=None, help='SQS queue name')
    parser.add_argument('--asg-name', type=str, default=None, help='Auto Scaling Group name')
    parser.add_argument('--bucket', type=str, default=None, help='S3 output bucket')
    parser.add_argument('--once', action='store_true', help='Run once and exit')
    args = parser.parse_args()
    
    # Get configuration
    queue_name = args.queue_name or os.environ.get('SQS_SPLIT_QUEUE_NAME', 'stt-split-jobs')
    asg_name = args.asg_name or f"stt-workers-{os.environ.get('ENVIRONMENT', 'prod')}"
    bucket = args.bucket or os.environ.get('AWS_S3_BUCKET', 'monlamai-stt-raw-audio')
    
    # Initialize clients
    region = os.environ.get('AWS_REGION', 'us-east-1')
    sqs_client = boto3.client('sqs', region_name=region)
    asg_client = boto3.client('autoscaling', region_name=region)
    s3_client = boto3.client('s3', region_name=region)
    
    print("=" * 70)
    print("STT PIPELINE MONITOR")
    print("=" * 70)
    print(f"Queue: {queue_name}")
    print(f"ASG: {asg_name}")
    print(f"Bucket: {bucket}")
    print(f"Refresh: {args.interval}s")
    print("=" * 70)
    
    start_time = time.time()
    initial_pending = None
    
    while True:
        try:
            now = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
            elapsed = format_duration(time.time() - start_time)
            
            # Get stats
            queue_stats = get_queue_stats(sqs_client, queue_name)
            asg_stats = get_asg_stats(asg_client, asg_name)
            
            # Track initial pending for progress
            if initial_pending is None and 'pending' in queue_stats:
                initial_pending = queue_stats['pending'] + queue_stats.get('in_flight', 0)
            
            # Calculate progress
            if initial_pending and initial_pending > 0:
                current = queue_stats.get('pending', 0) + queue_stats.get('in_flight', 0)
                processed = initial_pending - current
                progress = (processed / initial_pending) * 100
            else:
                processed = 0
                progress = 0
            
            # Clear screen and print status
            print("\033[2J\033[H", end="")  # Clear screen
            print("=" * 70)
            print(f"STT PIPELINE MONITOR - {now}")
            print(f"Elapsed: {elapsed}")
            print("=" * 70)
            
            # Queue stats
            print(f"\n📥 SQS QUEUE ({queue_name})")
            if 'error' in queue_stats:
                print(f"   Error: {queue_stats['error']}")
            else:
                print(f"   Pending:   {queue_stats['pending']:,}")
                print(f"   In-flight: {queue_stats['in_flight']:,}")
                print(f"   Delayed:   {queue_stats['delayed']:,}")
                if initial_pending:
                    print(f"   Processed: {processed:,} / {initial_pending:,} ({progress:.1f}%)")
            
            # ASG stats
            print(f"\n🖥️  AUTO SCALING ({asg_name})")
            if 'error' in asg_stats:
                print(f"   Error: {asg_stats['error']}")
            else:
                print(f"   Running:     {asg_stats['running']}")
                print(f"   Pending:     {asg_stats['pending']}")
                print(f"   Terminating: {asg_stats['terminating']}")
                print(f"   Desired:     {asg_stats['desired']} (min: {asg_stats['min']}, max: {asg_stats['max']})")
            
            # Progress bar
            if initial_pending and initial_pending > 0:
                bar_width = 50
                filled = int(bar_width * progress / 100)
                bar = "█" * filled + "░" * (bar_width - filled)
                print(f"\n📊 PROGRESS")
                print(f"   [{bar}] {progress:.1f}%")
                
                # ETA calculation
                if processed > 0:
                    elapsed_sec = time.time() - start_time
                    rate = processed / elapsed_sec
                    remaining = initial_pending - processed
                    if rate > 0:
                        eta_sec = remaining / rate
                        print(f"   Rate: {rate:.1f} jobs/sec")
                        print(f"   ETA:  {format_duration(eta_sec)}")
            
            # Completion check
            if queue_stats.get('pending', 1) == 0 and queue_stats.get('in_flight', 1) == 0:
                print("\n✅ PROCESSING COMPLETE!")
                if not args.once:
                    print("   All jobs processed. Monitoring will continue...")
            
            print("\n" + "=" * 70)
            print("Press Ctrl+C to exit")
            
            if args.once:
                break
            
            time.sleep(args.interval)
            
        except KeyboardInterrupt:
            print("\n\nMonitoring stopped.")
            break
        except Exception as e:
            print(f"\nError: {e}")
            if args.once:
                break
            time.sleep(args.interval)


if __name__ == '__main__':
    main()
