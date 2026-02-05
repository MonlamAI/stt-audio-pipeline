"""
MinIO/S3-compatible storage utility module for audio processing pipeline.
Uses boto3 for better compatibility with various S3 implementations.
"""

import os
import json
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import warnings

# Suppress SSL warnings when verify=False
warnings.filterwarnings('ignore', message='Unverified HTTPS request')

_client = None


def get_s3_client():
    """
    Get or create an S3 client singleton.
    
    Environment variables:
        MINIO_ENDPOINT: MinIO server endpoint (default: localhost:9000)
        MINIO_ACCESS_KEY: Access key for authentication
        MINIO_SECRET_KEY: Secret key for authentication
        MINIO_SECURE: Use HTTPS (default: false)
        MINIO_VERIFY_SSL: Verify SSL certificate (default: true)
    
    Returns:
        boto3 S3 client
    """
    global _client
    if _client is None:
        endpoint = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
        secure = os.environ.get("MINIO_SECURE", "false").lower() == "true"
        verify_ssl = os.environ.get("MINIO_VERIFY_SSL", "true").lower() == "true"
        
        # Build endpoint URL
        protocol = "https" if secure else "http"
        if not endpoint.startswith("http"):
            endpoint_url = f"{protocol}://{endpoint}"
        else:
            endpoint_url = endpoint
        
        _client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY"),
            aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY"),
            verify=verify_ssl,
            config=Config(signature_version='s3v4')
        )
    return _client


# Alias for backward compatibility
get_minio_client = get_s3_client


def ensure_bucket_exists(bucket: str):
    """
    Create bucket if it doesn't exist.
    
    Args:
        bucket: Bucket name to create
    """
    client = get_s3_client()
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            client.create_bucket(Bucket=bucket)
            print(f"Created bucket: {bucket}")
        else:
            raise


def upload_file(local_path: str, bucket: str, object_name: str):
    """
    Upload a local file to MinIO/S3.
    
    Args:
        local_path: Path to local file
        bucket: Target bucket name
        object_name: Object key in bucket
    """
    client = get_s3_client()
    client.upload_file(local_path, bucket, object_name)
    print(f"Uploaded {local_path} -> s3://{bucket}/{object_name}")


def download_file(bucket: str, object_name: str, local_path: str):
    """
    Download a file from MinIO/S3 to local path.
    
    Args:
        bucket: Source bucket name
        object_name: Object key in bucket
        local_path: Local path to save file
    """
    client = get_s3_client()
    
    # Ensure parent directory exists
    os.makedirs(os.path.dirname(local_path) or '.', exist_ok=True)
    
    client.download_file(bucket, object_name, local_path)
    print(f"Downloaded s3://{bucket}/{object_name} -> {local_path}")


def object_exists(bucket: str, object_name: str) -> bool:
    """
    Check if object exists in bucket (for idempotency).
    
    Args:
        bucket: Bucket name
        object_name: Object key to check
        
    Returns:
        bool: True if object exists, False otherwise
    """
    try:
        client = get_s3_client()
        client.head_object(Bucket=bucket, Key=object_name)
        return True
    except ClientError:
        return False


def list_objects(bucket: str, prefix: str = "", recursive: bool = True):
    """
    List objects in bucket with optional prefix.
    
    Args:
        bucket: Bucket name
        prefix: Filter objects by prefix
        recursive: Include objects in subdirectories
        
    Returns:
        list: List of object info dictionaries
    """
    client = get_s3_client()
    objects = []
    
    paginator = client.get_paginator('list_objects_v2')
    
    params = {'Bucket': bucket, 'Prefix': prefix}
    if not recursive:
        params['Delimiter'] = '/'
    
    for page in paginator.paginate(**params):
        for obj in page.get('Contents', []):
            objects.append({
                "name": obj['Key'],
                "size": obj['Size'],
                "last_modified": obj['LastModified']
            })
    
    return objects


def read_json(bucket: str, object_name: str) -> dict:
    """
    Read and parse JSON file from MinIO/S3.
    
    Args:
        bucket: Bucket name
        object_name: Object key for JSON file
        
    Returns:
        dict: Parsed JSON content
    """
    client = get_s3_client()
    response = client.get_object(Bucket=bucket, Key=object_name)
    return json.loads(response['Body'].read().decode('utf-8'))


def write_json(bucket: str, object_name: str, data: dict):
    """
    Write dictionary as JSON to MinIO/S3.
    
    Args:
        bucket: Target bucket name
        object_name: Object key for JSON file
        data: Dictionary to serialize as JSON
    """
    client = get_s3_client()
    ensure_bucket_exists(bucket)
    
    json_bytes = json.dumps(data, indent=2).encode('utf-8')
    
    client.put_object(
        Bucket=bucket,
        Key=object_name,
        Body=json_bytes,
        ContentType='application/json'
    )
    print(f"Wrote JSON to s3://{bucket}/{object_name}")


def delete_object(bucket: str, object_name: str):
    """
    Delete an object from MinIO/S3.
    
    Args:
        bucket: Bucket name
        object_name: Object key to delete
    """
    client = get_s3_client()
    client.delete_object(Bucket=bucket, Key=object_name)
    print(f"Deleted s3://{bucket}/{object_name}")


def get_object_url(bucket: str, object_name: str, expires_hours: int = 24) -> str:
    """
    Get a presigned URL for an object.
    
    Args:
        bucket: Bucket name
        object_name: Object key
        expires_hours: URL expiration time in hours
        
    Returns:
        str: Presigned URL
    """
    client = get_s3_client()
    return client.generate_presigned_url(
        'get_object',
        Params={'Bucket': bucket, 'Key': object_name},
        ExpiresIn=expires_hours * 3600
    )
