#!/usr/bin/env python3
"""
Deploy the EC2 Spot Auto Scaling infrastructure using CloudFormation.
Usage: python deploy_stack.py --action deploy
"""

import os
import sys
import time
import argparse

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Load environment
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

STACK_NAME = 'stt-audio-pipeline'
TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), '..', 'cloudformation', 'ec2-spot-workers.yaml')


def get_clients():
    """Get AWS clients."""
    region = os.environ.get('AWS_REGION', 'us-east-1')
    return {
        'cfn': boto3.client('cloudformation', region_name=region),
        'ssm': boto3.client('ssm', region_name=region),
        'asg': boto3.client('autoscaling', region_name=region),
    }


def store_secrets(ssm_client):
    """Store MinIO credentials in SSM Parameter Store."""
    secrets = {
        '/stt-pipeline/minio-access-key': os.environ.get('MINIO_ACCESS_KEY', 'monlam'),
        '/stt-pipeline/minio-secret-key': os.environ.get('MINIO_SECRET_KEY', ''),
    }
    
    print("\nStoring secrets in SSM Parameter Store...")
    for name, value in secrets.items():
        if not value:
            print(f"  Skipping {name} (empty value)")
            continue
        try:
            ssm_client.put_parameter(
                Name=name,
                Value=value,
                Type='SecureString',
                Overwrite=True
            )
            print(f"  ✓ {name}")
        except Exception as e:
            print(f"  ✗ {name}: {e}")


def deploy_stack(cfn_client, environment='prod'):
    """Deploy or update the CloudFormation stack."""
    print(f"\nDeploying stack: {STACK_NAME}")
    
    # Read template
    with open(TEMPLATE_PATH, 'r') as f:
        template_body = f.read()
    
    # Parameters
    parameters = [
        {'ParameterKey': 'Environment', 'ParameterValue': environment},
        {'ParameterKey': 'SQSQueueName', 'ParameterValue': os.environ.get('SQS_SPLIT_QUEUE_NAME', 'stt-split-jobs')},
        {'ParameterKey': 'S3OutputBucket', 'ParameterValue': os.environ.get('AWS_S3_BUCKET', 'monlamai-stt-raw-audio')},
        {'ParameterKey': 'MinIOEndpoint', 'ParameterValue': os.environ.get('MINIO_ENDPOINT', 's3.monlam.ai')},
        {'ParameterKey': 'MaxWorkers', 'ParameterValue': '20'},
    ]
    
    # Check if stack exists
    try:
        cfn_client.describe_stacks(StackName=STACK_NAME)
        action = 'update'
    except ClientError as e:
        if 'does not exist' in str(e):
            action = 'create'
        else:
            raise
    
    print(f"  Action: {action}")
    
    # Deploy
    try:
        if action == 'create':
            response = cfn_client.create_stack(
                StackName=STACK_NAME,
                TemplateBody=template_body,
                Parameters=parameters,
                Capabilities=['CAPABILITY_NAMED_IAM'],
                Tags=[
                    {'Key': 'Project', 'Value': 'stt-audio-pipeline'},
                    {'Key': 'Environment', 'Value': environment},
                ]
            )
            print(f"  Stack creation initiated: {response['StackId']}")
        else:
            response = cfn_client.update_stack(
                StackName=STACK_NAME,
                TemplateBody=template_body,
                Parameters=parameters,
                Capabilities=['CAPABILITY_NAMED_IAM'],
            )
            print(f"  Stack update initiated: {response['StackId']}")
    except ClientError as e:
        if 'No updates are to be performed' in str(e):
            print("  No changes to deploy")
            return True
        raise
    
    # Wait for completion
    print("\n  Waiting for deployment...")
    waiter_name = f"stack_{action}_complete"
    waiter = cfn_client.get_waiter(waiter_name)
    
    try:
        waiter.wait(
            StackName=STACK_NAME,
            WaiterConfig={'Delay': 10, 'MaxAttempts': 60}
        )
        print(f"  ✓ Stack {action}d successfully!")
        return True
    except Exception as e:
        print(f"  ✗ Stack {action} failed: {e}")
        return False


def get_stack_outputs(cfn_client):
    """Get stack outputs."""
    try:
        response = cfn_client.describe_stacks(StackName=STACK_NAME)
        outputs = response['Stacks'][0].get('Outputs', [])
        return {o['OutputKey']: o['OutputValue'] for o in outputs}
    except Exception as e:
        print(f"Error getting outputs: {e}")
        return {}


def delete_stack(cfn_client):
    """Delete the CloudFormation stack."""
    print(f"\nDeleting stack: {STACK_NAME}")
    
    try:
        cfn_client.delete_stack(StackName=STACK_NAME)
        print("  Deletion initiated...")
        
        waiter = cfn_client.get_waiter('stack_delete_complete')
        waiter.wait(
            StackName=STACK_NAME,
            WaiterConfig={'Delay': 10, 'MaxAttempts': 60}
        )
        print("  ✓ Stack deleted successfully!")
        return True
    except Exception as e:
        print(f"  ✗ Stack deletion failed: {e}")
        return False


def scale_workers(asg_client, count, asg_name=None):
    """Manually scale workers."""
    asg_name = asg_name or f"stt-workers-{os.environ.get('ENVIRONMENT', 'prod')}"
    
    print(f"\nScaling {asg_name} to {count} instances...")
    
    try:
        asg_client.set_desired_capacity(
            AutoScalingGroupName=asg_name,
            DesiredCapacity=count
        )
        print(f"  ✓ Scaled to {count}")
        return True
    except Exception as e:
        print(f"  ✗ Scaling failed: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description='Deploy STT pipeline infrastructure')
    parser.add_argument('--action', choices=['deploy', 'delete', 'status', 'scale', 'secrets'],
                        default='status', help='Action to perform')
    parser.add_argument('--environment', default='prod', help='Environment name')
    parser.add_argument('--count', type=int, default=0, help='Worker count for scale action')
    args = parser.parse_args()
    
    clients = get_clients()
    
    print("=" * 60)
    print("STT PIPELINE INFRASTRUCTURE")
    print("=" * 60)
    
    if args.action == 'secrets':
        store_secrets(clients['ssm'])
        
    elif args.action == 'deploy':
        store_secrets(clients['ssm'])
        success = deploy_stack(clients['cfn'], args.environment)
        
        if success:
            print("\n" + "=" * 60)
            print("STACK OUTPUTS")
            print("=" * 60)
            outputs = get_stack_outputs(clients['cfn'])
            for key, value in outputs.items():
                print(f"  {key}: {value}")
                
    elif args.action == 'delete':
        delete_stack(clients['cfn'])
        
    elif args.action == 'scale':
        scale_workers(clients['asg'], args.count)
        
    elif args.action == 'status':
        print("\nStack Status:")
        try:
            response = clients['cfn'].describe_stacks(StackName=STACK_NAME)
            stack = response['Stacks'][0]
            print(f"  Name: {stack['StackName']}")
            print(f"  Status: {stack['StackStatus']}")
            print(f"  Created: {stack.get('CreationTime', 'N/A')}")
            
            print("\nOutputs:")
            outputs = get_stack_outputs(clients['cfn'])
            for key, value in outputs.items():
                print(f"  {key}: {value}")
        except ClientError as e:
            if 'does not exist' in str(e):
                print("  Stack not deployed")
            else:
                raise


if __name__ == '__main__':
    main()
