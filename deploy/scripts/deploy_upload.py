#!/usr/bin/env python3
"""
Deploy and manage the MinIO-to-S3 upload infrastructure.
Usage:
    python deploy_upload.py --action deploy
    python deploy_upload.py --action status
    python deploy_upload.py --action scale --count 3
    python deploy_upload.py --action delete
"""

import os
import sys
import argparse

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Load environment
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

STACK_NAME = 'stt-upload-pipeline'
TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), '..', 'cloudformation', 'ec2-upload-worker.yaml')


def get_clients():
    """Get AWS clients."""
    region = os.environ.get('AWS_REGION', 'us-east-1')
    return {
        'cfn': boto3.client('cloudformation', region_name=region),
        'ssm': boto3.client('ssm', region_name=region),
        'asg': boto3.client('autoscaling', region_name=region),
        'sqs': boto3.client('sqs', region_name=region),
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
            print(f"  Stored {name}")
        except Exception as e:
            print(f"  Failed {name}: {e}")


def deploy_stack(cfn_client, environment='prod'):
    """Deploy or update the CloudFormation stack."""
    print(f"\nDeploying stack: {STACK_NAME}")

    # Read template
    with open(TEMPLATE_PATH, 'r') as f:
        template_body = f.read()

    # Parameters
    parameters = [
        {'ParameterKey': 'Environment', 'ParameterValue': environment},
        {'ParameterKey': 'SQSQueueName', 'ParameterValue': os.environ.get('SQS_UPLOAD_QUEUE_NAME', 'stt-upload-jobs')},
        {'ParameterKey': 'S3OutputBucket', 'ParameterValue': os.environ.get('AWS_S3_BUCKET', 'monlamai-stt-raw-audio')},
        {'ParameterKey': 'MinIOEndpoint', 'ParameterValue': os.environ.get('MINIO_ENDPOINT', 's3.monlam.ai')},
        {'ParameterKey': 'DesiredCapacity', 'ParameterValue': '1'},
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
                    {'Key': 'Project', 'Value': 'stt-upload-pipeline'},
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
        print(f"  Stack {action}d successfully!")
        return True
    except Exception as e:
        print(f"  Stack {action} failed: {e}")
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
        print("  Stack deleted successfully!")
        return True
    except Exception as e:
        print(f"  Stack deletion failed: {e}")
        return False


def scale_workers(asg_client, count, environment='prod'):
    """Manually scale upload workers."""
    asg_name = f"stt-upload-workers-{environment}"

    print(f"\nScaling {asg_name} to {count} instances...")

    try:
        asg_client.set_desired_capacity(
            AutoScalingGroupName=asg_name,
            DesiredCapacity=count
        )
        print(f"  Scaled to {count}")
        return True
    except Exception as e:
        print(f"  Scaling failed: {e}")
        return False


def show_status(clients, environment='prod'):
    """Show stack status, queue stats, and instance state."""
    cfn_client = clients['cfn']
    sqs_client = clients['sqs']
    asg_client = clients['asg']

    # Stack status
    print("\nStack Status:")
    try:
        response = cfn_client.describe_stacks(StackName=STACK_NAME)
        stack = response['Stacks'][0]
        print(f"  Name:    {stack['StackName']}")
        print(f"  Status:  {stack['StackStatus']}")
        print(f"  Created: {stack.get('CreationTime', 'N/A')}")
    except ClientError as e:
        if 'does not exist' in str(e):
            print("  Stack not deployed")
            return
        raise

    # Queue stats
    print("\nQueue Stats:")
    queue_name = os.environ.get('SQS_UPLOAD_QUEUE_NAME', 'stt-upload-jobs')
    try:
        queue_url = sqs_client.get_queue_url(QueueName=queue_name)['QueueUrl']
        attrs = sqs_client.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=[
                'ApproximateNumberOfMessages',
                'ApproximateNumberOfMessagesNotVisible',
                'ApproximateNumberOfMessagesDelayed'
            ]
        )['Attributes']

        pending = int(attrs.get('ApproximateNumberOfMessages', 0))
        in_flight = int(attrs.get('ApproximateNumberOfMessagesNotVisible', 0))
        delayed = int(attrs.get('ApproximateNumberOfMessagesDelayed', 0))

        print(f"  Queue:     {queue_name}")
        print(f"  Pending:   {pending}")
        print(f"  In-flight: {in_flight}")
        print(f"  Delayed:   {delayed}")
        print(f"  Total:     {pending + in_flight + delayed}")
    except Exception as e:
        print(f"  Could not get queue stats: {e}")

    # ASG status
    print("\nWorker Instances:")
    asg_name = f"stt-upload-workers-{environment}"
    try:
        response = asg_client.describe_auto_scaling_groups(
            AutoScalingGroupNames=[asg_name]
        )
        if response['AutoScalingGroups']:
            asg = response['AutoScalingGroups'][0]
            print(f"  ASG:      {asg_name}")
            print(f"  Desired:  {asg['DesiredCapacity']}")
            print(f"  Running:  {len([i for i in asg['Instances'] if i['LifecycleState'] == 'InService'])}")
            print(f"  Min/Max:  {asg['MinSize']}/{asg['MaxSize']}")

            if asg['Instances']:
                print(f"\n  Instances:")
                for inst in asg['Instances']:
                    print(f"    - {inst['InstanceId']} ({inst['InstanceType']}) - {inst['LifecycleState']}")
        else:
            print(f"  ASG {asg_name} not found")
    except Exception as e:
        print(f"  Could not get ASG status: {e}")

    # Stack outputs
    print("\nStack Outputs:")
    outputs = get_stack_outputs(cfn_client)
    for key, value in outputs.items():
        print(f"  {key}: {value}")


def main():
    parser = argparse.ArgumentParser(description='Deploy MinIO-to-S3 upload infrastructure')
    parser.add_argument('--action', choices=['deploy', 'delete', 'status', 'scale'],
                        default='status', help='Action to perform')
    parser.add_argument('--environment', default='prod', help='Environment name')
    parser.add_argument('--count', type=int, default=1, help='Instance count for scale action')
    args = parser.parse_args()

    clients = get_clients()

    print("=" * 60)
    print("MINIO-TO-S3 UPLOAD INFRASTRUCTURE")
    print("=" * 60)

    if args.action == 'deploy':
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
        scale_workers(clients['asg'], args.count, args.environment)

    elif args.action == 'status':
        show_status(clients, args.environment)


if __name__ == '__main__':
    main()
