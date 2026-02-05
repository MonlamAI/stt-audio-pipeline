# STT Audio Pipeline

Scalable audio splitting pipeline for Speech-to-Text data preparation. Downloads audio from MinIO, splits using Silero VAD into segments (max 30 seconds), and uploads to AWS S3.

**Key Features:**
- AWS SQS job queue for distributed processing
- EC2 Spot Auto Scaling (cost-effective for large batches)
- Silero VAD for accurate speech detection
- Supports 75,000+ hours of audio processing

## Quick Start

```bash
# 1. Configure environment
cp deploy/.env.example deploy/.env
# Edit deploy/.env with your credentials

# 2. Deploy infrastructure
python deploy/scripts/deploy_stack.py --action deploy

# 3. Enqueue jobs
python deploy/scripts/enqueue_batch.py --limit 1000 --collection amdo

# 4. Monitor progress
python deploy/scripts/monitor_progress.py
```

## Architecture

```
MinIO (source) -> SQS Queue -> EC2 Spot Workers -> AWS S3 (output)
```

## License

MIT License
