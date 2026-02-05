# STT Audio Pipeline - Implementation Plan

## Project Overview
Scale audio splitting pipeline to process 85,700 hours of Tibetan audio from MinIO using AWS EC2 Spot instances with SQS job orchestration.

---

## Phase 1: Infrastructure Setup

### 1.1 AWS CloudFormation Stack
- [ ] Fix Security Group VPC issue in CloudFormation template
- [ ] Deploy CloudFormation stack successfully
- [ ] Verify IAM roles and permissions
- [ ] Confirm Auto Scaling Group configuration

### 1.2 AWS SQS Queues
- [x] Create `stt-split-jobs` queue
- [x] Create `stt-download-jobs` queue
- [x] Configure queue attributes (visibility timeout, retention)

### 1.3 AWS S3 Output Bucket
- [x] Verify `monlamai-stt-raw-audio` bucket exists
- [x] Test upload permissions with Monlam credentials
- [x] Configure bucket for segment storage

---

## Phase 2: Core Pipeline Testing

### 2.1 Local Pipeline Test
- [x] Download sample audio from MinIO
- [x] Split with Silero VAD (30s max segments)
- [x] Upload segments to AWS S3
- [x] Verify metadata.json generation

### 2.2 SQS Integration Test
- [x] Enqueue test job to SQS
- [x] Process job from queue
- [x] Delete message after completion
- [x] Verify end-to-end flow

---

## Phase 3: EC2 Spot Scaling (Current)

### 3.1 CloudFormation Deployment
- [ ] Delete failed stack (ROLLBACK_COMPLETE)
- [ ] Fix security group configuration (remove VpcId requirement)
- [ ] Redeploy CloudFormation stack
- [ ] Verify all resources created

### 3.2 Launch Template Verification
- [ ] Confirm User Data script correctness
- [ ] Verify AMI selection (Amazon Linux 2023)
- [ ] Test instance launch manually

### 3.3 Auto Scaling Configuration
- [ ] Verify scaling policy based on SQS queue depth
- [ ] Test scale-out (add instances when jobs queued)
- [ ] Test scale-in (remove instances when queue empty)
- [ ] Configure scale-to-zero when idle

### 3.4 1000-File Stress Test
- [ ] Enqueue 1000 jobs from MinIO `amdo` collection
- [ ] Monitor Auto Scaling (expect 5-10 instances)
- [ ] Track processing progress
- [ ] Verify all 1000 files processed
- [ ] Confirm segments in S3 with metadata
- [ ] Validate ASG scales back to 0

---

## Phase 4: Full Production Run

### 4.1 Collection Processing Order
| Priority | Collection | Files | Hours | Est. Time |
|----------|------------|-------|-------|-----------|
| 1 | amdo | 290 | 145 | ~30 min |
| 2 | kham | 278 | 138 | ~30 min |
| 3 | utsang | 648 | 328 | ~1 hour |
| 4 | loc | 1,420 | 4,771 | ~3 hours |
| 5 | other | 45,891 | 3,916 | ~8 hours |
| 6 | voa | 83,678 | 76,390 | ~15 hours |

### 4.2 Monitoring & Verification
- [ ] Set up CloudWatch dashboard
- [ ] Monitor queue depth in real-time
- [ ] Track instance count and health
- [ ] Verify S3 output integrity
- [ ] Log processing errors to DLQ

### 4.3 Cost Tracking
- [ ] Monitor Spot instance costs
- [ ] Track S3 storage growth
- [ ] Calculate cost per hour of audio processed

---

## Phase 5: Post-Processing

### 5.1 Data Validation
- [ ] Verify segment counts match source files
- [ ] Check all segments <= 30 seconds
- [ ] Validate metadata.json for each file
- [ ] Spot-check audio quality

### 5.2 Documentation
- [x] Update README with pipeline description
- [x] Push code to MonlamAI/stt-audio-pipeline
- [ ] Document deployment process
- [ ] Create runbook for operations

---

## Quick Commands

```bash
# Deploy infrastructure
python deploy/scripts/deploy_stack.py --action deploy

# Enqueue jobs (dry-run first)
python deploy/scripts/enqueue_batch.py --dry-run --limit 1000 --collection amdo

# Enqueue for real
python deploy/scripts/enqueue_batch.py --limit 1000 --collection amdo

# Monitor progress
python deploy/scripts/monitor_progress.py --interval 30

# Scale workers manually (if needed)
python deploy/scripts/deploy_stack.py --action scale --count 10

# Delete stack (cleanup)
python deploy/scripts/deploy_stack.py --action delete
```

---

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  MinIO Source   │────▶│   AWS SQS       │────▶│  EC2 Spot       │
│  (s3.monlam.ai) │     │  (split-jobs)   │     │  Workers (N)    │
│  132,206 files  │     │                 │     │                 │
└─────────────────┘     └─────────────────┘     └────────┬────────┘
                                                         │
                                                         ▼
                                               ┌─────────────────┐
                                               │   AWS S3        │
                                               │  (segments)     │
                                               │  monlamai-stt-  │
                                               │  raw-audio      │
                                               └─────────────────┘
```

---

## Credentials Reference

| Service | Access Key | Notes |
|---------|------------|-------|
| MinIO (source) | monlam | Read-only, audio bucket |
| AWS (SQS/S3/EC2) | AKIAUM7ZHR7YXNP56SM2 | Full access to stt resources |

---

## Repository

**GitHub:** https://github.com/MonlamAI/stt-audio-pipeline

---

*Last updated: February 5, 2026*
