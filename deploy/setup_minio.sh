#!/bin/bash
# Setup script for MinIO buckets (Monlam Audio Processing)

set -e

# Monlam MinIO Configuration
MINIO_ALIAS="${MINIO_ALIAS:-monlam}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-https://s3.monlam.ai}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-monlam}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-ScL4mottIF1zDp8RLy9m8s2UafLgqIiw}"

echo "Setting up MinIO for Monlam STT audio processing..."
echo "Endpoint: $MINIO_ENDPOINT"

# Configure MinIO client alias (with --insecure for self-signed cert)
echo "Configuring MinIO client..."
mc alias set $MINIO_ALIAS $MINIO_ENDPOINT $MINIO_ACCESS_KEY $MINIO_SECRET_KEY --insecure

# List existing buckets
echo ""
echo "Existing buckets:"
mc --insecure ls $MINIO_ALIAS/

# Show audio collections
echo ""
echo "Audio collections in 'audio' bucket:"
mc --insecure ls $MINIO_ALIAS/audio/

# Create segments bucket for output
echo ""
echo "Creating segments bucket for output..."
mc --insecure mb --ignore-existing $MINIO_ALIAS/segments
echo "  Created: segments (for split audio segments)"

# Show collection sizes
echo ""
echo "Collection sizes:"
for collection in voa other loc utsang amdo kham; do
    echo -n "  $collection: "
    mc --insecure du $MINIO_ALIAS/audio/$collection/ 2>/dev/null | tail -1 || echo "N/A"
done

echo ""
echo "MinIO setup complete!"
echo ""
echo "Quick start commands:"
echo ""
echo "# List files in a collection:"
echo "mc --insecure ls $MINIO_ALIAS/audio/amdo/"
echo ""
echo "# Schedule split jobs for specific collections:"
echo "python -m queue.scheduler --source bucket --bucket audio --collections amdo,kham,utsang"
echo ""
echo "# Schedule split jobs for ALL collections (85K+ hours!):"
echo "python -m queue.scheduler --source bucket --bucket audio"
echo ""
echo "# Dry run to see what would be scheduled:"
echo "python -m queue.scheduler --source bucket --collections amdo --dry-run --limit 100"
