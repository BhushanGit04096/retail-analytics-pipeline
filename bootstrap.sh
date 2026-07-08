cat > bootstrap.sh << 'EOF'
#!/bin/bash
set -e

PROJECT_ID=$(gcloud config get-value project)
REGION="us-central1"
BUCKET_NAME="retail-raw-${PROJECT_ID}"
DATASET_NAME="retail_warehouse"

echo "========================================="
echo "Bootstrapping GCP resources"
echo "Project ID: $PROJECT_ID"
echo "Bucket: gs://$BUCKET_NAME"
echo "Dataset: $DATASET_NAME"
echo "========================================="

# Create GCS bucket
gsutil mb -l $REGION "gs://$BUCKET_NAME" 2>/dev/null || echo "Bucket already exists"

# Create BigQuery dataset
bq mk --location=$REGION --dataset $PROJECT_ID:$DATASET_NAME 2>/dev/null || echo "Dataset already exists"

echo "========================================="
echo "Bootstrap complete!"
echo "========================================="
EOF

chmod +x bootstrap.sh
