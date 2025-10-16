#!/bin/bash

# Test Script for S3 Data Upload
# Uploads gymvisits.csv from inputs folder to S3 input bucket

set -e

# Color codes for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[✓]${NC} $1"
}

log_data() {
    echo -e "${BLUE}[DATA]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[!]${NC} $1"
}

log_error() {
    echo -e "${RED}[✗]${NC} $1"
}

# Configuration
REGION=$(aws configure get region 2>/dev/null)
if [ -z "$REGION" ]; then
    log_error "No default region configured. Please run: aws configure set region us-east-2"
    exit 1
fi

# Load configuration from environment or config file
if [ -z "$INPUT_DATA_BUCKET" ] || [ -z "$INPUT_DATA_BUCKET_TABLE_NAME" ]; then
    if [ -f "/tmp/flink-config.env" ]; then
        source /tmp/flink-config.env
    fi
fi

if [ -z "$INPUT_DATA_BUCKET" ]; then
    log_error "INPUT_DATA_BUCKET not set. Please run ./iac_create.sh first or source /tmp/flink-config.env"
    exit 1
fi

INPUT_TABLE_NAME="${INPUT_DATA_BUCKET_TABLE_NAME:-datafall}"
INPUT_FILE="inputs/gymvisits.csv"

# Check if AWS CLI is installed
if ! command -v aws &> /dev/null; then
    log_error "AWS CLI is not installed. Please install it first."
    exit 1
fi

# Verify AWS credentials
if ! aws sts get-caller-identity &> /dev/null; then
    log_error "AWS credentials are not configured properly."
    exit 1
fi

# Check if input file exists
if [ ! -f "${INPUT_FILE}" ]; then
    log_error "Input file not found: ${INPUT_FILE}"
    log_error "Please ensure gymvisits.csv exists in the inputs folder."
    exit 1
fi

# Verify S3 bucket exists
if ! aws s3 ls "s3://${INPUT_DATA_BUCKET}" --region "${REGION}" &> /dev/null; then
    log_error "S3 bucket ${INPUT_DATA_BUCKET} not found in region ${REGION}"
    log_error "Please run ./iac_create.sh first to create the bucket."
    exit 1
fi

echo "╔════════════════════════════════════════════════════════╗"
echo "║     S3 Data Upload Test                                ║"
echo "╚════════════════════════════════════════════════════════╝"
echo ""
log_info "Input File: ${INPUT_FILE}"
log_info "S3 Bucket: ${INPUT_DATA_BUCKET}"
log_info "S3 Path: s3://${INPUT_DATA_BUCKET}/${INPUT_TABLE_NAME}/"
log_info "Region: ${REGION}"
echo ""

# Get file size and line count
FILE_SIZE=$(du -h "${INPUT_FILE}" | cut -f1)
LINE_COUNT=$(wc -l < "${INPUT_FILE}")

log_data "File size: ${FILE_SIZE}"
log_data "Number of records: ${LINE_COUNT}"
echo ""

# Generate unique filename with timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
S3_KEY="${INPUT_TABLE_NAME}/gymvisits_${TIMESTAMP}.csv"

log_info "Uploading file to S3..."
log_data "S3 Key: ${S3_KEY}"
echo ""

# Upload file to S3
if aws s3 cp "${INPUT_FILE}" "s3://${INPUT_DATA_BUCKET}/${S3_KEY}" --region "${REGION}"; then
    log_info "File uploaded successfully!"
    echo ""
    log_info "=============================================="
    log_info "Upload Complete!"
    log_info "=============================================="
    echo ""
    echo "S3 Location: s3://${INPUT_DATA_BUCKET}/${S3_KEY}"
    echo ""
    log_info "The Flink application will automatically detect and process this file."
    echo ""
    log_info "To monitor processing:"
    echo "  - Check logs: aws logs tail /aws/kinesis-analytics/datahose-app --follow --region ${REGION}"
    echo "  - Check output: aws s3 ls s3://\${OUTPUT_DATA_BUCKET}/\${OUTPUT_DATA_BUCKET_TABLE_NAME}/ --recursive --region ${REGION}"
    echo ""
else
    log_error "Failed to upload file to S3"
    exit 1
fi
