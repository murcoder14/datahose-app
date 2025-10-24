#!/bin/bash

# Destroy Infrastructure using CloudFormation
# This script deletes all AWS infrastructure for the Flink Streaming Application
# using CloudFormation (replacement for iac_destroy.sh)

set -e  # Exit on error

# Check for force flag
FORCE_MODE=false
if [ "$1" == "--force" ]; then
    FORCE_MODE=true
fi

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Configuration
STACK_NAME="${STACK_NAME:-datahose-app-infrastructure}"
APP_NAME="${APP_NAME:-datahose-app}"

# Get region from AWS CLI default profile configuration
REGION=$(aws configure get region 2>/dev/null)
if [ -z "$REGION" ]; then
    log_error "No default region configured. Please run: aws configure set region <your-region>"
    exit 1
fi

log_warn "=============================================="
log_warn "WARNING: This will destroy all infrastructure!"
log_warn "=============================================="
echo ""

# Check if AWS CLI is installed
if ! command -v aws &> /dev/null; then
    log_error "AWS CLI is not installed. Please install it first."
    exit 1
fi

# Verify AWS credentials
log_info "Verifying AWS credentials..."
if ! aws sts get-caller-identity &> /dev/null; then
    log_error "AWS credentials are not configured properly."
    exit 1
fi

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
log_info "AWS Account ID: ${ACCOUNT_ID}"

# Check if stack exists
STACK_EXISTS=$(aws cloudformation describe-stacks \
    --stack-name "${STACK_NAME}" \
    --region "${REGION}" \
    --query 'Stacks[0].StackStatus' \
    --output text 2>/dev/null || echo "DOES_NOT_EXIST")

if [ "${STACK_EXISTS}" == "DOES_NOT_EXIST" ]; then
    log_error "Stack ${STACK_NAME} does not exist."
    exit 1
fi

# Get stack outputs to determine bucket names
log_info "Retrieving stack information..."
OUTPUTS=$(aws cloudformation describe-stacks \
    --stack-name "${STACK_NAME}" \
    --region "${REGION}" \
    --query 'Stacks[0].Outputs' 2>/dev/null || echo "[]")

STREAMING_APP_BUCKET=$(echo "${OUTPUTS}" | jq -r '.[] | select(.OutputKey=="StreamingAppBucketName") | .OutputValue' 2>/dev/null || echo "")
DATA_BUCKET=$(echo "${OUTPUTS}" | jq -r '.[] | select(.OutputKey=="DataBucketName") | .OutputValue' 2>/dev/null || echo "")
KINESIS_STREAM_NAME=$(echo "${OUTPUTS}" | jq -r '.[] | select(.OutputKey=="KinesisStreamName") | .OutputValue' 2>/dev/null || echo "")

log_warn "Stack to be deleted: ${STACK_NAME}"
log_warn "Status: ${STACK_EXISTS}"
echo ""
log_warn "Resources to be removed:"
if [ -n "${STREAMING_APP_BUCKET}" ]; then
    echo "  - S3 Bucket: ${STREAMING_APP_BUCKET} (and all contents)"
fi
if [ -n "${DATA_BUCKET}" ]; then
    echo "  - S3 Bucket: ${DATA_BUCKET} (and all contents)"
fi
if [ -n "${KINESIS_STREAM_NAME}" ]; then
    echo "  - Kinesis Data Stream: ${KINESIS_STREAM_NAME}"
fi
echo "  - IAM Roles and Policies"
echo "  - CloudWatch Log Groups"
echo "  - Flink Application: ${APP_NAME} (if exists)"
echo ""

# Prompt for confirmation
if [ "$FORCE_MODE" == "false" ]; then
    read -p "Are you sure you want to continue? (yes/no): " CONFIRM < /dev/tty || CONFIRM="no"
    if [ "$CONFIRM" != "yes" ]; then
        log_info "Destruction cancelled."
        exit 0
    fi
else
    log_warn "Running in FORCE mode - skipping confirmation."
fi

log_info "Starting infrastructure destruction..."

# Step 1: Stop and delete Flink application if it exists
log_info "Checking for Flink application: ${APP_NAME}..."
set +e
APP_EXISTS=$(aws kinesisanalyticsv2 list-applications \
    --region "${REGION}" \
    --query "ApplicationSummaries[?ApplicationName=='${APP_NAME}'].ApplicationName" \
    --output text 2>/dev/null)
set -e

if [ -n "${APP_EXISTS}" ]; then
    log_info "Found Flink application. Checking status..."
    
    APP_STATUS=$(aws kinesisanalyticsv2 describe-application \
        --application-name "${APP_NAME}" \
        --region "${REGION}" \
        --query 'ApplicationDetail.ApplicationStatus' \
        --output text 2>/dev/null || echo "UNKNOWN")
    
    log_info "Application status: ${APP_STATUS}"
    
    # Stop the application if it's running
    if [ "${APP_STATUS}" == "RUNNING" ] || [ "${APP_STATUS}" == "STARTING" ]; then
        log_info "Stopping Flink application..."
        aws kinesisanalyticsv2 stop-application \
            --application-name "${APP_NAME}" \
            --region "${REGION}" \
            --force 2>&1 || log_warn "Failed to stop application gracefully, continuing..."
        
        # Wait for application to stop
        log_info "Waiting for application to stop..."
        for i in {1..30}; do
            APP_STATUS=$(aws kinesisanalyticsv2 describe-application \
                --application-name "${APP_NAME}" \
                --region "${REGION}" \
                --query 'ApplicationDetail.ApplicationStatus' \
                --output text 2>/dev/null || echo "DELETED")
            
            if [ "${APP_STATUS}" == "READY" ] || [ "${APP_STATUS}" == "DELETED" ]; then
                log_info "Application stopped."
                break
            fi
            
            log_info "Current status: ${APP_STATUS}. Waiting... (${i}/30)"
            sleep 10
        done
    fi
    
    # Delete the application
    log_info "Deleting Flink application..."
    CREATE_TIMESTAMP=$(aws kinesisanalyticsv2 describe-application \
        --application-name "${APP_NAME}" \
        --region "${REGION}" \
        --query 'ApplicationDetail.CreateTimestamp' \
        --output text 2>/dev/null || echo "")
    
    if [ -n "${CREATE_TIMESTAMP}" ]; then
        if aws kinesisanalyticsv2 delete-application \
            --application-name "${APP_NAME}" \
            --region "${REGION}" \
            --create-timestamp "${CREATE_TIMESTAMP}" 2>&1; then
            log_info "Flink application deleted successfully."
        else
            log_error "Failed to delete application. Manual cleanup may be required."
        fi
    fi
else
    log_warn "Flink application ${APP_NAME} not found. Skipping..."
fi

# Step 2: Empty S3 buckets before deleting stack
# CloudFormation cannot delete non-empty buckets, so we need to empty them first

empty_s3_bucket() {
    local BUCKET_NAME=$1
    
    if [ -z "${BUCKET_NAME}" ]; then
        return 0
    fi
    
    log_info "Emptying S3 bucket: ${BUCKET_NAME}..."
    
    if ! aws s3 ls "s3://${BUCKET_NAME}" --region "${REGION}" &> /dev/null; then
        log_warn "Bucket ${BUCKET_NAME} not found. Skipping..."
        return 0
    fi
    
    # Delete all object versions
    log_info "Deleting object versions from ${BUCKET_NAME}..."
    aws s3api list-object-versions \
        --bucket "${BUCKET_NAME}" \
        --region "${REGION}" \
        --output json 2>/dev/null | \
    jq -r '.Versions[]? | "--key \"" + .Key + "\" --version-id \"" + .VersionId + "\""' | \
    while read -r args; do
        if [ -n "$args" ]; then
            eval aws s3api delete-object --bucket "${BUCKET_NAME}" --region "${REGION}" $args 2>&1 > /dev/null || true
        fi
    done
    
    # Delete all delete markers
    log_info "Deleting delete markers from ${BUCKET_NAME}..."
    aws s3api list-object-versions \
        --bucket "${BUCKET_NAME}" \
        --region "${REGION}" \
        --output json 2>/dev/null | \
    jq -r '.DeleteMarkers[]? | "--key \"" + .Key + "\" --version-id \"" + .VersionId + "\""' | \
    while read -r args; do
        if [ -n "$args" ]; then
            eval aws s3api delete-object --bucket "${BUCKET_NAME}" --region "${REGION}" $args 2>&1 > /dev/null || true
        fi
    done
    
    log_info "Bucket ${BUCKET_NAME} emptied successfully."
}

if [ -n "${STREAMING_APP_BUCKET}" ]; then
    empty_s3_bucket "${STREAMING_APP_BUCKET}"
fi

if [ -n "${DATA_BUCKET}" ]; then
    empty_s3_bucket "${DATA_BUCKET}"
fi

# Step 3: Delete CloudFormation stack
log_info "Deleting CloudFormation stack: ${STACK_NAME}..."

aws cloudformation delete-stack \
    --stack-name "${STACK_NAME}" \
    --region "${REGION}"

log_info "Stack deletion initiated. Waiting for completion..."

# Wait for stack deletion
aws cloudformation wait stack-delete-complete \
    --stack-name "${STACK_NAME}" \
    --region "${REGION}" 2>&1 || {
    FINAL_STATUS=$(aws cloudformation describe-stacks \
        --stack-name "${STACK_NAME}" \
        --region "${REGION}" \
        --query 'Stacks[0].StackStatus' \
        --output text 2>/dev/null || echo "DELETE_COMPLETE")
    
    if [ "${FINAL_STATUS}" == "DELETE_COMPLETE" ]; then
        log_info "Stack deleted successfully!"
    else
        log_error "Stack deletion failed with status: ${FINAL_STATUS}"
        log_error "Check the CloudFormation console for details."
        exit 1
    fi
}

# Clean up configuration file
if [ -f "/tmp/flink-config.env" ]; then
    rm -f /tmp/flink-config.env
    log_info "Configuration file cleaned up."
fi

# Output summary
log_info "=============================================="
log_info "Infrastructure Destruction Complete!"
log_info "=============================================="
echo ""
log_info "All resources have been removed:"
echo "  ✓ S3 Buckets deleted"
echo "  ✓ Kinesis Data Stream deleted"
echo "  ✓ IAM Roles and Policies deleted"
echo "  ✓ CloudWatch Log Groups deleted"
echo "  ✓ Flink Application deleted"
echo "  ✓ CloudFormation Stack deleted"
echo ""
log_info "Cleanup completed successfully."
