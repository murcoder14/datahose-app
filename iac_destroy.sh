#!/bin/bash

# IaC Script to Destroy AWS Infrastructure for Flink Streaming Application
# This script removes all resources created by iac_create.sh
# Usage: ./iac_destroy.sh [--force]

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
APP_NAME="datahose-app"
# Auto-detect latest dynamic S3 buckets by prefix and creation date
# Load bucket names from environment or config file
if [ -z "$STREAMING_APP_BUCKET" ] || [ -z "$VISITS_INPUT_BUCKET" ] || [ -z "$VISITS_OUTPUT_BUCKET" ] || [ -z "$CLAIMS_OUTPUT_BUCKET" ] || [ -z "$LEAVEREQUESTS_OUTPUT_BUCKET" ] || [ -z "$DEFAULT_OUTPUT_BUCKET" ]; then
    if [ -f "/tmp/flink-config.env" ]; then
        source /tmp/flink-config.env
    fi
fi
if [ -z "$STREAMING_APP_BUCKET" ] || [ -z "$VISITS_INPUT_BUCKET" ] || [ -z "$VISITS_OUTPUT_BUCKET" ] || [ -z "$CLAIMS_OUTPUT_BUCKET" ] || [ -z "$LEAVEREQUESTS_OUTPUT_BUCKET" ] || [ -z "$DEFAULT_OUTPUT_BUCKET" ]; then
    echo -e "${RED}[ERROR]${NC} Required bucket variables not set. Please export them or source /tmp/flink-config.env from your deployment before running this script."
    echo "Required: STREAMING_APP_BUCKET, VISITS_INPUT_BUCKET, VISITS_OUTPUT_BUCKET, CLAIMS_OUTPUT_BUCKET, LEAVEREQUESTS_OUTPUT_BUCKET, DEFAULT_OUTPUT_BUCKET"
    exit 1
fi
# Get region from AWS CLI default profile configuration
REGION=$(aws configure get region 2>/dev/null)
if [ -z "$REGION" ]; then
    echo -e "${RED}[ERROR]${NC} No default region configured. Please run: aws configure set region us-east-2"
    exit 1
fi
IAM_ROLE_NAME="${APP_NAME}-flink-role"
IAM_POLICY_NAME="${APP_NAME}-flink-policy"
USER_POLICY_NAME="${APP_NAME}-s3-upload-policy"
LOG_GROUP_NAME="/aws/kinesis-analytics/${APP_NAME}"
KINESIS_STREAM_NAME="${APP_NAME}-stream"

log_warn "=============================================="
log_warn "WARNING: This will destroy all infrastructure!"
log_warn "=============================================="
echo ""
log_warn "Resources to be deleted:"
echo "  - S3 Bucket: ${STREAMING_APP_BUCKET} (and all contents)"
echo "  - Kinesis Data Stream: ${KINESIS_STREAM_NAME}"
echo "  - S3 Bucket: ${VISITS_INPUT_BUCKET} (and all contents)"
echo "  - S3 Bucket: ${VISITS_OUTPUT_BUCKET} (and all contents)"
echo "  - S3 Bucket: ${CLAIMS_OUTPUT_BUCKET} (and all contents)"
echo "  - S3 Bucket: ${LEAVEREQUESTS_OUTPUT_BUCKET} (and all contents)"
echo "  - S3 Bucket: ${DEFAULT_OUTPUT_BUCKET} (and all contents)"
echo "  - IAM Role: ${IAM_ROLE_NAME}"
echo "  - IAM Policy (Flink): ${IAM_POLICY_NAME}"
echo "  - IAM Policy (S3 Upload): ${USER_POLICY_NAME}"
echo "  - CloudWatch Log Group: ${LOG_GROUP_NAME}"
echo "  - Flink Application: ${APP_NAME}"
echo ""

# Prompt for confirmation (read from terminal explicitly)
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

# Stop and delete Flink application if it exists
log_info "Checking for Flink application: ${APP_NAME}..."

# Check if application exists (temporarily disable exit-on-error for this check)
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
    
    # Delete the application - get the create timestamp first
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
            log_error "Failed to delete application. Please delete manually."
        fi
    else
        log_error "Could not retrieve application timestamp. Cannot delete application."
    fi
else
    log_warn "Flink application ${APP_NAME} not found. Skipping..."
fi

# Function to delete S3 bucket completely
delete_s3_bucket() {
    local BUCKET_NAME=$1
    
    log_info "Deleting S3 bucket: ${BUCKET_NAME}..."
    
    if ! aws s3 ls "s3://${BUCKET_NAME}" --region "${REGION}" &> /dev/null; then
        log_warn "Bucket ${BUCKET_NAME} not found. Skipping..."
        return 0
    fi
    
    # First, try simple force delete (works if versioning is not enabled or simple objects)
    log_info "Attempting simple delete for ${BUCKET_NAME}..."
    if aws s3 rb "s3://${BUCKET_NAME}" --force --region "${REGION}" 2>&1; then
        log_info "Bucket ${BUCKET_NAME} deleted successfully."
        return 0
    fi
    
    log_info "Simple delete failed, removing versioned objects..."
    
    # Delete all object versions
    log_info "Deleting object versions..."
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
    log_info "Deleting delete markers..."
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
    
    # Final bucket deletion
    log_info "Deleting empty bucket..."
    if aws s3api delete-bucket --bucket "${BUCKET_NAME}" --region "${REGION}" 2>&1; then
        log_info "Bucket ${BUCKET_NAME} deleted successfully."
    else
        log_error "Failed to delete bucket ${BUCKET_NAME}. Manual cleanup may be required."
        return 1
    fi
}

# Delete all S3 buckets
delete_s3_bucket "${STREAMING_APP_BUCKET}"
delete_s3_bucket "${VISITS_INPUT_BUCKET}"
delete_s3_bucket "${VISITS_OUTPUT_BUCKET}"
delete_s3_bucket "${CLAIMS_OUTPUT_BUCKET}"
delete_s3_bucket "${LEAVEREQUESTS_OUTPUT_BUCKET}"
delete_s3_bucket "${DEFAULT_OUTPUT_BUCKET}"

# Delete Kinesis Data Stream
log_info "Deleting Kinesis Data Stream: ${KINESIS_STREAM_NAME}..."
if aws kinesis describe-stream --stream-name "${KINESIS_STREAM_NAME}" --region "${REGION}" &> /dev/null; then
    aws kinesis delete-stream \
        --stream-name "${KINESIS_STREAM_NAME}" \
        --region "${REGION}" \
        --enforce-consumer-deletion 2>&1
    log_info "Kinesis Data Stream deletion initiated. It may take a few minutes to complete."
else
    log_warn "Kinesis Data Stream ${KINESIS_STREAM_NAME} not found. Skipping..."
fi

# Detach user policy from sunny0524 and delete user policy
USER_POLICY_ARN="arn:aws:iam::${ACCOUNT_ID}:policy/${USER_POLICY_NAME}"

log_info "Detaching user policy from sunny0524..."
if aws iam get-user --user-name sunny0524 &> /dev/null; then
    aws iam detach-user-policy \
        --user-name sunny0524 \
        --policy-arn "${USER_POLICY_ARN}" 2>/dev/null || log_warn "Policy may not be attached."
    log_info "Policy detached from user."
else
    log_warn "User sunny0524 not found."
fi

log_info "Deleting user IAM policy: ${USER_POLICY_NAME}..."
if aws iam get-policy --policy-arn "${USER_POLICY_ARN}" &> /dev/null; then
    # Delete all non-default versions first
    VERSIONS=$(aws iam list-policy-versions --policy-arn "${USER_POLICY_ARN}" --query 'Versions[?IsDefaultVersion==`false`].VersionId' --output text 2>/dev/null)
    for VERSION in $VERSIONS; do
        aws iam delete-policy-version --policy-arn "${USER_POLICY_ARN}" --version-id "${VERSION}" 2>/dev/null || true
    done
    
    # Delete the policy
    aws iam delete-policy --policy-arn "${USER_POLICY_ARN}" 2>&1
    log_info "User IAM policy deleted."
else
    log_warn "User IAM policy ${USER_POLICY_NAME} not found. Skipping..."
fi

# Detach and delete IAM policy for Flink
POLICY_ARN="arn:aws:iam::${ACCOUNT_ID}:policy/${IAM_POLICY_NAME}"

log_info "Detaching IAM policy from role..."
if aws iam get-role --role-name "${IAM_ROLE_NAME}" &> /dev/null; then
    aws iam detach-role-policy \
        --role-name "${IAM_ROLE_NAME}" \
        --policy-arn "${POLICY_ARN}" 2>/dev/null || log_warn "Policy may not be attached."
    log_info "Policy detached from role."
else
    log_warn "IAM role ${IAM_ROLE_NAME} not found."
fi

# Delete IAM policy
log_info "Deleting IAM policy: ${IAM_POLICY_NAME}..."
if aws iam get-policy --policy-arn "${POLICY_ARN}" &> /dev/null; then
    # Delete all non-default versions first
    VERSIONS=$(aws iam list-policy-versions --policy-arn "${POLICY_ARN}" --query 'Versions[?IsDefaultVersion==`false`].VersionId' --output text 2>/dev/null)
    for VERSION in $VERSIONS; do
        aws iam delete-policy-version --policy-arn "${POLICY_ARN}" --version-id "${VERSION}" 2>/dev/null || true
    done
    
    # Delete the policy
    aws iam delete-policy --policy-arn "${POLICY_ARN}" 2>&1
    log_info "IAM policy deleted."
else
    log_warn "IAM policy ${IAM_POLICY_NAME} not found. Skipping..."
fi

# Delete IAM role
log_info "Deleting IAM role: ${IAM_ROLE_NAME}..."
if aws iam get-role --role-name "${IAM_ROLE_NAME}" &> /dev/null; then
    aws iam delete-role --role-name "${IAM_ROLE_NAME}" 2>&1
    log_info "IAM role deleted."
else
    log_warn "IAM role ${IAM_ROLE_NAME} not found. Skipping..."
fi

# Delete CloudWatch Log Group
log_info "Deleting CloudWatch Log Group: ${LOG_GROUP_NAME}..."
if aws logs describe-log-groups --log-group-name-prefix "${LOG_GROUP_NAME}" --region "${REGION}" 2>/dev/null | grep -q "${LOG_GROUP_NAME}"; then
    aws logs delete-log-group \
        --log-group-name "${LOG_GROUP_NAME}" \
        --region "${REGION}" 2>&1
    log_info "CloudWatch Log Group deleted."
else
    log_warn "CloudWatch Log Group ${LOG_GROUP_NAME} not found. Skipping..."
fi

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
echo "  ✓ S3 Buckets deleted (Application JAR, Visits Input, Visits Output, Claims Output, Leave Requests Output, Default Output)"
echo "  ✓ Kinesis Data Stream deleted"
echo "  ✓ IAM Role and Policies deleted"
echo "  ✓ CloudWatch Log Group deleted"
echo "  ✓ Flink Application deleted"
echo ""
log_info "Cleanup completed successfully."
