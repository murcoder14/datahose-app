#!/bin/bash

# Destroy CI/CD Pipeline using CloudFormation
# This script deletes the CodePipeline and CodeBuild resources

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
STACK_NAME="${PIPELINE_STACK_NAME:-datahose-app-pipeline}"
APP_NAME="${APP_NAME:-datahose-app}"

# Get region from AWS CLI default profile configuration
REGION=$(aws configure get region 2>/dev/null)
if [ -z "$REGION" ]; then
    log_error "No default region configured. Please run: aws configure set region <your-region>"
    exit 1
fi

log_warn "=============================================="
log_warn "WARNING: This will destroy the CI/CD pipeline!"
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

# Get stack outputs to determine resource names
log_info "Retrieving stack information..."
OUTPUTS=$(aws cloudformation describe-stacks \
    --stack-name "${STACK_NAME}" \
    --region "${REGION}" \
    --query 'Stacks[0].Outputs' 2>/dev/null || echo "[]")

PIPELINE_NAME=$(echo "${OUTPUTS}" | jq -r '.[] | select(.OutputKey=="PipelineName") | .OutputValue' 2>/dev/null || echo "")
CODEBUILD_PROJECT=$(echo "${OUTPUTS}" | jq -r '.[] | select(.OutputKey=="CodeBuildProjectName") | .OutputValue' 2>/dev/null || echo "")
ARTIFACT_BUCKET=$(echo "${OUTPUTS}" | jq -r '.[] | select(.OutputKey=="ArtifactBucketName") | .OutputValue' 2>/dev/null || echo "")

log_warn "Stack to be deleted: ${STACK_NAME}"
log_warn "Status: ${STACK_EXISTS}"
echo ""
log_warn "Resources to be removed:"
if [ -n "${PIPELINE_NAME}" ]; then
    echo "  - CodePipeline: ${PIPELINE_NAME}"
fi
if [ -n "${CODEBUILD_PROJECT}" ]; then
    echo "  - CodeBuild Project: ${CODEBUILD_PROJECT}"
fi
if [ -n "${ARTIFACT_BUCKET}" ]; then
    echo "  - S3 Artifact Bucket: ${ARTIFACT_BUCKET} (and all contents)"
fi
echo "  - IAM Roles and Policies"
echo "  - GitHub Webhook"
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

log_info "Starting pipeline destruction..."

# Step 1: Stop any running pipeline executions
if [ -n "${PIPELINE_NAME}" ]; then
    log_info "Checking for running pipeline executions..."
    
    RUNNING_EXECUTIONS=$(aws codepipeline list-pipeline-executions \
        --pipeline-name "${PIPELINE_NAME}" \
        --region "${REGION}" \
        --query "pipelineExecutionSummaries[?status=='InProgress'].pipelineExecutionId" \
        --output text 2>/dev/null || echo "")
    
    if [ -n "${RUNNING_EXECUTIONS}" ]; then
        log_info "Found running executions. Stopping them..."
        for EXEC_ID in ${RUNNING_EXECUTIONS}; do
            aws codepipeline stop-pipeline-execution \
                --pipeline-name "${PIPELINE_NAME}" \
                --pipeline-execution-id "${EXEC_ID}" \
                --reason "Pipeline stack being deleted" \
                --region "${REGION}" 2>/dev/null || true
            log_info "Stopped execution: ${EXEC_ID}"
        done
        
        # Wait a bit for executions to stop
        sleep 5
    else
        log_info "No running executions found."
    fi
fi

# Step 2: Stop any running CodeBuild builds
if [ -n "${CODEBUILD_PROJECT}" ]; then
    log_info "Checking for running builds..."
    
    RUNNING_BUILDS=$(aws codebuild list-builds-for-project \
        --project-name "${CODEBUILD_PROJECT}" \
        --region "${REGION}" \
        --query "ids" \
        --output text 2>/dev/null || echo "")
    
    if [ -n "${RUNNING_BUILDS}" ]; then
        for BUILD_ID in ${RUNNING_BUILDS}; do
            BUILD_STATUS=$(aws codebuild batch-get-builds \
                --ids "${BUILD_ID}" \
                --region "${REGION}" \
                --query "builds[0].buildStatus" \
                --output text 2>/dev/null || echo "")
            
            if [ "${BUILD_STATUS}" == "IN_PROGRESS" ]; then
                log_info "Stopping build: ${BUILD_ID}"
                aws codebuild stop-build \
                    --id "${BUILD_ID}" \
                    --region "${REGION}" 2>/dev/null || true
            fi
        done
        
        # Wait a bit for builds to stop
        sleep 5
    else
        log_info "No running builds found."
    fi
fi

# Step 3: Empty the S3 artifact bucket
# CloudFormation cannot delete non-empty buckets, so we need to empty it first

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

if [ -n "${ARTIFACT_BUCKET}" ]; then
    empty_s3_bucket "${ARTIFACT_BUCKET}"
fi

# Step 4: Delete CloudFormation stack
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

# Output summary
log_info "=============================================="
log_info "CI/CD Pipeline Destruction Complete!"
log_info "=============================================="
echo ""
log_info "All pipeline resources have been removed:"
echo "  ✓ CodePipeline deleted"
echo "  ✓ CodeBuild project deleted"
echo "  ✓ S3 Artifact bucket deleted"
echo "  ✓ IAM Roles and Policies deleted"
echo "  ✓ GitHub Webhook removed"
echo "  ✓ CloudFormation Stack deleted"
echo ""
log_info "Note: The infrastructure stack (application resources) is still running."
log_info "To destroy infrastructure, run: ./destroy-infrastructure.sh"
echo ""
log_info "Cleanup completed successfully."
