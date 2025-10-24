#!/bin/bash

# Deploy Infrastructure using CloudFormation
# This script creates all AWS infrastructure for the Flink Streaming Application
# using CloudFormation (replacement for iac_create.sh)

set -e  # Exit on error

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
TEMPLATE_FILE="$(dirname "$0")/infrastructure.yaml"
APP_NAME="${APP_NAME:-datahose-app}"
TABLE_NAME="${TABLE_NAME:-datafall}"
KINESIS_STREAM_NAME="${KINESIS_STREAM_NAME:-tm-input-stream}"
KINESIS_SHARD_COUNT="${KINESIS_SHARD_COUNT:-1}"
LOG_RETENTION_DAYS="${LOG_RETENTION_DAYS:-7}"
KINESIS_PRODUCER_USER="${KINESIS_PRODUCER_USER:-sunny0524}"

# Get region from AWS CLI default profile configuration
REGION=$(aws configure get region 2>/dev/null)
if [ -z "$REGION" ]; then
    log_error "No default region configured. Please run: aws configure set region <your-region>"
    exit 1
fi

log_info "=============================================="
log_info "Deploying Infrastructure with CloudFormation"
log_info "=============================================="
echo ""
log_info "Configuration:"
echo "  - Stack Name: ${STACK_NAME}"
echo "  - Application Name: ${APP_NAME}"
echo "  - Region: ${REGION}"
echo "  - Kinesis Stream: ${KINESIS_STREAM_NAME}"
echo "  - Table Name: ${TABLE_NAME}"
echo "  - Template: ${TEMPLATE_FILE}"
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

# Check if template file exists
if [ ! -f "${TEMPLATE_FILE}" ]; then
    log_error "Template file not found: ${TEMPLATE_FILE}"
    exit 1
fi

# Validate CloudFormation template
log_info "Validating CloudFormation template..."
if ! aws cloudformation validate-template \
    --template-body "file://${TEMPLATE_FILE}" \
    --region "${REGION}" &> /dev/null; then
    log_error "Template validation failed!"
    exit 1
fi
log_info "Template validation successful."

# Check if stack exists
STACK_EXISTS=$(aws cloudformation describe-stacks \
    --stack-name "${STACK_NAME}" \
    --region "${REGION}" \
    --query 'Stacks[0].StackStatus' \
    --output text 2>/dev/null || echo "DOES_NOT_EXIST")

if [ "${STACK_EXISTS}" != "DOES_NOT_EXIST" ]; then
    log_warn "Stack ${STACK_NAME} already exists with status: ${STACK_EXISTS}"
    
    if [ "${STACK_EXISTS}" == "ROLLBACK_COMPLETE" ]; then
        log_error "Stack is in ROLLBACK_COMPLETE state. Please delete it first:"
        log_error "  ./destroy-infrastructure.sh"
        exit 1
    fi
    
    log_info "Updating existing stack..."
    OPERATION="update-stack"
    
    # Try to update the stack
    if aws cloudformation update-stack \
        --stack-name "${STACK_NAME}" \
        --template-body "file://${TEMPLATE_FILE}" \
        --parameters \
            ParameterKey=ApplicationName,ParameterValue="${APP_NAME}" \
            ParameterKey=TableName,ParameterValue="${TABLE_NAME}" \
            ParameterKey=KinesisStreamName,ParameterValue="${KINESIS_STREAM_NAME}" \
            ParameterKey=KinesisShardCount,ParameterValue="${KINESIS_SHARD_COUNT}" \
            ParameterKey=LogRetentionDays,ParameterValue="${LOG_RETENTION_DAYS}" \
            ParameterKey=KinesisProducerUserName,ParameterValue="${KINESIS_PRODUCER_USER}" \
        --capabilities CAPABILITY_NAMED_IAM \
        --region "${REGION}" 2>&1 | tee /tmp/cfn-update.log; then
        
        log_info "Stack update initiated. Waiting for completion..."
    else
        if grep -q "No updates are to be performed" /tmp/cfn-update.log; then
            log_warn "No updates are required for the stack."
            OPERATION="none"
        else
            log_error "Stack update failed. Check the error above."
            exit 1
        fi
    fi
else
    log_info "Creating new stack..."
    OPERATION="create-stack"
    
    aws cloudformation create-stack \
        --stack-name "${STACK_NAME}" \
        --template-body "file://${TEMPLATE_FILE}" \
        --parameters \
            ParameterKey=ApplicationName,ParameterValue="${APP_NAME}" \
            ParameterKey=TableName,ParameterValue="${TABLE_NAME}" \
            ParameterKey=KinesisStreamName,ParameterValue="${KINESIS_STREAM_NAME}" \
            ParameterKey=KinesisShardCount,ParameterValue="${KINESIS_SHARD_COUNT}" \
            ParameterKey=LogRetentionDays,ParameterValue="${LOG_RETENTION_DAYS}" \
            ParameterKey=KinesisProducerUserName,ParameterValue="${KINESIS_PRODUCER_USER}" \
        --capabilities CAPABILITY_NAMED_IAM \
        --region "${REGION}"
    
    log_info "Stack creation initiated. Waiting for completion..."
fi

# Wait for stack operation to complete
if [ "${OPERATION}" == "create-stack" ]; then
    aws cloudformation wait stack-create-complete \
        --stack-name "${STACK_NAME}" \
        --region "${REGION}"
    log_info "Stack created successfully!"
elif [ "${OPERATION}" == "update-stack" ]; then
    aws cloudformation wait stack-update-complete \
        --stack-name "${STACK_NAME}" \
        --region "${REGION}" || {
        FINAL_STATUS=$(aws cloudformation describe-stacks \
            --stack-name "${STACK_NAME}" \
            --region "${REGION}" \
            --query 'Stacks[0].StackStatus' \
            --output text)
        
        if [ "${FINAL_STATUS}" == "UPDATE_COMPLETE" ]; then
            log_info "Stack updated successfully!"
        else
            log_error "Stack update failed with status: ${FINAL_STATUS}"
            exit 1
        fi
    }
    log_info "Stack updated successfully!"
fi

# Retrieve stack outputs
log_info "Retrieving stack outputs..."
OUTPUTS=$(aws cloudformation describe-stacks \
    --stack-name "${STACK_NAME}" \
    --region "${REGION}" \
    --query 'Stacks[0].Outputs')

STREAMING_APP_BUCKET=$(echo "${OUTPUTS}" | jq -r '.[] | select(.OutputKey=="StreamingAppBucketName") | .OutputValue')
DATA_BUCKET=$(echo "${OUTPUTS}" | jq -r '.[] | select(.OutputKey=="DataBucketName") | .OutputValue')
KINESIS_STREAM_ARN=$(echo "${OUTPUTS}" | jq -r '.[] | select(.OutputKey=="KinesisStreamArn") | .OutputValue')
FLINK_ROLE_ARN=$(echo "${OUTPUTS}" | jq -r '.[] | select(.OutputKey=="FlinkRoleArn") | .OutputValue')
LOG_GROUP=$(echo "${OUTPUTS}" | jq -r '.[] | select(.OutputKey=="FlinkLogGroupName") | .OutputValue')
LOG_STREAM=$(echo "${OUTPUTS}" | jq -r '.[] | select(.OutputKey=="FlinkLogStreamName") | .OutputValue')

# Output summary
log_info "=============================================="
log_info "Infrastructure Deployment Complete!"
log_info "=============================================="
echo ""
log_info "Stack Details:"
echo "  - Stack Name: ${STACK_NAME}"
echo "  - Stack Status: CREATE_COMPLETE or UPDATE_COMPLETE"
echo ""
log_info "Resources Created:"
echo "  - S3 Bucket (Application JAR): ${STREAMING_APP_BUCKET}"
echo "  - S3 Bucket (Data Sink): ${DATA_BUCKET}"
echo "  - S3 Table: ${TABLE_NAME}"
echo "  - Kinesis Data Stream: ${KINESIS_STREAM_NAME}"
echo "  - Kinesis Stream ARN: ${KINESIS_STREAM_ARN}"
echo "  - IAM Role ARN: ${FLINK_ROLE_ARN}"
echo "  - CloudWatch Log Group: ${LOG_GROUP}"
echo "  - CloudWatch Log Stream: ${LOG_STREAM}"
echo ""
log_info "Environment Variables (save these for use in CI/CD):"
echo "export FLINK_ROLE_ARN=\"${FLINK_ROLE_ARN}\""
echo "export STREAMING_APP_BUCKET=\"${STREAMING_APP_BUCKET}\""
echo "export DATA_BUCKET=\"${DATA_BUCKET}\""
echo "export KINESIS_STREAM_NAME=\"${KINESIS_STREAM_NAME}\""
echo "export KINESIS_STREAM_ARN=\"${KINESIS_STREAM_ARN}\""
echo "export LOG_GROUP=\"${LOG_GROUP}\""
echo "export LOG_STREAM=\"${LOG_STREAM}\""
echo "export APP_NAME=\"${APP_NAME}\""
echo "export REGION=\"${REGION}\""
echo "export S3_TABLE=\"${TABLE_NAME}\""
echo "export STACK_NAME=\"${STACK_NAME}\""
echo ""

# Save configuration to file
cat > /tmp/flink-config.env <<EOF
export FLINK_ROLE_ARN="${FLINK_ROLE_ARN}"
export STREAMING_APP_BUCKET="${STREAMING_APP_BUCKET}"
export DATA_BUCKET="${DATA_BUCKET}"
export KINESIS_STREAM_NAME="${KINESIS_STREAM_NAME}"
export KINESIS_STREAM_ARN="${KINESIS_STREAM_ARN}"
export LOG_GROUP="${LOG_GROUP}"
export LOG_STREAM="${LOG_STREAM}"
export APP_NAME="${APP_NAME}"
export REGION="${REGION}"
export S3_TABLE="${TABLE_NAME}"
export STACK_NAME="${STACK_NAME}"
EOF

log_info "Configuration saved to /tmp/flink-config.env"
log_info "Source this file: source /tmp/flink-config.env"
echo ""
log_info "Next steps:"
echo "  1. Source the configuration: source /tmp/flink-config.env"
echo "  2. Build and deploy your application using the CI/CD pipeline"
echo "  3. Or manually run: ./cicd.sh"
