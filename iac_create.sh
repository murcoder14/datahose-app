#!/bin/bash

# IaC Script to Create AWS Infrastructure for Flink Streaming Application
# This script creates S3 buckets, IAM roles, policies, and CloudWatch log resources

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
# Generate date and epoch suffix for unique bucket names
DATE_SUFFIX=$(date +%Y%m%d)
EPOCH_SUFFIX=$(date +%s)
BUCKET_SUFFIX="${DATE_SUFFIX}-${EPOCH_SUFFIX}"

APP_NAME="datahose-app"
STREAMING_APP_BUCKET="tm-streaming-app-bucket-${BUCKET_SUFFIX}"
VISITS_INPUT_BUCKET="tm-visits-input-${BUCKET_SUFFIX}"
VISITS_OUTPUT_BUCKET="tm-visits-output-${BUCKET_SUFFIX}"
CLAIMS_OUTPUT_BUCKET="tm-claims-output-${BUCKET_SUFFIX}"
LEAVEREQUESTS_OUTPUT_BUCKET="tm-leaverequests-output-${BUCKET_SUFFIX}"
DEFAULT_OUTPUT_BUCKET="tm-output-${BUCKET_SUFFIX}"
VISITS_INPUT_KEY="filefall/gymvisits.csv"
KINESIS_STREAM_NAME="${APP_NAME}-stream"
KINESIS_SHARD_COUNT=1

# Get region from AWS CLI default profile configuration
REGION=$(aws configure get region 2>/dev/null)
if [ -z "$REGION" ]; then
    log_error "No default region configured. Please run: aws configure set region us-east-2"
    exit 1
fi
IAM_ROLE_NAME="${APP_NAME}-flink-role"
IAM_POLICY_NAME="${APP_NAME}-flink-policy"
USER_POLICY_NAME="${APP_NAME}-s3-upload-policy"
LOG_GROUP_NAME="/aws/kinesis-analytics/${APP_NAME}"
LOG_STREAM_NAME="flink-application"

log_info "Starting infrastructure creation for ${APP_NAME}..."
log_info "Region: ${REGION}"

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

# Create S3 bucket for Flink application JAR
log_info "Creating S3 bucket for Flink application JAR: ${STREAMING_APP_BUCKET}..."
if aws s3 ls "s3://${STREAMING_APP_BUCKET}" 2>&1 | grep -q 'NoSuchBucket'; then
    aws s3api create-bucket \
        --bucket "${STREAMING_APP_BUCKET}" \
        --region "${REGION}" \
        --create-bucket-configuration LocationConstraint="${REGION}"
    
    # Enable versioning
    aws s3api put-bucket-versioning \
        --bucket "${STREAMING_APP_BUCKET}" \
        --versioning-configuration Status=Enabled
    
    log_info "Bucket ${STREAMING_APP_BUCKET} created successfully."
else
    log_warn "Bucket ${STREAMING_APP_BUCKET} already exists."
fi

# Create S3 bucket for visits input data
log_info "Creating S3 bucket for visits input data: ${VISITS_INPUT_BUCKET}..."
if aws s3 ls "s3://${VISITS_INPUT_BUCKET}" 2>&1 | grep -q 'NoSuchBucket'; then
    aws s3api create-bucket \
        --bucket "${VISITS_INPUT_BUCKET}" \
        --region "${REGION}" \
        --create-bucket-configuration LocationConstraint="${REGION}"
    
    # Enable versioning
    aws s3api put-bucket-versioning \
        --bucket "${VISITS_INPUT_BUCKET}" \
        --versioning-configuration Status=Enabled
    
    log_info "Bucket ${VISITS_INPUT_BUCKET} created successfully."
else
    log_warn "Bucket ${VISITS_INPUT_BUCKET} already exists."
fi

# Create folder structure for input data
log_info "Creating S3 input folder structure..."
INPUT_FOLDER=$(dirname "${VISITS_INPUT_KEY}")
aws s3api put-object \
    --bucket "${VISITS_INPUT_BUCKET}" \
    --key "${INPUT_FOLDER}/" \
    --region "${REGION}" || log_warn "Input folder may already exist."

log_info "S3 input folder ${INPUT_FOLDER} structure created in bucket ${VISITS_INPUT_BUCKET}."

# Create S3 bucket for visits output data
log_info "Creating S3 bucket for visits output data: ${VISITS_OUTPUT_BUCKET}..."
if aws s3 ls "s3://${VISITS_OUTPUT_BUCKET}" 2>&1 | grep -q 'NoSuchBucket'; then
    aws s3api create-bucket \
        --bucket "${VISITS_OUTPUT_BUCKET}" \
        --region "${REGION}" \
        --create-bucket-configuration LocationConstraint="${REGION}"
    
    # Enable versioning
    aws s3api put-bucket-versioning \
        --bucket "${VISITS_OUTPUT_BUCKET}" \
        --versioning-configuration Status=Enabled
    
    log_info "Bucket ${VISITS_OUTPUT_BUCKET} created successfully."
else
    log_warn "Bucket ${VISITS_OUTPUT_BUCKET} already exists."
fi

# Create S3 bucket for claims output data
log_info "Creating S3 bucket for claims output data: ${CLAIMS_OUTPUT_BUCKET}..."
if aws s3 ls "s3://${CLAIMS_OUTPUT_BUCKET}" 2>&1 | grep -q 'NoSuchBucket'; then
    aws s3api create-bucket \
        --bucket "${CLAIMS_OUTPUT_BUCKET}" \
        --region "${REGION}" \
        --create-bucket-configuration LocationConstraint="${REGION}"
    
    # Enable versioning
    aws s3api put-bucket-versioning \
        --bucket "${CLAIMS_OUTPUT_BUCKET}" \
        --versioning-configuration Status=Enabled
    
    log_info "Bucket ${CLAIMS_OUTPUT_BUCKET} created successfully."
else
    log_warn "Bucket ${CLAIMS_OUTPUT_BUCKET} already exists."
fi

# Create S3 bucket for leave requests output data
log_info "Creating S3 bucket for leave requests output data: ${LEAVEREQUESTS_OUTPUT_BUCKET}..."
if aws s3 ls "s3://${LEAVEREQUESTS_OUTPUT_BUCKET}" 2>&1 | grep -q 'NoSuchBucket'; then
    aws s3api create-bucket \
        --bucket "${LEAVEREQUESTS_OUTPUT_BUCKET}" \
        --region "${REGION}" \
        --create-bucket-configuration LocationConstraint="${REGION}"
    
    # Enable versioning
    aws s3api put-bucket-versioning \
        --bucket "${LEAVEREQUESTS_OUTPUT_BUCKET}" \
        --versioning-configuration Status=Enabled
    
    log_info "Bucket ${LEAVEREQUESTS_OUTPUT_BUCKET} created successfully."
else
    log_warn "Bucket ${LEAVEREQUESTS_OUTPUT_BUCKET} already exists."
fi

# Create S3 bucket for default output data (unknown messages)
log_info "Creating S3 bucket for default output data: ${DEFAULT_OUTPUT_BUCKET}..."
if aws s3 ls "s3://${DEFAULT_OUTPUT_BUCKET}" 2>&1 | grep -q 'NoSuchBucket'; then
    aws s3api create-bucket \
        --bucket "${DEFAULT_OUTPUT_BUCKET}" \
        --region "${REGION}" \
        --create-bucket-configuration LocationConstraint="${REGION}"
    
    # Enable versioning
    aws s3api put-bucket-versioning \
        --bucket "${DEFAULT_OUTPUT_BUCKET}" \
        --versioning-configuration Status=Enabled
    
    log_info "Bucket ${DEFAULT_OUTPUT_BUCKET} created successfully."
else
    log_warn "Bucket ${DEFAULT_OUTPUT_BUCKET} already exists."
fi

# Create Kinesis Data Stream
log_info "Creating Kinesis Data Stream: ${KINESIS_STREAM_NAME}..."
if aws kinesis describe-stream --stream-name "${KINESIS_STREAM_NAME}" --region "${REGION}" &> /dev/null; then
    log_warn "Kinesis Data Stream ${KINESIS_STREAM_NAME} already exists."
    STREAM_ARN=$(aws kinesis describe-stream --stream-name "${KINESIS_STREAM_NAME}" --region "${REGION}" --query 'StreamDescription.StreamARN' --output text)
    log_info "Stream ARN: ${STREAM_ARN}"
else
    aws kinesis create-stream \
        --stream-name "${KINESIS_STREAM_NAME}" \
        --shard-count "${KINESIS_SHARD_COUNT}" \
        --region "${REGION}"
    
    log_info "Waiting for Kinesis Data Stream to become active..."
    aws kinesis wait stream-exists \
        --stream-name "${KINESIS_STREAM_NAME}" \
        --region "${REGION}"
    
    STREAM_ARN=$(aws kinesis describe-stream --stream-name "${KINESIS_STREAM_NAME}" --region "${REGION}" --query 'StreamDescription.StreamARN' --output text)
    log_info "Kinesis Data Stream created successfully."
    log_info "Stream ARN: ${STREAM_ARN}"
fi

# Create CloudWatch Log Group
log_info "Creating CloudWatch Log Group: ${LOG_GROUP_NAME}..."
if aws logs describe-log-groups --log-group-name-prefix "${LOG_GROUP_NAME}" --region "${REGION}" | grep -q "${LOG_GROUP_NAME}"; then
    log_warn "Log group ${LOG_GROUP_NAME} already exists."
else
    aws logs create-log-group \
        --log-group-name "${LOG_GROUP_NAME}" \
        --region "${REGION}"
    
    # Set retention policy (7 days)
    aws logs put-retention-policy \
        --log-group-name "${LOG_GROUP_NAME}" \
        --retention-in-days 7 \
        --region "${REGION}"
    
    log_info "CloudWatch Log Group created successfully."
fi

# Create CloudWatch Log Stream
log_info "Creating CloudWatch Log Stream: ${LOG_STREAM_NAME}..."
aws logs create-log-stream \
    --log-group-name "${LOG_GROUP_NAME}" \
    --log-stream-name "${LOG_STREAM_NAME}" \
    --region "${REGION}" 2>/dev/null || log_warn "Log stream may already exist."

# Create IAM trust policy document
log_info "Creating IAM trust policy document..."
cat > /tmp/trust-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "kinesisanalytics.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

# Create IAM role
log_info "Creating IAM role: ${IAM_ROLE_NAME}..."
if aws iam get-role --role-name "${IAM_ROLE_NAME}" &> /dev/null; then
    log_warn "IAM role ${IAM_ROLE_NAME} already exists."
    ROLE_ARN=$(aws iam get-role --role-name "${IAM_ROLE_NAME}" --query 'Role.Arn' --output text)
else
    ROLE_ARN=$(aws iam create-role \
        --role-name "${IAM_ROLE_NAME}" \
        --assume-role-policy-document file:///tmp/trust-policy.json \
        --description "Service role for Managed Service for Apache Flink - ${APP_NAME}" \
        --query 'Role.Arn' \
        --output text)
    log_info "IAM role created successfully: ${ROLE_ARN}"
fi

# Create IAM policy document for Flink application
log_info "Creating IAM policy document for Flink application..."
cat > /tmp/flink-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ReadApplicationJAR",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion"
      ],
      "Resource": [
        "arn:aws:s3:::${STREAMING_APP_BUCKET}/*"
      ]
    },
    {
      "Sid": "ListApplicationBucket",
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::${STREAMING_APP_BUCKET}"
      ]
    },
    {
      "Sid": "ReadFromVisitsInputBucket",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::${VISITS_INPUT_BUCKET}",
        "arn:aws:s3:::${VISITS_INPUT_BUCKET}/*"
      ]
    },
    {
      "Sid": "WriteToVisitsOutputBucket",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::${VISITS_OUTPUT_BUCKET}",
        "arn:aws:s3:::${VISITS_OUTPUT_BUCKET}/*"
      ]
    },
    {
      "Sid": "WriteToClaimsOutputBucket",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::${CLAIMS_OUTPUT_BUCKET}",
        "arn:aws:s3:::${CLAIMS_OUTPUT_BUCKET}/*"
      ]
    },
    {
      "Sid": "WriteToLeaveRequestsOutputBucket",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::${LEAVEREQUESTS_OUTPUT_BUCKET}",
        "arn:aws:s3:::${LEAVEREQUESTS_OUTPUT_BUCKET}/*"
      ]
    },
    {
      "Sid": "WriteToDefaultOutputBucket",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::${DEFAULT_OUTPUT_BUCKET}",
        "arn:aws:s3:::${DEFAULT_OUTPUT_BUCKET}/*"
      ]
    },
    {
      "Sid": "KinesisStreamAccess",
      "Effect": "Allow",
      "Action": [
        "kinesis:DescribeStream",
        "kinesis:GetShardIterator",
        "kinesis:GetRecords",
        "kinesis:ListShards",
        "kinesis:DescribeStreamSummary",
        "kinesis:RegisterStreamConsumer"
      ],
      "Resource": "arn:aws:kinesis:${REGION}:${ACCOUNT_ID}:stream/${KINESIS_STREAM_NAME}"
    },
    {
      "Sid": "CloudWatchLogsAccess",
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents",
        "logs:DescribeLogGroups",
        "logs:DescribeLogStreams"
      ],
      "Resource": [
        "arn:aws:logs:${REGION}:${ACCOUNT_ID}:log-group:${LOG_GROUP_NAME}",
        "arn:aws:logs:${REGION}:${ACCOUNT_ID}:log-group:${LOG_GROUP_NAME}:*"
      ]
    },
    {
      "Sid": "CloudWatchMetricsAccess",
      "Effect": "Allow",
      "Action": [
        "cloudwatch:PutMetricData"
      ],
      "Resource": "*"
    },
    {
      "Sid": "VPCAccess",
      "Effect": "Allow",
      "Action": [
        "ec2:DescribeVpcs",
        "ec2:DescribeSubnets",
        "ec2:DescribeSecurityGroups",
        "ec2:DescribeDhcpOptions",
        "ec2:CreateNetworkInterface",
        "ec2:CreateNetworkInterfacePermission",
        "ec2:DescribeNetworkInterfaces",
        "ec2:DeleteNetworkInterface"
      ],
      "Resource": "*"
    }
  ]
}
EOF

# Create or update IAM policy
log_info "Creating IAM policy: ${IAM_POLICY_NAME}..."
POLICY_ARN="arn:aws:iam::${ACCOUNT_ID}:policy/${IAM_POLICY_NAME}"

if aws iam get-policy --policy-arn "${POLICY_ARN}" &> /dev/null; then
    log_warn "IAM policy ${IAM_POLICY_NAME} already exists. Creating a new version..."
    
    # Delete old versions if there are too many
    VERSIONS=$(aws iam list-policy-versions --policy-arn "${POLICY_ARN}" --query 'Versions[?IsDefaultVersion==`false`].VersionId' --output text)
    for VERSION in $VERSIONS; do
        aws iam delete-policy-version --policy-arn "${POLICY_ARN}" --version-id "${VERSION}" || true
    done
    
    # Create new version
    aws iam create-policy-version \
        --policy-arn "${POLICY_ARN}" \
        --policy-document file:///tmp/flink-policy.json \
        --set-as-default
else
    POLICY_ARN=$(aws iam create-policy \
        --policy-name "${IAM_POLICY_NAME}" \
        --policy-document file:///tmp/flink-policy.json \
        --description "Policy for Managed Service for Apache Flink - ${APP_NAME}" \
        --query 'Policy.Arn' \
        --output text)
    log_info "IAM policy created successfully: ${POLICY_ARN}"
fi

# Attach policy to role
log_info "Attaching policy to role..."
aws iam attach-role-policy \
    --role-name "${IAM_ROLE_NAME}" \
    --policy-arn "${POLICY_ARN}"

log_info "Policy attached successfully."

# Create IAM policy for user sunny0524 to upload to S3 input bucket
log_info "Creating IAM policy for S3 upload (user sunny0524)..."
cat > /tmp/s3-upload-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "UploadToVisitsInputBucket",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:PutObjectAcl",
        "s3:GetObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::${VISITS_INPUT_BUCKET}",
        "arn:aws:s3:::${VISITS_INPUT_BUCKET}/*"
      ]
    }
  ]
}
EOF

USER_POLICY_ARN="arn:aws:iam::${ACCOUNT_ID}:policy/${USER_POLICY_NAME}"

if aws iam get-policy --policy-arn "${USER_POLICY_ARN}" &> /dev/null; then
    log_warn "User policy ${USER_POLICY_NAME} already exists. Creating a new version..."
    
    # Delete old versions if there are too many
    VERSIONS=$(aws iam list-policy-versions --policy-arn "${USER_POLICY_ARN}" --query 'Versions[?IsDefaultVersion==`false`].VersionId' --output text)
    for VERSION in $VERSIONS; do
        aws iam delete-policy-version --policy-arn "${USER_POLICY_ARN}" --version-id "${VERSION}" || true
    done
    
    # Create new version
    aws iam create-policy-version \
        --policy-arn "${USER_POLICY_ARN}" \
        --policy-document file:///tmp/s3-upload-policy.json \
        --set-as-default
else
    USER_POLICY_ARN=$(aws iam create-policy \
        --policy-name "${USER_POLICY_NAME}" \
        --policy-document file:///tmp/s3-upload-policy.json \
        --description "Policy for user sunny0524 to upload files to S3 input bucket ${VISITS_INPUT_BUCKET}" \
        --query 'Policy.Arn' \
        --output text)
    log_info "User policy created successfully: ${USER_POLICY_ARN}"
fi

# Attach policy to user sunny0524
log_info "Attaching policy to user sunny0524..."
if aws iam get-user --user-name sunny0524 &> /dev/null; then
    aws iam attach-user-policy \
        --user-name sunny0524 \
        --policy-arn "${USER_POLICY_ARN}" 2>&1 || log_warn "Policy may already be attached to user"
    log_info "Policy attached to user sunny0524 successfully."
else
    log_warn "User sunny0524 not found. You may need to attach the policy manually."
    log_warn "Policy ARN: ${USER_POLICY_ARN}"
fi

# Clean up temporary files
rm -f /tmp/trust-policy.json /tmp/flink-policy.json /tmp/s3-upload-policy.json

# Output summary
log_info "=============================================="
log_info "Infrastructure Creation Complete!"
log_info "=============================================="
echo ""
log_info "Resources Created:"
echo "  - S3 Bucket (Application JAR): ${STREAMING_APP_BUCKET}"
echo "  - Kinesis Data Stream: ${KINESIS_STREAM_NAME}"
echo "  - S3 Bucket (Visits Input): ${VISITS_INPUT_BUCKET}"
echo "  - S3 Bucket (Visits Output): ${VISITS_OUTPUT_BUCKET}"
echo "  - S3 Bucket (Claims Output): ${CLAIMS_OUTPUT_BUCKET}"
echo "  - S3 Bucket (Leave Requests Output): ${LEAVEREQUESTS_OUTPUT_BUCKET}"
echo "  - S3 Bucket (Default Output): ${DEFAULT_OUTPUT_BUCKET}"
echo "  - Visits Input Key: ${VISITS_INPUT_KEY}"
echo "  - IAM Role (Flink): ${IAM_ROLE_NAME}"
echo "  - IAM Role ARN: ${ROLE_ARN}"
echo "  - IAM Policy (Flink): ${IAM_POLICY_NAME}"
echo "  - IAM Policy ARN: ${POLICY_ARN}"
echo "  - IAM Policy (S3 Upload): ${USER_POLICY_NAME}"
echo "  - IAM Policy ARN (S3 Upload): ${USER_POLICY_ARN}"
echo "  - CloudWatch Log Group: ${LOG_GROUP_NAME}"
echo "  - CloudWatch Log Stream: ${LOG_STREAM_NAME}"
echo ""
log_info "Save these values for use in cicd.sh and test.sh:"
echo "export FLINK_ROLE_ARN=\"${ROLE_ARN}\""
echo "export STREAMING_APP_BUCKET=\"${STREAMING_APP_BUCKET}\""
echo "export KINESIS_STREAM_NAME=\"${KINESIS_STREAM_NAME}\""
echo "export VISITS_INPUT_BUCKET=\"${VISITS_INPUT_BUCKET}\""
echo "export VISITS_OUTPUT_BUCKET=\"${VISITS_OUTPUT_BUCKET}\""
echo "export CLAIMS_OUTPUT_BUCKET=\"${CLAIMS_OUTPUT_BUCKET}\""
echo "export LEAVEREQUESTS_OUTPUT_BUCKET=\"${LEAVEREQUESTS_OUTPUT_BUCKET}\""
echo "export DEFAULT_OUTPUT_BUCKET=\"${DEFAULT_OUTPUT_BUCKET}\""
echo "export VISITS_INPUT_KEY=\"${VISITS_INPUT_KEY}\""
echo "export LOG_GROUP=\"${LOG_GROUP_NAME}\""
echo "export LOG_STREAM=\"${LOG_STREAM_NAME}\""
echo "export BUCKET_SUFFIX=\"${BUCKET_SUFFIX}\""
echo ""

# Save configuration to file

cat > /tmp/flink-config.env <<EOF
export FLINK_ROLE_ARN="${ROLE_ARN}"
export STREAMING_APP_BUCKET="${STREAMING_APP_BUCKET}"
export KINESIS_STREAM_NAME="${KINESIS_STREAM_NAME}"
export VISITS_INPUT_BUCKET="${VISITS_INPUT_BUCKET}"
export VISITS_OUTPUT_BUCKET="${VISITS_OUTPUT_BUCKET}"
export CLAIMS_OUTPUT_BUCKET="${CLAIMS_OUTPUT_BUCKET}"
export LEAVEREQUESTS_OUTPUT_BUCKET="${LEAVEREQUESTS_OUTPUT_BUCKET}"
export DEFAULT_OUTPUT_BUCKET="${DEFAULT_OUTPUT_BUCKET}"
export VISITS_INPUT_KEY="${VISITS_INPUT_KEY}"
export LOG_GROUP="${LOG_GROUP_NAME}"
export LOG_STREAM="${LOG_STREAM_NAME}"
export APP_NAME="${APP_NAME}"
export REGION="${REGION}"
export BUCKET_SUFFIX="${BUCKET_SUFFIX}"
EOF

log_info "Configuration saved to /tmp/flink-config.env"
log_info "Source this file before running cicd.sh: source /tmp/flink-config.env"
