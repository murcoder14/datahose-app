#!/bin/bash

# CI/CD Script for Flink Streaming Application
# This script builds the application, uploads to S3, creates/updates the Flink application,
# and starts it in streaming mode

set -e  # Exit on error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
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

log_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

# Load configuration from environment or file
if [ -f "/tmp/flink-config.env" ]; then
    log_info "Loading configuration from /tmp/flink-config.env..."
    source /tmp/flink-config.env
fi

# Configuration
APP_NAME="${APP_NAME:-datahose-app}"
STREAMING_APP_BUCKET="${STREAMING_APP_BUCKET}"
VISITS_INPUT_BUCKET="${VISITS_INPUT_BUCKET}"
VISITS_OUTPUT_BUCKET="${VISITS_OUTPUT_BUCKET}"
CLAIMS_OUTPUT_BUCKET="${CLAIMS_OUTPUT_BUCKET}"
LEAVEREQUESTS_OUTPUT_BUCKET="${LEAVEREQUESTS_OUTPUT_BUCKET}"
DEFAULT_OUTPUT_BUCKET="${DEFAULT_OUTPUT_BUCKET}"
VISITS_INPUT_KEY="${VISITS_INPUT_KEY:-filefall/gymvisits.csv}"
LOG_GROUP="${LOG_GROUP:-/aws/kinesis-analytics/${APP_NAME}}"
KINESIS_STREAM_NAME="${KINESIS_STREAM_NAME}"

# Get region from AWS CLI default profile configuration
REGION=$(aws configure get region 2>/dev/null)
if [ -z "$REGION" ]; then
    echo -e "${RED}[ERROR]${NC} No default region configured. Please run: aws configure set region us-east-2"
    exit 1
fi

JAR_FILE="target/${APP_NAME}.jar"
S3_JAR_KEY="${APP_NAME}.jar"
FLINK_VERSION="FLINK-1_20"
RUNTIME_ENVIRONMENT="FLINK-1_20"

# Verify required environment variables
if [ -z "${FLINK_ROLE_ARN}" ]; then
    log_error "FLINK_ROLE_ARN is not set. Please run iac_create.sh first and source the configuration."
    log_error "Or set it manually: export FLINK_ROLE_ARN=<your-role-arn>"
    exit 1
fi

if [ -z "${VISITS_INPUT_BUCKET}" ]; then
    log_error "VISITS_INPUT_BUCKET is not set. Please run iac_create.sh first and source the configuration."
    log_error "Or set it manually: export VISITS_INPUT_BUCKET=<your-input-bucket>"
    exit 1
fi

if [ -z "${VISITS_OUTPUT_BUCKET}" ] || [ -z "${CLAIMS_OUTPUT_BUCKET}" ] || [ -z "${LEAVEREQUESTS_OUTPUT_BUCKET}" ] || [ -z "${DEFAULT_OUTPUT_BUCKET}" ]; then
    log_error "One or more output buckets are not set. Please run iac_create.sh first and source the configuration."
    exit 1
fi

if [ -z "${KINESIS_STREAM_NAME}" ]; then
    log_error "KINESIS_STREAM_NAME is not set. Please set it manually: export KINESIS_STREAM_NAME=<your-stream-name>"
    exit 1
fi

log_info "=============================================="
log_info "CI/CD Pipeline for ${APP_NAME}"
log_info "=============================================="
echo ""
log_info "Configuration:"
echo "  - Application Name: ${APP_NAME}"
echo "  - Application Bucket: ${STREAMING_APP_BUCKET}"
echo "  - Visits Input Bucket: ${VISITS_INPUT_BUCKET}"
echo "  - Visits Input Key: ${VISITS_INPUT_KEY}"
echo "  - Visits Output Bucket: ${VISITS_OUTPUT_BUCKET}"
echo "  - Claims Output Bucket: ${CLAIMS_OUTPUT_BUCKET}"
echo "  - Leave Requests Output Bucket: ${LEAVEREQUESTS_OUTPUT_BUCKET}"
echo "  - Default Output Bucket: ${DEFAULT_OUTPUT_BUCKET}"
echo "  - Kinesis Stream: ${KINESIS_STREAM_NAME}"
echo "  - CloudWatch Logs: ${LOG_GROUP}"
echo "  - Region: ${REGION}"
echo "  - IAM Role ARN: ${FLINK_ROLE_ARN}"
echo "  - Flink Version: ${FLINK_VERSION}"
echo ""

# Check if AWS CLI is installed
if ! command -v aws &> /dev/null; then
    log_error "AWS CLI is not installed. Please install it first."
    exit 1
fi

# Check if Maven is installed
if ! command -v mvn &> /dev/null; then
    log_error "Maven is not installed. Please install it first."
    exit 1
fi

# Initialize SDKMAN and set Java 11
log_info "Initializing Java 11 using SDKMAN..."
if [ -s "$HOME/.sdkman/bin/sdkman-init.sh" ]; then
    source "$HOME/.sdkman/bin/sdkman-init.sh"
    sdk use java 11.0.29-amzn
    log_info "Java version set to: $(java -version 2>&1 | head -n 1)"
else
    log_warn "SDKMAN not found. Attempting to use system Java..."
    if ! java -version 2>&1 | grep -q "version \"11\."; then
        log_error "Java 11 is required but not found. Please install Java 11 or SDKMAN."
        exit 1
    fi
fi

# Verify AWS credentials
log_info "Verifying AWS credentials..."
if ! aws sts get-caller-identity &> /dev/null; then
    log_error "AWS credentials are not configured properly."
    exit 1
fi

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
log_info "AWS Account ID: ${ACCOUNT_ID}"

# Verify CloudWatch Log Group exists (created by iac_create.sh)
log_info "Verifying CloudWatch Log Group exists..."
if ! aws logs describe-log-groups --log-group-name-prefix "${LOG_GROUP}" --region "${REGION}" 2>/dev/null | grep -q "${LOG_GROUP}"; then
    log_error "CloudWatch Log Group ${LOG_GROUP} does not exist!"
    log_error "Please run iac_create.sh first to create the infrastructure."
    exit 1
fi
log_info "CloudWatch Log Group verified: ${LOG_GROUP}"

# Step 1: Build the application using Maven
log_step "Step 1: Building Java application with Maven..."
log_info "Cleaning and building the project..."

mvn clean package -DskipTests

if [ ! -f "${JAR_FILE}" ]; then
    log_error "Build failed! JAR file not found: ${JAR_FILE}"
    exit 1
fi

JAR_SIZE=$(du -h "${JAR_FILE}" | cut -f1)
log_info "Build successful! JAR file: ${JAR_FILE} (${JAR_SIZE})"

# Verify JAR size is under 512MB
JAR_SIZE_BYTES=$(stat -c%s "${JAR_FILE}")
MAX_SIZE_BYTES=$((512 * 1024 * 1024))

if [ ${JAR_SIZE_BYTES} -gt ${MAX_SIZE_BYTES} ]; then
    log_error "JAR file size (${JAR_SIZE}) exceeds the 512MB limit for Managed Service for Apache Flink!"
    exit 1
fi

# Step 2: Upload JAR to S3
log_step "Step 2: Uploading JAR to S3 bucket: ${STREAMING_APP_BUCKET}..."

aws s3 cp "${JAR_FILE}" "s3://${STREAMING_APP_BUCKET}/${S3_JAR_KEY}" --region "${REGION}"

# Get the S3 object version
S3_OBJECT_VERSION=$(aws s3api head-object \
    --bucket "${STREAMING_APP_BUCKET}" \
    --key "${S3_JAR_KEY}" \
    --region "${REGION}" \
    --query 'VersionId' \
    --output text)

log_info "JAR uploaded successfully. Object version: ${S3_OBJECT_VERSION}"

# Step 3: Create or update Flink application
log_step "Step 3: Creating/Updating Flink application..."

# Check if application exists
if aws kinesisanalyticsv2 describe-application --application-name "${APP_NAME}" --region "${REGION}" &> /dev/null; then
    log_info "Application ${APP_NAME} already exists. Updating..."
    
    # Check current application status
    CURRENT_STATUS=$(aws kinesisanalyticsv2 describe-application \
        --application-name "${APP_NAME}" \
        --region "${REGION}" \
        --query 'ApplicationDetail.ApplicationStatus' \
        --output text)
    
    log_info "Current application status: ${CURRENT_STATUS}"
    
    # Stop the application if it's running
    if [ "${CURRENT_STATUS}" == "RUNNING" ] || [ "${CURRENT_STATUS}" == "STARTING" ]; then
        log_info "Stopping application before update..."
        aws kinesisanalyticsv2 stop-application \
            --application-name "${APP_NAME}" \
            --region "${REGION}" \
            --force 2>&1 || log_warn "Stop command issued (may already be stopping)"
        
        # Wait for application to stop
        log_info "Waiting for application to stop..."
        for i in {1..30}; do
            STOP_STATUS=$(aws kinesisanalyticsv2 describe-application \
                --application-name "${APP_NAME}" \
                --region "${REGION}" \
                --query 'ApplicationDetail.ApplicationStatus' \
                --output text)
            
            if [ "${STOP_STATUS}" == "READY" ]; then
                log_info "Application stopped successfully."
                break
            fi
            
            if [ $i -eq 30 ]; then
                log_error "Timeout waiting for application to stop. Current status: ${STOP_STATUS}"
                exit 1
            fi
            
            log_info "Current status: ${STOP_STATUS}. Waiting... (${i}/30)"
            sleep 10
        done
    fi
    
    # Get current application version
    APP_VERSION=$(aws kinesisanalyticsv2 describe-application \
        --application-name "${APP_NAME}" \
        --region "${REGION}" \
        --query 'ApplicationDetail.ApplicationVersionId' \
        --output text)
    
    log_info "Current application version: ${APP_VERSION}"
    
    # Update the application
    aws kinesisanalyticsv2 update-application \
        --application-name "${APP_NAME}" \
        --region "${REGION}" \
        --current-application-version-id ${APP_VERSION} \
        --application-configuration-update "{\
            \"ApplicationCodeConfigurationUpdate\": {\
                \"CodeContentTypeUpdate\": \"ZIPFILE\",\
                \"CodeContentUpdate\": {\
                    \"S3ContentLocationUpdate\": {\
                        \"BucketARNUpdate\": \"arn:aws:s3:::${STREAMING_APP_BUCKET}\",\
                        \"FileKeyUpdate\": \"${S3_JAR_KEY}\",\
                        \"ObjectVersionUpdate\": \"${S3_OBJECT_VERSION}\"\
                    }\
                }\
            },\
            \"FlinkApplicationConfigurationUpdate\": {\
                \"MonitoringConfigurationUpdate\": {\
                    \"ConfigurationTypeUpdate\": \"CUSTOM\",\
                    \"LogLevelUpdate\": \"INFO\",\
                    \"MetricsLevelUpdate\": \"APPLICATION\"\
                }\
            },\
            \"EnvironmentPropertyUpdates\": {\
                \"PropertyGroups\": [\
                    {\
                        \"PropertyGroupId\": \"KinesisSource\",\
                        \"PropertyMap\": {\
                            \"aws.region\": \"${REGION}\",\
                            \"stream.arn\": \"arn:aws:kinesis:${REGION}:${ACCOUNT_ID}:stream/${KINESIS_STREAM_NAME}\"\
                        }\
                    },\
                    {\
                        \"PropertyGroupId\": \"s3source\",\
                        \"PropertyMap\": {\
                            \"visits-input-bucket\": \"${VISITS_INPUT_BUCKET}\",\
                            \"visits-input-key\": \"${VISITS_INPUT_KEY}\"\
                        }\
                    },\
                    {\
                        \"PropertyGroupId\": \"s3sink\",\
                        \"PropertyMap\": {\
                            \"visits-output-bucket\": \"${VISITS_OUTPUT_BUCKET}\",\
                            \"claims-output-bucket\": \"${CLAIMS_OUTPUT_BUCKET}\",\
                            \"leaverequests-output-bucket\": \"${LEAVEREQUESTS_OUTPUT_BUCKET}\",\
                            \"default-output-bucket\": \"${DEFAULT_OUTPUT_BUCKET}\"\
                        }\
                    }\
                ]\
            }\
        }"
    
    log_info "Application updated successfully."
    
else
    log_info "Creating new Flink application..."
    
    # Create the application without CloudWatch logging options (will be added separately)
    aws kinesisanalyticsv2 create-application \
        --application-name "${APP_NAME}" \
        --region "${REGION}" \
        --runtime-environment "${RUNTIME_ENVIRONMENT}" \
        --service-execution-role "${FLINK_ROLE_ARN}" \
        --application-configuration "{\
            \"ApplicationCodeConfiguration\": {\
                \"CodeContent\": {\
                    \"S3ContentLocation\": {\
                        \"BucketARN\": \"arn:aws:s3:::${STREAMING_APP_BUCKET}\",\
                        \"FileKey\": \"${S3_JAR_KEY}\",\
                        \"ObjectVersion\": \"${S3_OBJECT_VERSION}\"\
                    }\
                },\
                \"CodeContentType\": \"ZIPFILE\"\
            },\
            \"FlinkApplicationConfiguration\": {\
                \"CheckpointConfiguration\": {\
                    \"ConfigurationType\": \"DEFAULT\"\
                },\
                \"MonitoringConfiguration\": {\
                    \"ConfigurationType\": \"CUSTOM\",\
                    \"MetricsLevel\": \"APPLICATION\",\
                    \"LogLevel\": \"INFO\"\
                },\
                \"ParallelismConfiguration\": {\
                    \"ConfigurationType\": \"CUSTOM\",\
                    \"Parallelism\": 1,\
                    \"ParallelismPerKPU\": 1,\
                    \"AutoScalingEnabled\": false\
                }\
            },\
            \"EnvironmentProperties\": {\
                \"PropertyGroups\": [\
                    {\
                        \"PropertyGroupId\": \"KinesisSource\",\
                        \"PropertyMap\": {\
                            \"aws.region\": \"${REGION}\",\
                            \"stream.arn\": \"arn:aws:kinesis:${REGION}:${ACCOUNT_ID}:stream/${KINESIS_STREAM_NAME}\"\
                        }\
                    },\
                    {\
                        \"PropertyGroupId\": \"s3source\",\
                        \"PropertyMap\": {\
                            \"visits-input-bucket\": \"${VISITS_INPUT_BUCKET}\",\
                            \"visits-input-key\": \"${VISITS_INPUT_KEY}\"\
                        }\
                    },\
                    {\
                        \"PropertyGroupId\": \"s3sink\",\
                        \"PropertyMap\": {\
                            \"visits-output-bucket\": \"${VISITS_OUTPUT_BUCKET}\",\
                            \"claims-output-bucket\": \"${CLAIMS_OUTPUT_BUCKET}\",\
                            \"leaverequests-output-bucket\": \"${LEAVEREQUESTS_OUTPUT_BUCKET}\",\
                            \"default-output-bucket\": \"${DEFAULT_OUTPUT_BUCKET}\"\
                        }\
                    }\
                ]\
            }\
        }"
    
    log_info "Application created successfully."
    
    # Wait for application to be ready
    log_info "Waiting for application to be fully created..."
    for i in {1..30}; do
        APP_STATUS=$(aws kinesisanalyticsv2 describe-application \
            --application-name "${APP_NAME}" \
            --region "${REGION}" \
            --query 'ApplicationDetail.ApplicationStatus' \
            --output text 2>/dev/null || echo "CREATING")
        
        if [ "${APP_STATUS}" == "READY" ]; then
            log_info "Application is ready."
            break
        fi
        
        if [ $i -eq 30 ]; then
            log_error "Timeout waiting for application to be ready."
            exit 1
        fi
        
        log_info "Current status: ${APP_STATUS}. Waiting... (${i}/30)"
        sleep 10
    done
    
    # Add CloudWatch logging after application is created
    log_info "Adding CloudWatch logging configuration..."
    
    APP_VERSION=$(aws kinesisanalyticsv2 describe-application \
        --application-name "${APP_NAME}" \
        --region "${REGION}" \
        --query 'ApplicationDetail.ApplicationVersionId' \
        --output text)
    
    aws kinesisanalyticsv2 add-application-cloud-watch-logging-option \
        --application-name "${APP_NAME}" \
        --region "${REGION}" \
        --current-application-version-id ${APP_VERSION} \
        --cloud-watch-logging-option "{\"LogStreamARN\":\"arn:aws:logs:${REGION}:${ACCOUNT_ID}:log-group:${LOG_GROUP}:log-stream:flink-application\"}"
    
    log_info "CloudWatch logging configured successfully."
fi

# Step 4: Start the application in streaming mode
log_step "Step 4: Starting Flink application in STREAMING mode..."

# Get current application version
APP_VERSION=$(aws kinesisanalyticsv2 describe-application \
    --application-name "${APP_NAME}" \
    --region "${REGION}" \
    --query 'ApplicationDetail.ApplicationVersionId' \
    --output text)

log_info "Starting application with version ${APP_VERSION}..."

aws kinesisanalyticsv2 start-application \
    --application-name "${APP_NAME}" \
    --region "${REGION}" \
    --run-configuration "{
        \"FlinkRunConfiguration\": {
            \"AllowNonRestoredState\": true
        }
    }"

log_info "Start command issued. Waiting for application to reach RUNNING state..."

# Wait for application to be running
for i in {1..60}; do
    APP_STATUS=$(aws kinesisanalyticsv2 describe-application \
        --application-name "${APP_NAME}" \
        --region "${REGION}" \
        --query 'ApplicationDetail.ApplicationStatus' \
        --output text)
    
    if [ "${APP_STATUS}" == "RUNNING" ]; then
        log_info "Application is now RUNNING!"
        break
    fi
    
    if [ "${APP_STATUS}" == "STOPPING" ] || [ "${APP_STATUS}" == "DELETING" ]; then
        log_error "Application entered unexpected state: ${APP_STATUS}"
        log_error "Checking logs for errors..."
        
        # Try to get CloudWatch logs
        log_info "Fetching recent logs from ${LOG_GROUP}..."
        aws logs tail "${LOG_GROUP}" --follow --since 5m --region "${REGION}" 2>/dev/null || log_warn "Could not fetch logs."
        exit 1
    fi
    
    if [ $i -eq 60 ]; then
        log_error "Timeout waiting for application to start."
        log_error "Current status: ${APP_STATUS}"
        log_error "Checking logs for errors..."
        
        # Try to get CloudWatch logs
        log_info "Fetching recent logs from ${LOG_GROUP}..."
        aws logs tail "${LOG_GROUP}" --follow --since 5m --region "${REGION}" 2>/dev/null || log_warn "Could not fetch logs."
        exit 1
    fi
    
    log_info "Current status: ${APP_STATUS}. Waiting... (${i}/60)"
    sleep 10
done

# Get final application details
log_info "Retrieving application details..."
APP_DETAILS=$(aws kinesisanalyticsv2 describe-application \
    --application-name "${APP_NAME}" \
    --region "${REGION}")

APP_ARN=$(echo "${APP_DETAILS}" | jq -r '.ApplicationDetail.ApplicationARN')
APP_STATUS=$(echo "${APP_DETAILS}" | jq -r '.ApplicationDetail.ApplicationStatus')
APP_VERSION=$(echo "${APP_DETAILS}" | jq -r '.ApplicationDetail.ApplicationVersionId')

# Output summary
log_info "=============================================="
log_info "CI/CD Pipeline Completed Successfully!"
log_info "=============================================="
echo ""
log_info "Application Details:"
echo "  - Name: ${APP_NAME}"
echo "  - ARN: ${APP_ARN}"
echo "  - Status: ${APP_STATUS}"
echo "  - Version: ${APP_VERSION}"
echo "  - Region: ${REGION}"
echo ""
log_info "Application Resources:"
echo "  - Application Code: s3://${STREAMING_APP_BUCKET}/${S3_JAR_KEY}"
echo "  - Kinesis Stream: ${KINESIS_STREAM_NAME}"
echo "  - Visits Input: s3://${VISITS_INPUT_BUCKET}/${VISITS_INPUT_KEY}"
echo "  - Visits Output: s3://${VISITS_OUTPUT_BUCKET}/"
echo "  - Claims Output: s3://${CLAIMS_OUTPUT_BUCKET}/"
echo "  - Leave Requests Output: s3://${LEAVEREQUESTS_OUTPUT_BUCKET}/"
echo "  - Default Output: s3://${DEFAULT_OUTPUT_BUCKET}/"
echo "  - CloudWatch Logs: ${LOG_GROUP}"
echo ""
log_info "Monitoring Commands:"
echo "  - View application status:"
echo "    aws kinesisanalyticsv2 describe-application --application-name ${APP_NAME} --region ${REGION}"
echo ""
echo "  - View CloudWatch logs:"
echo "    aws logs tail ${LOG_GROUP} --follow --region ${REGION}"
echo ""
echo "  - List visits input files:"
echo "    aws s3 ls s3://${VISITS_INPUT_BUCKET}/ --recursive --region ${REGION}"
echo ""
echo "  - List visits output:"
echo "    aws s3 ls s3://${VISITS_OUTPUT_BUCKET}/ --recursive --region ${REGION}"
echo ""
echo "  - List claims output:"
echo "    aws s3 ls s3://${CLAIMS_OUTPUT_BUCKET}/ --recursive --region ${REGION}"
echo ""
echo "  - List leave requests output:"
echo "    aws s3 ls s3://${LEAVEREQUESTS_OUTPUT_BUCKET}/ --recursive --region ${REGION}"
echo ""
echo "  - List default output:"
echo "    aws s3 ls s3://${DEFAULT_OUTPUT_BUCKET}/ --recursive --region ${REGION}"
echo ""
echo "  - Download all output files:"
echo "    aws s3 sync s3://${VISITS_OUTPUT_BUCKET}/ ./output/visits/ --region ${REGION}"
echo "    aws s3 sync s3://${CLAIMS_OUTPUT_BUCKET}/ ./output/claims/ --region ${REGION}"
echo "    aws s3 sync s3://${LEAVEREQUESTS_OUTPUT_BUCKET}/ ./output/leaverequests/ --region ${REGION}"
echo "    aws s3 sync s3://${DEFAULT_OUTPUT_BUCKET}/ ./output/default/ --region ${REGION}"
echo ""
log_info "To stop the application:"
echo "  aws kinesisanalyticsv2 stop-application --application-name ${APP_NAME} --region ${REGION}"
echo ""

# Tail logs for a few seconds to show initial output
log_info "Showing initial application logs (10 seconds)..."
echo ""
timeout 10 aws logs tail "${LOG_GROUP}" --follow --region "${REGION}" 2>/dev/null || true
echo ""

log_info "Deployment completed successfully!"
log_info "The application is now monitoring s3://${INPUT_DATA_BUCKET}/${INPUT_DATA_BUCKET_TABLE_NAME}/ for new files"
log_info "Processed data will be written to s3://${OUTPUT_DATA_BUCKET}/${OUTPUT_DATA_BUCKET_TABLE_NAME}/"
