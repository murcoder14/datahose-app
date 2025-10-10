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
STREAMING_APP_BUCKET="${STREAMING_APP_BUCKET:-tm-streaming-app-bucket-20251010}"
DATA_BUCKET="${DATA_BUCKET:-tm-data-bucket-20251010}"
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

log_info "=============================================="
log_info "CI/CD Pipeline for ${APP_NAME}"
log_info "=============================================="
echo ""
log_info "Configuration:"
echo "  - Application Name: ${APP_NAME}"
echo "  - Application Bucket: ${STREAMING_APP_BUCKET}"
echo "  - Data Bucket: ${DATA_BUCKET}"
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
    sdk use java 11.0.28-amzn
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
    log_info "Application ${APP_NAME} already exists. Checking status..."
    
    APP_STATUS=$(aws kinesisanalyticsv2 describe-application \
        --application-name "${APP_NAME}" \
        --region "${REGION}" \
        --query 'ApplicationDetail.ApplicationStatus' \
        --output text)
    
    APP_VERSION=$(aws kinesisanalyticsv2 describe-application \
        --application-name "${APP_NAME}" \
        --region "${REGION}" \
        --query 'ApplicationDetail.ApplicationVersionId' \
        --output text)
    
    log_info "Current status: ${APP_STATUS}, Version: ${APP_VERSION}"
    
    # Stop the application if it's running
    if [ "${APP_STATUS}" == "RUNNING" ]; then
        log_info "Stopping application before update..."
        aws kinesisanalyticsv2 stop-application \
            --application-name "${APP_NAME}" \
            --region "${REGION}" \
            --force
        
        # Wait for application to stop
        log_info "Waiting for application to stop..."
        for i in {1..60}; do
            APP_STATUS=$(aws kinesisanalyticsv2 describe-application \
                --application-name "${APP_NAME}" \
                --region "${REGION}" \
                --query 'ApplicationDetail.ApplicationStatus' \
                --output text)
            
            if [ "${APP_STATUS}" == "READY" ]; then
                log_info "Application stopped successfully."
                break
            fi
            
            if [ $i -eq 60 ]; then
                log_error "Timeout waiting for application to stop."
                exit 1
            fi
            
            log_info "Current status: ${APP_STATUS}. Waiting... (${i}/60)"
            sleep 10
        done
        
        # Get updated version after stopping
        APP_VERSION=$(aws kinesisanalyticsv2 describe-application \
            --application-name "${APP_NAME}" \
            --region "${REGION}" \
            --query 'ApplicationDetail.ApplicationVersionId' \
            --output text)
    fi
    
    # Update application
    log_info "Updating application configuration..."
    aws kinesisanalyticsv2 update-application \
        --application-name "${APP_NAME}" \
        --region "${REGION}" \
        --current-application-version-id ${APP_VERSION} \
        --application-configuration-update "{
            \"ApplicationCodeConfigurationUpdate\": {
                \"CodeContentTypeUpdate\": \"ZIPFILE\",
                \"CodeContentUpdate\": {
                    \"S3ContentLocationUpdate\": {
                        \"BucketARNUpdate\": \"arn:aws:s3:::${STREAMING_APP_BUCKET}\",
                        \"FileKeyUpdate\": \"${S3_JAR_KEY}\",
                        \"ObjectVersionUpdate\": \"${S3_OBJECT_VERSION}\"
                    }
                }
            }
        }"
    
    log_info "Application updated to READY state."
    
else
    log_info "Creating new Flink application..."
    
    # Create the application
    aws kinesisanalyticsv2 create-application \
        --application-name "${APP_NAME}" \
        --region "${REGION}" \
        --runtime-environment "${RUNTIME_ENVIRONMENT}" \
        --service-execution-role "${FLINK_ROLE_ARN}" \
        --application-configuration "{
            \"ApplicationCodeConfiguration\": {
                \"CodeContent\": {
                    \"S3ContentLocation\": {
                        \"BucketARN\": \"arn:aws:s3:::${STREAMING_APP_BUCKET}\",
                        \"FileKey\": \"${S3_JAR_KEY}\",
                        \"ObjectVersion\": \"${S3_OBJECT_VERSION}\"
                    }
                },
                \"CodeContentType\": \"ZIPFILE\"
            },
            \"FlinkApplicationConfiguration\": {
                \"CheckpointConfiguration\": {
                    \"ConfigurationType\": \"DEFAULT\"
                },
                \"MonitoringConfiguration\": {
                    \"ConfigurationType\": \"CUSTOM\",
                    \"MetricsLevel\": \"APPLICATION\",
                    \"LogLevel\": \"INFO\"
                },
                \"ParallelismConfiguration\": {
                    \"ConfigurationType\": \"CUSTOM\",
                    \"Parallelism\": 1,
                    \"ParallelismPerKPU\": 1,
                    \"AutoScalingEnabled\": false
                }
            },
            \"EnvironmentProperties\": {
                \"PropertyGroups\": []
            }
        }"
    
    log_info "Application created successfully in READY state."
    
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
        if [ -n "${LOG_GROUP}" ] && [ -n "${LOG_STREAM}" ]; then
            log_info "Fetching recent logs from ${LOG_GROUP}..."
            aws logs tail "${LOG_GROUP}" --follow --since 5m --region "${REGION}" 2>/dev/null || log_warn "Could not fetch logs."
        fi
        exit 1
    fi
    
    if [ $i -eq 60 ]; then
        log_error "Timeout waiting for application to start."
        log_error "Current status: ${APP_STATUS}"
        log_error "Checking logs for errors..."
        
        # Try to get CloudWatch logs
        if [ -n "${LOG_GROUP}" ] && [ -n "${LOG_STREAM}" ]; then
            log_info "Fetching recent logs from ${LOG_GROUP}..."
            aws logs tail "${LOG_GROUP}" --follow --since 5m --region "${REGION}" 2>/dev/null || log_warn "Could not fetch logs."
        fi
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
echo "  - Data Output: s3://${DATA_BUCKET}/datafall/"
if [ -n "${LOG_GROUP}" ]; then
    echo "  - CloudWatch Logs: ${LOG_GROUP}"
fi
echo ""
log_info "Monitoring Commands:"
echo "  - View application status:"
echo "    aws kinesisanalyticsv2 describe-application --application-name ${APP_NAME} --region ${REGION}"
echo ""
echo "  - View CloudWatch logs:"
if [ -n "${LOG_GROUP}" ]; then
    echo "    aws logs tail ${LOG_GROUP} --follow --region ${REGION}"
fi
echo ""
echo "  - List output files in S3:"
echo "    aws s3 ls s3://${DATA_BUCKET}/datafall/ --recursive --region ${REGION}"
echo ""
echo "  - Download output files:"
echo "    aws s3 sync s3://${DATA_BUCKET}/datafall/ ./output/ --region ${REGION}"
echo ""
log_info "To stop the application:"
echo "  aws kinesisanalyticsv2 stop-application --application-name ${APP_NAME} --region ${REGION}"
echo ""

# Tail logs for a few seconds to show initial output
if [ -n "${LOG_GROUP}" ]; then
    log_info "Showing initial application logs (10 seconds)..."
    echo ""
    timeout 10 aws logs tail "${LOG_GROUP}" --follow --region "${REGION}" 2>/dev/null || true
    echo ""
fi

log_info "Deployment completed successfully!"
log_info "The application is now processing data and writing to s3://${DATA_BUCKET}/datafall/"
