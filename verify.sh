#!/bin/bash

# Verification Script for Flink Application
# This script checks the status and health of the deployed Flink application

set -e

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[✓]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[!]${NC} $1"; }
log_error() { echo -e "${RED}[✗]${NC} $1"; }
log_section() { echo -e "\n${BLUE}=== $1 ===${NC}"; }

# Configuration
APP_NAME="${APP_NAME:-datahose-app}"
# Auto-detect latest dynamic S3 buckets by prefix and creation date
STREAMING_APP_BUCKET=$(aws s3api list-buckets --query 'Buckets[?starts_with(Name, `tm-streaming-app-bucket-`)] | sort_by(@, &CreationDate)[-1].Name' --output text)
ICEBERG_WAREHOUSE_BUCKET=$(aws s3api list-buckets --query 'Buckets[?starts_with(Name, `tm-iceberg-warehouse-`)] | sort_by(@, &CreationDate)[-1].Name' --output text)
DEFAULT_OUTPUT_BUCKET=$(aws s3api list-buckets --query 'Buckets[?starts_with(Name, `tm-output-`)] | sort_by(@, &CreationDate)[-1].Name' --output text)
GLUE_DATABASE_NAME="tm_data_lake"
# Get region from AWS CLI default profile configuration
REGION=$(aws configure get region 2>/dev/null)
if [ -z "$REGION" ]; then
    echo "ERROR: No default region configured. Please run: aws configure set region us-east-2"
    exit 1
fi
IAM_ROLE_NAME="${APP_NAME}-flink-role"
LOG_GROUP_NAME="/aws/kinesis-analytics/${APP_NAME}"
KINESIS_STREAM_NAME="datahose-app-stream"

echo "╔════════════════════════════════════════════════════════╗"
echo "║     Flink Application Verification Report             ║"
echo "╚════════════════════════════════════════════════════════╝"
echo ""

# Check AWS CLI
if ! command -v aws &> /dev/null; then
    log_error "AWS CLI is not installed"
    exit 1
fi

# Check credentials
log_section "AWS Credentials"
if aws sts get-caller-identity &> /dev/null; then
    ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
    USER_ARN=$(aws sts get-caller-identity --query Arn --output text)
    log_info "Account ID: ${ACCOUNT_ID}"
    log_info "User/Role: ${USER_ARN}"
else
    log_error "AWS credentials not configured"
    exit 1
fi

# Check S3 Buckets
log_section "S3 Buckets"

if [ -n "${STREAMING_APP_BUCKET}" ] && aws s3 ls "s3://${STREAMING_APP_BUCKET}" &> /dev/null; then
    log_info "Application bucket exists: ${STREAMING_APP_BUCKET}"
    JAR_COUNT=$(aws s3 ls "s3://${STREAMING_APP_BUCKET}/" --region "${REGION}" | awk '{print $4}' | grep -c ".jar$" || echo "0")
    log_info "JAR files in bucket: ${JAR_COUNT}"
    if [ "$JAR_COUNT" -gt 0 ]; then
        echo "    Recent JAR(s):"
        aws s3 ls "s3://${STREAMING_APP_BUCKET}/" --region "${REGION}" | grep ".jar$" | tail -3 | sed 's/^/    /'
    fi
else
    log_error "Application bucket not found: ${STREAMING_APP_BUCKET}"
fi

if [ -n "${ICEBERG_WAREHOUSE_BUCKET}" ] && aws s3 ls "s3://${ICEBERG_WAREHOUSE_BUCKET}" &> /dev/null; then
    log_info "Iceberg warehouse exists: ${ICEBERG_WAREHOUSE_BUCKET}"
    
    # Check for claims and leave_requests folders
    if aws s3 ls "s3://${ICEBERG_WAREHOUSE_BUCKET}/claims/" &> /dev/null; then
        CLAIMS_FILES=$(aws s3 ls "s3://${ICEBERG_WAREHOUSE_BUCKET}/claims/" --recursive --region "${REGION}" | wc -l)
        log_info "Claims table files: ${CLAIMS_FILES}"
    fi
    
    if aws s3 ls "s3://${ICEBERG_WAREHOUSE_BUCKET}/leave_requests/" &> /dev/null; then
        LEAVE_FILES=$(aws s3 ls "s3://${ICEBERG_WAREHOUSE_BUCKET}/leave_requests/" --recursive --region "${REGION}" | wc -l)
        log_info "Leave requests table files: ${LEAVE_FILES}"
    fi
else
    log_error "Iceberg warehouse not found: ${ICEBERG_WAREHOUSE_BUCKET}"
fi

if [ -n "${DEFAULT_OUTPUT_BUCKET}" ] && aws s3 ls "s3://${DEFAULT_OUTPUT_BUCKET}" &> /dev/null; then
    log_info "Default output bucket exists: ${DEFAULT_OUTPUT_BUCKET}"
else
    log_warn "Default output bucket not found: ${DEFAULT_OUTPUT_BUCKET}"
fi

# Data Streaming Architecture
log_section "Data Streaming Architecture"
log_info "Architecture: Kinesis → Flink → Iceberg (Data Lake)"
if aws kinesis describe-stream --stream-name "${KINESIS_STREAM_NAME}" --region "${REGION}" &> /dev/null; then
    log_info "Kinesis Stream: ${KINESIS_STREAM_NAME} ✓"
else
    log_warn "Kinesis Stream not found: ${KINESIS_STREAM_NAME}"
fi

# Check Glue Catalog
log_section "Glue Catalog"
if aws glue get-database --name "${GLUE_DATABASE_NAME}" --region "${REGION}" &> /dev/null; then
    log_info "Glue database exists: ${GLUE_DATABASE_NAME}"
    
    # List tables
    TABLES=$(aws glue get-tables --database-name "${GLUE_DATABASE_NAME}" --region "${REGION}" --query 'TableList[*].Name' --output text)
    if [ -n "${TABLES}" ]; then
        log_info "Tables: ${TABLES}"
    else
        log_warn "No tables found in database"
    fi
else
    log_error "Glue database not found: ${GLUE_DATABASE_NAME}"
fi

# Check IAM Role
log_section "IAM Resources"

if aws iam get-role --role-name "${IAM_ROLE_NAME}" &> /dev/null; then
    ROLE_ARN=$(aws iam get-role --role-name "${IAM_ROLE_NAME}" --query 'Role.Arn' --output text)
    log_info "IAM Role exists: ${IAM_ROLE_NAME}"
    log_info "Role ARN: ${ROLE_ARN}"
    
    POLICY_COUNT=$(aws iam list-attached-role-policies --role-name "${IAM_ROLE_NAME}" --query 'AttachedPolicies' --output json | jq '. | length')
    log_info "Attached policies: ${POLICY_COUNT}"
else
    log_error "IAM Role not found: ${IAM_ROLE_NAME}"
fi

# Check CloudWatch Logs
log_section "CloudWatch Logs"

if aws logs describe-log-groups --log-group-name-prefix "${LOG_GROUP_NAME}" --region "${REGION}" | grep -q "${LOG_GROUP_NAME}"; then
    log_info "Log group exists: ${LOG_GROUP_NAME}"
    
    RETENTION=$(aws logs describe-log-groups --log-group-name-prefix "${LOG_GROUP_NAME}" --region "${REGION}" --query 'logGroups[0].retentionInDays' --output text)
    log_info "Retention period: ${RETENTION} days"
    
    STREAM_COUNT=$(aws logs describe-log-streams --log-group-name "${LOG_GROUP_NAME}" --region "${REGION}" --query 'logStreams' --output json | jq '. | length')
    log_info "Log streams: ${STREAM_COUNT}"
else
    log_warn "Log group not found: ${LOG_GROUP_NAME}"
fi

# Check Flink Application
log_section "Flink Application"

if aws kinesisanalyticsv2 describe-application --application-name "${APP_NAME}" --region "${REGION}" &> /dev/null; then
    log_info "Application exists: ${APP_NAME}"
    
    APP_DETAILS=$(aws kinesisanalyticsv2 describe-application --application-name "${APP_NAME}" --region "${REGION}" --query 'ApplicationDetail')
    
    APP_STATUS=$(echo "${APP_DETAILS}" | jq -r '.ApplicationStatus')
    APP_VERSION=$(echo "${APP_DETAILS}" | jq -r '.ApplicationVersionId')
    APP_ARN=$(echo "${APP_DETAILS}" | jq -r '.ApplicationARN')
    RUNTIME_ENV=$(echo "${APP_DETAILS}" | jq -r '.RuntimeEnvironment')
    CREATE_TIME=$(echo "${APP_DETAILS}" | jq -r '.CreateTimestamp')
    LAST_UPDATE=$(echo "${APP_DETAILS}" | jq -r '.LastUpdateTimestamp')
    
    if [ "${APP_STATUS}" == "RUNNING" ]; then
        log_info "Status: ${APP_STATUS} ✓"
    elif [ "${APP_STATUS}" == "READY" ]; then
        log_warn "Status: ${APP_STATUS} (not running)"
    else
        log_error "Status: ${APP_STATUS}"
    fi
    
    log_info "Version: ${APP_VERSION}"
    log_info "Runtime: ${RUNTIME_ENV}"
    log_info "Created: ${CREATE_TIME}"
    log_info "Last Updated: ${LAST_UPDATE}"
    
    # Check for recent activity
    if [ "${APP_STATUS}" == "RUNNING" ]; then
        echo ""
        log_info "Recent log entries (last 5 minutes):"
        echo ""
        aws logs filter-log-events \
            --log-group-name "${LOG_GROUP_NAME}" \
            --region "${REGION}" \
            --start-time $(($(date +%s) * 1000 - 300000)) \
            --query 'events[*].message' \
            --output text 2>/dev/null | tail -10 | sed 's/^/    /' || log_warn "No recent logs found"
    fi
else
    log_error "Application not found: ${APP_NAME}"
fi

# Summary
log_section "Summary"

TOTAL_CHECKS=7
PASSED=0

aws s3 ls "s3://${STREAMING_APP_BUCKET}" &> /dev/null && ((PASSED++))
aws s3 ls "s3://${ICEBERG_WAREHOUSE_BUCKET}" &> /dev/null && ((PASSED++))
aws glue get-database --name "${GLUE_DATABASE_NAME}" --region "${REGION}" &> /dev/null && ((PASSED++))
aws iam get-role --role-name "${IAM_ROLE_NAME}" &> /dev/null && ((PASSED++))
aws logs describe-log-groups --log-group-name-prefix "${LOG_GROUP_NAME}" --region "${REGION}" | grep -q "${LOG_GROUP_NAME}" && ((PASSED++))
aws kinesisanalyticsv2 describe-application --application-name "${APP_NAME}" --region "${REGION}" &> /dev/null && ((PASSED++))
[ "${APP_STATUS}" == "RUNNING" ] && ((PASSED++))

echo ""
echo "Health Score: ${PASSED}/${TOTAL_CHECKS} checks passed"
echo ""

if [ ${PASSED} -eq ${TOTAL_CHECKS} ]; then
    log_info "All systems operational! ✓"
    echo ""
    echo "Useful commands:"
    echo "  - View live logs: aws logs tail ${LOG_GROUP_NAME} --follow --region ${REGION}"
    echo "  - Send test data: ./test.sh"
    echo "  - Query data: Use Athena to query tm_data_lake.claims and tm_data_lake.leave_requests"
    echo "  - Stop app: aws kinesisanalyticsv2 stop-application --application-name ${APP_NAME} --region ${REGION}"
    exit 0
elif [ ${PASSED} -ge 5 ]; then
    log_warn "System partially operational. Review warnings above."
    exit 0
else
    log_error "System has issues. Review errors above."
    exit 1
fi
