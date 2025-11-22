#!/bin/bash

# Test Script for Kinesis Data Stream
# Sends sample JSON messages (Claims, Leave Requests, Unknown) to Kinesis Data Stream

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
if [ -z "$KINESIS_STREAM_NAME" ] || [ -z "$CLAIMS_OUTPUT_BUCKET" ] || [ -z "$LEAVEREQUESTS_OUTPUT_BUCKET" ] || [ -z "$DEFAULT_OUTPUT_BUCKET" ]; then
    if [ -f "/tmp/flink-config.env" ]; then
        source /tmp/flink-config.env
    fi
fi

if [ -z "$KINESIS_STREAM_NAME" ]; then
    log_error "KINESIS_STREAM_NAME not set. Please run ./iac_create.sh first or source /tmp/flink-config.env"
    exit 1
fi

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

# Verify Kinesis stream exists
if ! aws kinesis describe-stream --stream-name "${KINESIS_STREAM_NAME}" --region "${REGION}" &> /dev/null; then
    log_error "Kinesis stream ${KINESIS_STREAM_NAME} not found in region ${REGION}"
    log_error "Please run ./iac_create.sh first to create the stream."
    exit 1
fi

echo "╔════════════════════════════════════════════════════════╗"
echo "║     Kinesis Data Stream Test                           ║"
echo "╚════════════════════════════════════════════════════════╝"
echo ""
log_info "Kinesis Stream: ${KINESIS_STREAM_NAME}"
log_info "Region: ${REGION}"
echo ""

# Create test messages
mkdir -p /tmp/test-messages

# 1. Claims Message
cat > /tmp/test-messages/claim.json << 'EOF'
{
  "messageType": "CLAIM",
  "timestamp": "2025-11-22T10:30:00Z",
  "data": {
    "claimId": "CLM-2025-001",
    "status": "Approved",
    "amount": 1250.50,
    "processedAt": "2025-11-22T10:30:00Z"
  }
}
EOF

# 2. Leave Request Message
cat > /tmp/test-messages/leave.json << 'EOF'
{
  "messageType": "LEAVE_REQUEST",
  "timestamp": "2025-11-22T10:35:00Z",
  "data": {
    "employeeId": "EMP-12345",
    "leaveType": "Vacation",
    "startDate": "2025-12-01",
    "endDate": "2025-12-15",
    "approvalStatus": "Approved"
  }
}
EOF

# 3. Unknown Message
cat > /tmp/test-messages/unknown.json << 'EOF'
{
  "messageType": "UNKNOWN_TYPE",
  "timestamp": "2025-11-22T10:40:00Z",
  "data": {
    "someField": "someValue"
  }
}
EOF

log_info "Test messages created in /tmp/test-messages/"
echo ""

# Send messages to Kinesis
log_info "Sending CLAIM message..."
CLAIM_DATA=$(cat /tmp/test-messages/claim.json | base64 -w 0)
CLAIM_RESPONSE=$(aws kinesis put-record \
    --stream-name "${KINESIS_STREAM_NAME}" \
    --partition-key "claim-001" \
    --data "${CLAIM_DATA}" \
    --region "${REGION}" \
    --output json)

CLAIM_SEQ=$(echo "$CLAIM_RESPONSE" | grep -o '"SequenceNumber": "[^"]*"' | cut -d'"' -f4)
log_data "Claim message sent - Sequence: ${CLAIM_SEQ}"
echo ""

sleep 1

log_info "Sending LEAVE_REQUEST message..."
LEAVE_DATA=$(cat /tmp/test-messages/leave.json | base64 -w 0)
LEAVE_RESPONSE=$(aws kinesis put-record \
    --stream-name "${KINESIS_STREAM_NAME}" \
    --partition-key "leave-001" \
    --data "${LEAVE_DATA}" \
    --region "${REGION}" \
    --output json)

LEAVE_SEQ=$(echo "$LEAVE_RESPONSE" | grep -o '"SequenceNumber": "[^"]*"' | cut -d'"' -f4)
log_data "Leave request message sent - Sequence: ${LEAVE_SEQ}"
echo ""

sleep 1

log_info "Sending UNKNOWN message..."
UNKNOWN_DATA=$(cat /tmp/test-messages/unknown.json | base64 -w 0)
UNKNOWN_RESPONSE=$(aws kinesis put-record \
    --stream-name "${KINESIS_STREAM_NAME}" \
    --partition-key "unknown-001" \
    --data "${UNKNOWN_DATA}" \
    --region "${REGION}" \
    --output json)

UNKNOWN_SEQ=$(echo "$UNKNOWN_RESPONSE" | grep -o '"SequenceNumber": "[^"]*"' | cut -d'"' -f4)
log_data "Unknown message sent - Sequence: ${UNKNOWN_SEQ}"
echo ""

log_info "=============================================="
log_info "Test Messages Sent Successfully!"
log_info "=============================================="
echo ""
log_info "Messages sent to stream: ${KINESIS_STREAM_NAME}"
log_data "- 1 Claim message"
log_data "- 1 Leave Request message"
log_data "- 1 Unknown message"
echo ""

log_info "Expected Output Locations:"
echo "  - Claims (Avro): s3://${CLAIMS_OUTPUT_BUCKET}/claims/"
echo "  - Leave Requests (Avro): s3://${LEAVEREQUESTS_OUTPUT_BUCKET}/leave-of-absence/"
echo "  - Unknown (Text): s3://${DEFAULT_OUTPUT_BUCKET}/unknown/"
echo ""

log_warn "Note: Files will appear in S3 after Flink's rolling policy triggers"
log_warn "      (2 minutes of data or 30 seconds of inactivity)"
echo ""

log_info "To monitor processing:"
echo "  - Check logs:"
echo "    aws logs tail /aws/kinesis-analytics/datahose-app --follow --region ${REGION}"
echo ""
echo "  - Check Claims output:"
echo "    aws s3 ls s3://${CLAIMS_OUTPUT_BUCKET}/claims/ --recursive --region ${REGION}"
echo ""
echo "  - Check Leave Requests output:"
echo "    aws s3 ls s3://${LEAVEREQUESTS_OUTPUT_BUCKET}/leave-of-absence/ --recursive --region ${REGION}"
echo ""
echo "  - Check Unknown output:"
echo "    aws s3 ls s3://${DEFAULT_OUTPUT_BUCKET}/unknown/ --recursive --region ${REGION}"
echo ""

# Optional: Send multiple messages for testing
read -p "Send 10 additional test claims? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    log_info "Sending 10 additional claim messages..."
    for i in {2..11}; do
        AMOUNT=$((RANDOM % 10000 + 100))
        cat > /tmp/test-messages/claim-${i}.json << EOF
{
  "messageType": "CLAIM",
  "timestamp": "2025-11-22T10:30:$(printf "%02d" $i)Z",
  "data": {
    "claimId": "CLM-2025-$(printf "%03d" $i)",
    "status": "Approved",
    "amount": ${AMOUNT}.50,
    "processedAt": "2025-11-22T10:30:$(printf "%02d" $i)Z"
  }
}
EOF
        
        MSG_DATA=$(cat /tmp/test-messages/claim-${i}.json | base64 -w 0)
        aws kinesis put-record \
            --stream-name "${KINESIS_STREAM_NAME}" \
            --partition-key "claim-$(printf "%03d" $i)" \
            --data "${MSG_DATA}" \
            --region "${REGION}" \
            --output text > /dev/null
        
        echo -n "."
        sleep 0.5
    done
    echo ""
    log_info "10 additional claims sent!"
fi

echo ""
log_info "Test complete!"
