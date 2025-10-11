#!/bin/bash

# Test Script for Kinesis Data Stream
# Generates random 4-character lowercase strings and sends them to Kinesis
# Runs continuously with 5-second intervals



# Color codes for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
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

# Configuration
KINESIS_STREAM_NAME="${KINESIS_STREAM_NAME:-tm-input-stream}"
REGION=$(aws configure get region 2>/dev/null)
if [ -z "$REGION" ]; then
    echo "ERROR: No default region configured. Please run: aws configure set region us-east-2"
    exit 1
fi

# Function to generate random string
generate_random_string() {
    local length=$1
    local chars="abcdefghijklmnopqrstuvwxyz"
    local random_string=""

    for (( i=0; i<$length; i++ )); do
        random_string+=${chars:$RANDOM%${#chars}:1}
    done
    
    echo "$random_string"
}

# Check if AWS CLI is installed
if ! command -v aws &> /dev/null; then
    echo "ERROR: AWS CLI is not installed. Please install it first."
    exit 1
fi

# Verify AWS credentials
if ! aws sts get-caller-identity &> /dev/null; then
    echo "ERROR: AWS credentials are not configured properly."
    exit 1
fi

# Verify Kinesis stream exists
if ! aws kinesis describe-stream --stream-name "${KINESIS_STREAM_NAME}" --region "${REGION}" &> /dev/null; then
    echo "ERROR: Kinesis stream ${KINESIS_STREAM_NAME} not found in region ${REGION}"
    echo "Please run ./iac_create.sh first to create the stream."
    exit 1
fi

echo "╔════════════════════════════════════════════════════════╗"
echo "║     Kinesis Data Stream Test Producer                 ║"
echo "╚════════════════════════════════════════════════════════╝"
echo ""
log_info "Stream: ${KINESIS_STREAM_NAME}"
log_info "Region: ${REGION}"
log_info "Interval: 5 seconds"
log_info "Data: 5 random 4-character lowercase words per batch"
echo ""
log_warn "Press Ctrl+C to stop"
echo ""

# Counter for records sent
RECORD_COUNT=0
BATCH_COUNT=0

# Trap Ctrl+C for graceful shutdown
trap ctrl_c INT

function ctrl_c() {
    echo ""
    echo ""
    log_info "=============================================="
    log_info "Test Producer Stopped"
    log_info "=============================================="
    echo "  Total batches sent: ${BATCH_COUNT}"
    echo "  Total records sent: ${RECORD_COUNT}"
    echo ""
    exit 0
}

# Main loop - generate and send data continuously
while true; do
    echo "LOOP START"
    ((BATCH_COUNT++))
    
    echo -e "${YELLOW}[Batch ${BATCH_COUNT}]${NC} Generating 5 random words..."
    
    # Generate 5 random words
    WORDS=()
    for i in {1..5}; do
        WORD=$(generate_random_string 4)
        WORDS+=("$WORD")
    done
    
    # Display the words
    log_data "Words: ${WORDS[*]}"
    
    # Send each word to Kinesis
    for WORD in "${WORDS[@]}"; do
        # Create partition key from word
        PARTITION_KEY="${WORD}-${RANDOM}"
        
        # Send to Kinesis
        # Enclose the word in double quotes for Kinesis (plain string)
        QUOTED_WORD="\"${WORD}\""
        RESULT=$(aws kinesis put-record \
            --stream-name "${KINESIS_STREAM_NAME}" \
            --region "${REGION}" \
            --partition-key "${PARTITION_KEY}" \
            --cli-binary-format raw-in-base64-out \
            --data "${QUOTED_WORD}" \
            --output json 2>&1)
        
        if [ $? -eq 0 ]; then
            ((RECORD_COUNT++))
            SHARD_ID=$(echo "$RESULT" | jq -r '.ShardId')
            SEQUENCE_NUM=$(echo "$RESULT" | jq -r '.SequenceNumber')
            log_info "Sent '${WORD}' → Shard: ${SHARD_ID}, Seq: ${SEQUENCE_NUM}"
        else
            log_warn "Failed to send '${WORD}': ${RESULT}"
        fi
    done
    
    echo ""
    echo -e "${GREEN}Summary:${NC} Batch ${BATCH_COUNT} complete. Total records: ${RECORD_COUNT}"
    echo -e "${YELLOW}Waiting 5 seconds...${NC}"
    echo ""
    
    # Wait 5 seconds before next batch
    sleep 5
done
