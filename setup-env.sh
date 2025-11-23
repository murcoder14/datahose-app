#!/bin/bash

# Environment Setup Script for Flink Application Development
# Source this script in your terminal: source ./setup-env.sh

# Color codes
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}Setting up development environment...${NC}"

# Initialize SDKMAN
if [ -s "$HOME/.sdkman/bin/sdkman-init.sh" ]; then
    source "$HOME/.sdkman/bin/sdkman-init.sh"
    echo -e "${GREEN}✓${NC} SDKMAN initialized"
else
    echo -e "${YELLOW}!${NC} SDKMAN not found at $HOME/.sdkman/bin/sdkman-init.sh"
fi

# Set Java 11
sdk use java 11.0.29-amzn
echo -e "${GREEN}✓${NC} Java version: $(java -version 2>&1 | head -n 1)"

# Load Flink configuration if it exists
if [ -f "/tmp/flink-config.env" ]; then
    source /tmp/flink-config.env
    echo -e "${GREEN}✓${NC} Flink configuration loaded"
    echo "  - Application: ${APP_NAME}"
    echo "  - Region: ${REGION}"
else
    echo -e "${YELLOW}!${NC} Flink configuration not found. Run ./iac_create.sh first."
fi

# Get region from AWS CLI default profile configuration
if [ -z "${AWS_DEFAULT_REGION}" ]; then
    export AWS_DEFAULT_REGION=$(aws configure get region 2>/dev/null)
    if [ -z "${AWS_DEFAULT_REGION}" ]; then
        echo -e "${YELLOW}!${NC} No default region configured in AWS CLI. Please run: aws configure set region us-east-2"
    else
        echo -e "${GREEN}✓${NC} AWS_DEFAULT_REGION set to: ${AWS_DEFAULT_REGION}"
    fi
fi

echo ""
echo -e "${GREEN}Environment ready!${NC}"
echo ""
echo "Available commands:"
echo "  - ./iac_create.sh   # Create AWS infrastructure"
echo "  - ./cicd.sh         # Build and deploy application"
echo "  - ./verify.sh       # Verify deployment"
echo "  - ./iac_destroy.sh  # Destroy infrastructure"
echo ""
