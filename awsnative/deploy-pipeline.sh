#!/bin/bash

# Deploy CI/CD Pipeline using CloudFormation
# This script creates the CodePipeline and CodeBuild resources for automated deployment

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

# Configuration
STACK_NAME="${PIPELINE_STACK_NAME:-datahose-app-pipeline}"
INFRASTRUCTURE_STACK_NAME="${INFRASTRUCTURE_STACK_NAME:-datahose-app-infrastructure}"
TEMPLATE_FILE="$(dirname "$0")/cicd-pipeline.yaml"
APP_NAME="${APP_NAME:-datahose-app}"

# Get region from AWS CLI default profile configuration
REGION=$(aws configure get region 2>/dev/null)
if [ -z "$REGION" ]; then
    log_error "No default region configured. Please run: aws configure set region <your-region>"
    exit 1
fi

log_info "=============================================="
log_info "Deploying CI/CD Pipeline with CloudFormation"
log_info "=============================================="
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

# Check if infrastructure stack exists
log_step "Verifying infrastructure stack exists..."
INFRA_STACK_STATUS=$(aws cloudformation describe-stacks \
    --stack-name "${INFRASTRUCTURE_STACK_NAME}" \
    --region "${REGION}" \
    --query 'Stacks[0].StackStatus' \
    --output text 2>/dev/null || echo "DOES_NOT_EXIST")

if [ "${INFRA_STACK_STATUS}" == "DOES_NOT_EXIST" ]; then
    log_error "Infrastructure stack ${INFRASTRUCTURE_STACK_NAME} does not exist!"
    log_error "Please deploy infrastructure first:"
    log_error "  cd cloudformation && ./deploy-infrastructure.sh"
    exit 1
elif [ "${INFRA_STACK_STATUS}" != "CREATE_COMPLETE" ] && [ "${INFRA_STACK_STATUS}" != "UPDATE_COMPLETE" ]; then
    log_error "Infrastructure stack is not in a valid state: ${INFRA_STACK_STATUS}"
    exit 1
fi

log_info "Infrastructure stack found: ${INFRASTRUCTURE_STACK_NAME} (${INFRA_STACK_STATUS})"

# Prompt for GitHub configuration
echo ""
log_step "GitHub Configuration"
echo ""

if [ -z "$GITHUB_OWNER" ]; then
    read -p "Enter GitHub repository owner (username or organization): " GITHUB_OWNER
fi

if [ -z "$GITHUB_REPO" ]; then
    read -p "Enter GitHub repository name [${APP_NAME}]: " GITHUB_REPO
    GITHUB_REPO=${GITHUB_REPO:-${APP_NAME}}
fi

if [ -z "$GITHUB_BRANCH" ]; then
    read -p "Enter GitHub branch to build from [main]: " GITHUB_BRANCH
    GITHUB_BRANCH=${GITHUB_BRANCH:-main}
fi

if [ -z "$GITHUB_TOKEN_SECRET" ]; then
    read -p "Enter AWS Secrets Manager secret name for GitHub token [github/personal-access-token]: " GITHUB_TOKEN_SECRET
    GITHUB_TOKEN_SECRET=${GITHUB_TOKEN_SECRET:-github/personal-access-token}
fi

echo ""
log_info "Pipeline Configuration:"
echo "  - Stack Name: ${STACK_NAME}"
echo "  - Infrastructure Stack: ${INFRASTRUCTURE_STACK_NAME}"
echo "  - Application Name: ${APP_NAME}"
echo "  - Region: ${REGION}"
echo "  - GitHub Owner: ${GITHUB_OWNER}"
echo "  - GitHub Repo: ${GITHUB_REPO}"
echo "  - GitHub Branch: ${GITHUB_BRANCH}"
echo "  - GitHub Token Secret: ${GITHUB_TOKEN_SECRET}"
echo "  - Template: ${TEMPLATE_FILE}"
echo ""

# Check if GitHub token secret exists
log_step "Verifying GitHub token secret..."
if ! aws secretsmanager describe-secret --secret-id "${GITHUB_TOKEN_SECRET}" --region "${REGION}" &> /dev/null; then
    log_error "GitHub token secret not found: ${GITHUB_TOKEN_SECRET}"
    echo ""
    log_info "To create the secret, run:"
    log_info "  aws secretsmanager create-secret \\"
    log_info "    --name ${GITHUB_TOKEN_SECRET} \\"
    log_info "    --description 'GitHub Personal Access Token for CodePipeline' \\"
    log_info "    --secret-string '{\"token\":\"YOUR_GITHUB_TOKEN\"}' \\"
    log_info "    --region ${REGION}"
    echo ""
    log_info "Your GitHub token needs the following permissions:"
    log_info "  - repo (Full control of private repositories)"
    log_info "  - admin:repo_hook (Full control of repository hooks)"
    echo ""
    exit 1
fi

log_info "GitHub token secret verified."

# Check if template file exists
if [ ! -f "${TEMPLATE_FILE}" ]; then
    log_error "Template file not found: ${TEMPLATE_FILE}"
    exit 1
fi

# Validate CloudFormation template
log_step "Validating CloudFormation template..."
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
        log_error "  aws cloudformation delete-stack --stack-name ${STACK_NAME} --region ${REGION}"
        exit 1
    fi
    
    log_step "Updating existing stack..."
    OPERATION="update-stack"
    
    # Try to update the stack
    if aws cloudformation update-stack \
        --stack-name "${STACK_NAME}" \
        --template-body "file://${TEMPLATE_FILE}" \
        --parameters \
            ParameterKey=ApplicationName,ParameterValue="${APP_NAME}" \
            ParameterKey=InfrastructureStackName,ParameterValue="${INFRASTRUCTURE_STACK_NAME}" \
            ParameterKey=GitHubOwner,ParameterValue="${GITHUB_OWNER}" \
            ParameterKey=GitHubRepo,ParameterValue="${GITHUB_REPO}" \
            ParameterKey=GitHubBranch,ParameterValue="${GITHUB_BRANCH}" \
            ParameterKey=GitHubTokenSecretName,ParameterValue="${GITHUB_TOKEN_SECRET}" \
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
    log_step "Creating new stack..."
    OPERATION="create-stack"
    
    aws cloudformation create-stack \
        --stack-name "${STACK_NAME}" \
        --template-body "file://${TEMPLATE_FILE}" \
        --parameters \
            ParameterKey=ApplicationName,ParameterValue="${APP_NAME}" \
            ParameterKey=InfrastructureStackName,ParameterValue="${INFRASTRUCTURE_STACK_NAME}" \
            ParameterKey=GitHubOwner,ParameterValue="${GITHUB_OWNER}" \
            ParameterKey=GitHubRepo,ParameterValue="${GITHUB_REPO}" \
            ParameterKey=GitHubBranch,ParameterValue="${GITHUB_BRANCH}" \
            ParameterKey=GitHubTokenSecretName,ParameterValue="${GITHUB_TOKEN_SECRET}" \
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
log_step "Retrieving stack outputs..."
OUTPUTS=$(aws cloudformation describe-stacks \
    --stack-name "${STACK_NAME}" \
    --region "${REGION}" \
    --query 'Stacks[0].Outputs')

PIPELINE_NAME=$(echo "${OUTPUTS}" | jq -r '.[] | select(.OutputKey=="PipelineName") | .OutputValue')
PIPELINE_URL=$(echo "${OUTPUTS}" | jq -r '.[] | select(.OutputKey=="PipelineUrl") | .OutputValue')
CODEBUILD_PROJECT=$(echo "${OUTPUTS}" | jq -r '.[] | select(.OutputKey=="CodeBuildProjectName") | .OutputValue')
CODEBUILD_URL=$(echo "${OUTPUTS}" | jq -r '.[] | select(.OutputKey=="CodeBuildProjectUrl") | .OutputValue')
ARTIFACT_BUCKET=$(echo "${OUTPUTS}" | jq -r '.[] | select(.OutputKey=="ArtifactBucketName") | .OutputValue')

# Output summary
log_info "=============================================="
log_info "CI/CD Pipeline Deployment Complete!"
log_info "=============================================="
echo ""
log_info "Stack Details:"
echo "  - Stack Name: ${STACK_NAME}"
echo "  - Stack Status: CREATE_COMPLETE or UPDATE_COMPLETE"
echo ""
log_info "Pipeline Resources Created:"
echo "  - CodePipeline: ${PIPELINE_NAME}"
echo "  - CodeBuild Project: ${CODEBUILD_PROJECT}"
echo "  - Artifact Bucket: ${ARTIFACT_BUCKET}"
echo ""
log_info "Console URLs:"
echo "  - Pipeline: ${PIPELINE_URL}"
echo "  - CodeBuild: ${CODEBUILD_URL}"
echo ""
log_info "GitHub Webhook:"
echo "  - Webhook has been automatically registered with GitHub"
echo "  - Push to ${GITHUB_BRANCH} branch will trigger the pipeline"
echo ""
log_info "Next Steps:"
echo "  1. Push code to GitHub repository: ${GITHUB_OWNER}/${GITHUB_REPO}"
echo "  2. Pipeline will automatically build and deploy on push to ${GITHUB_BRANCH}"
echo "  3. Monitor pipeline: ${PIPELINE_URL}"
echo ""
log_info "Manual Pipeline Execution:"
echo "  aws codepipeline start-pipeline-execution --name ${PIPELINE_NAME} --region ${REGION}"
echo ""
log_info "To trigger the pipeline now, push your code to GitHub or run:"
echo "  aws codepipeline start-pipeline-execution --name ${PIPELINE_NAME} --region ${REGION}"
