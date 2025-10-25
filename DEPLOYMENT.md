# DataHose App - Deployment Guide

Complete step-by-step guide to deploy the Flink streaming application using Terraform and AWS CodePipeline.

> **See [README.md](./README.md) for architecture overview and quick reference.**

## Prerequisites

### 1. Install Tools
```bash
# Terraform (>= 1.0)
brew install terraform  # macOS
# Or download from https://www.terraform.io/downloads

# AWS CLI (>= 2.0)
brew install awscli  # macOS
aws --version
```

### 2. Configure AWS Credentials
```bash
aws configure
# Provide: Access Key ID, Secret Access Key, Region (us-east-2), Output format (json)

# Verify credentials
aws sts get-caller-identity
```

### 3. GitHub Setup

**Option A: CodeStar Connection (Recommended)**
```bash
# Create CodeStar connection via AWS Console:
# 1. Go to: AWS Console → Developer Tools → Settings → Connections
# 2. Click "Create connection"
# 3. Select "GitHub" and follow OAuth flow
# 4. Copy the Connection ARN
```

**Option B: GitHub Personal Access Token**
```bash
# 1. Create PAT: GitHub → Settings → Developer settings → Personal access tokens
# 2. Scopes needed: repo, admin:repo_hook
# 3. Store in AWS Secrets Manager:

aws secretsmanager create-secret \
  --name github-token \
  --secret-string '{"token":"ghp_your_token_here"}' \
  --region us-east-2

# Copy the secret ARN
```

## Deployment Steps

### Step 1: Configure Terraform Variables

```bash
cd terraform/

# Create terraform.tfvars from example
cp terraform.tfvars.example terraform.tfvars

# Edit terraform.tfvars
vim terraform.tfvars
```

Update these key values:
```hcl
aws_region = "us-east-2"
app_name   = "datahose-app"

# Provide your GitHub connection ARN or Secrets Manager ARN
github_token_secret_arn = "arn:aws:codestar-connections:us-east-2:ACCOUNT:connection/UUID"
# OR
# github_token_secret_arn = "arn:aws:secretsmanager:us-east-2:ACCOUNT:secret:github-token-XXXXX"

github_repo_owner = "murcoder14"
github_repo_name  = "datahose-app"
github_branch     = "main"  # or "feature/oh-lambda"
```

> **⚠️ SECURITY WARNING:** 
> - The `terraform.tfvars` file contains **sensitive information** (GitHub token ARN)
> - This file is already in `.gitignore` and should **NEVER** be committed to version control
> - Each team member should create their own `terraform.tfvars` locally
> - Use environment variables or CI/CD secrets for production deployments

### Step 2: Initialize Terraform

```bash
terraform init
```

Expected output:
```
Initializing modules...
Initializing the backend...
Initializing provider plugins...
Terraform has been successfully initialized!
```

### Step 3: Review Infrastructure Plan

```bash
terraform plan
```

Review the resources to be created:
- S3 buckets (3)
- IAM roles and policies (4 roles, 4 policies)
- CloudWatch log groups (2)
- Lambda function (1)
- CodeBuild project (1)
- CodePipeline (1)

### Step 4: Deploy Infrastructure

```bash
terraform apply
```

Type `yes` when prompted. Deployment takes ~2-3 minutes.

**Save the outputs:**
```bash
terraform output > ../deployment-outputs.txt
```

### Step 5: Verify Deployment

```bash
# Check S3 buckets
aws s3 ls | grep datahose

# Check Flink application (won't exist yet until pipeline runs)
aws kinesisanalyticsv2 list-applications --region us-east-2

# Check Lambda function
aws lambda get-function --function-name datahose-app-flink-lifecycle --region us-east-2

# Check CodePipeline
aws codepipeline list-pipelines --region us-east-2
```

### Step 6: Trigger Pipeline

**Option A: Git Push (Automatic)**
```bash
# Make a change and push to configured branch
git add .
git commit -m "Trigger pipeline"
git push origin main
```

**Option B: Manual Trigger**
```bash
aws codepipeline start-pipeline-execution \
  --name datahose-app-pipeline \
  --region us-east-2
```

### Step 7: Monitor Pipeline Execution

**Via AWS Console:**
1. Go to: AWS Console → Developer Tools → CodePipeline
2. Click `datahose-app-pipeline`
3. Watch stages: Source → Build → Deploy

**Via CLI:**
```bash
# Get pipeline status
aws codepipeline get-pipeline-state \
  --name datahose-app-pipeline \
  --region us-east-2

# Watch CodeBuild logs
aws logs tail /aws/codebuild/datahose-app-build --follow --region us-east-2

# Watch Lambda logs
aws logs tail /aws/lambda/datahose-app-flink-lifecycle --follow --region us-east-2

# Watch Flink application logs
aws logs tail /aws/kinesis-analytics/datahose-app --follow --region us-east-2
```

## Testing the Application

### 1. Upload Test Data

```bash
# Get bucket name from Terraform output
INPUT_BUCKET=$(terraform output -raw input_data_bucket)

# Create sample CSV
cat > /tmp/visits.csv <<EOF
name,visits
Alice,5
Bob,3
Alice,2
Charlie,7
Bob,1
EOF

# Upload to S3
aws s3 cp /tmp/visits.csv s3://${INPUT_BUCKET}/datafall/ --region us-east-2
```

### 2. Monitor Processing

```bash
# View Flink application status
aws kinesisanalyticsv2 describe-application \
  --application-name datahose-app \
  --region us-east-2 \
  --query 'ApplicationDetail.ApplicationStatus'

# Watch logs for processing
aws logs tail /aws/kinesis-analytics/datahose-app --follow --region us-east-2
```

### 3. Check Output Data

```bash
# Get output bucket name
OUTPUT_BUCKET=$(terraform output -raw output_data_bucket)

# List output files
aws s3 ls s3://${OUTPUT_BUCKET}/datalake/ --recursive --region us-east-2

# Download and view results
aws s3 sync s3://${OUTPUT_BUCKET}/datalake/ ./output/ --region us-east-2
cat output/*
```

Expected output:
```
Alice visited the gym 7 times
Bob visited the gym 4 times
Charlie visited the gym 7 times
```

## Management Commands

### View Application Status
```bash
aws kinesisanalyticsv2 describe-application \
  --application-name datahose-app \
  --region us-east-2

# Or use Makefile
make flink-status
```

### Stop/Start Application
```bash
# Stop
make flink-stop

# Start
make flink-start
```

### View Logs
```bash
# Flink application logs
make logs-flink

# Lambda logs
make logs-lambda

# CodeBuild logs
make logs-codebuild
```

## Updating the Application

### Code Changes
1. Make changes to `src/main/java/org/muralis/datahose/StreamingApp.java`
2. Commit and push to GitHub
3. CodePipeline automatically:
   - Builds new JAR
   - Uploads to S3
   - Stops Flink app
   - Updates app with new JAR
   - Restarts app

### Infrastructure Changes
```bash
cd terraform/

# Edit variables or module files
vim variables.tf

# Plan changes
terraform plan

# Apply changes
terraform apply
```

## Troubleshooting

### Pipeline Fails at Build Stage
```bash
make logs-codebuild

# Common issues:
# - Maven build errors: Check pom.xml syntax
# - Java version mismatch: Ensure buildspec.yml uses Java 11
# - S3 upload permissions: Verify CodeBuild IAM role
```

### Pipeline Fails at Deploy Stage
```bash
make logs-lambda

# Common issues:
# - Flink app stuck in transition: Wait 5-10 minutes or manually stop
# - IAM permission denied: Check Lambda execution role has KDA permissions
# - JAR file not found: Verify S3 upload succeeded in Build stage
```

### Flink Application Fails to Start
```bash
make logs-flink

# Common issues:
# - RuntimeExecutionMode mismatch: Check StreamingApp.java (should be STREAMING)
# - S3 read permissions: Verify Flink execution role can read input bucket
# - Invalid environment properties: Check Lambda environment variables
```

### Lambda Timeout
If Lambda times out during Flink status polling:
```bash
# Edit terraform/modules/lambda/main.tf
timeout = 600  # Increase from 300 to 600

terraform apply
```

## Cost Estimation

**Monthly costs (us-east-2, light usage):**
- Flink (1 KPU, 24/7): ~$160.00
- S3 storage (10 GB): ~$0.23
- Lambda (100 invocations): ~$0.00
- CodeBuild (10 builds): ~$0.10
- CloudWatch Logs (1 GB): ~$0.50
- CodePipeline: ~$1.00
- **Total: ~$162/month**

**Cost optimization:**
```bash
# Stop Flink when not in use (saves ~$160/month)
make flink-stop

# Start when needed
make flink-start
```

## Cleanup

### Destroy All Resources
```bash
cd terraform/
terraform destroy
# Type 'yes' when prompted

# Or use Makefile
make destroy
```

**Note:** Terraform will handle deleting all resources including the Flink application, S3 buckets (if empty), IAM roles, Lambda function, and CodePipeline.

## Next Steps

- Set up Terraform remote state (S3 + DynamoDB) for team collaboration
- Add SNS notifications for pipeline failures
- Configure Flink auto-scaling
- Implement CloudWatch dashboards
- Set up blue/green deployments

## Support

For detailed architecture information, see [README.md](./README.md)

**Useful commands:**
- `make help` - Show all available commands
- `make logs-flink` - View Flink application logs
- `make flink-status` - Check Flink application status
- `make pipeline-status` - Check pipeline status
