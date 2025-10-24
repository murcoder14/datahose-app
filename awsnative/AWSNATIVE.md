# AWS Native Infrastructure and CI/CD Setup

This document describes the AWS-native Infrastructure as Code (IaC) and CI/CD pipeline implementation for the Flink Streaming Application using AWS CloudFormation, CodePipeline, and CodeBuild.

## Overview

This setup provides a fully automated, cloud-native alternative to the shell script-based deployment (`iac_create.sh`, `iac_destroy.sh`, `cicd.sh`). It uses:

- **AWS CloudFormation**: Infrastructure as Code for reproducible deployments
- **AWS CodePipeline**: Continuous Integration/Continuous Deployment pipeline
- **AWS CodeBuild**: Build and deployment automation
- **GitHub Integration**: Automatic triggering on code changes

## Architecture

### Infrastructure Components

The CloudFormation templates create the following resources:

1. **S3 Buckets**
   - Application JAR storage bucket (versioned)
   - Data sink bucket for Flink output (versioned)
   - Pipeline artifacts bucket

2. **Kinesis Data Stream**
   - Configurable shard count
   - 24-hour retention period

3. **IAM Roles and Policies**
   - Flink service role with permissions for S3, Kinesis, CloudWatch
   - Kinesis producer policy for data ingestion
   - CodeBuild service role
   - CodePipeline service role

4. **CloudWatch Resources**
   - Log groups for Flink application
   - Log streams with configurable retention

5. **CI/CD Pipeline**
   - CodePipeline for orchestration
   - CodeBuild project for Maven builds
   - GitHub webhook for automatic triggering

## Directory Structure

```
datahose-app/
├── ./
│   ├── infrastructure.yaml        # Infrastructure CloudFormation template
│   ├── cicd-pipeline.yaml        # CI/CD pipeline CloudFormation template
│   ├── deploy-infrastructure.sh  # Deploy infrastructure stack
│   ├── destroy-infrastructure.sh # Destroy infrastructure stack
│   └── deploy-pipeline.sh        # Deploy CI/CD pipeline stack
├── buildspec.yml                 # CodeBuild build specification
├── pom.xml                       # Maven project file
└── src/                          # Java source code
```

## Prerequisites

1. **AWS CLI**: Version 2.x or later
   ```bash
   aws --version
   ```

2. **AWS Credentials**: Configured with appropriate permissions
   ```bash
   aws configure
   aws sts get-caller-identity
   ```

3. **jq**: JSON processor (for parsing CloudFormation outputs)
   ```bash
   sudo dnf install jq  # Fedora/RHEL
   # or
   sudo apt install jq  # Ubuntu/Debian
   ```

4. **GitHub Repository**: Your code repository on GitHub

5. **GitHub Personal Access Token**: With `repo` and `admin:repo_hook` permissions

## Quick Start

### Step 1: Deploy Infrastructure

Deploy the base infrastructure (S3, Kinesis, IAM roles, CloudWatch):

```bash
cd awsnative
./deploy-infrastructure.sh
```

This will create a CloudFormation stack named `datahose-app-infrastructure` with all required resources.

**Configuration saved to**: `/tmp/flink-config.env`

Load the configuration:
```bash
source /tmp/flink-config.env
```

### Step 2: Store GitHub Token in AWS Secrets Manager

Create a secret in AWS Secrets Manager for your GitHub Personal Access Token:

```bash
aws secretsmanager create-secret \
  --name github/personal-access-token \
  --description "GitHub Personal Access Token for CodePipeline" \
  --secret-string '{"token":"YOUR_GITHUB_TOKEN_HERE"}' \
  --region us-east-2
```

**GitHub Token Permissions Required**:
- `repo` - Full control of private repositories
- `admin:repo_hook` - Full control of repository hooks

### Step 3: Deploy CI/CD Pipeline

Deploy the automated CI/CD pipeline:

```bash
cd awsnative
./deploy-pipeline.sh
```

You'll be prompted for:
- GitHub repository owner (username or organization)
- GitHub repository name
- GitHub branch to build from (default: `main`)
- Secrets Manager secret name (default: `github/personal-access-token`)

The script will create:
- CodePipeline for orchestration
- CodeBuild project for builds
- GitHub webhook for automatic triggering
- S3 bucket for build artifacts

### Step 4: Trigger the Pipeline

The pipeline will automatically trigger on every push to the configured branch.

**Manual trigger**:
```bash
aws codepipeline start-pipeline-execution \
  --name datahose-app-pipeline \
  --region us-east-2
```

## Infrastructure Template (`infrastructure.yaml`)

### Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `ApplicationName` | `datahose-app` | Name of the Flink application |
| `TableName` | `datafall` | S3 table name for data sink |
| `KinesisStreamName` | `tm-input-stream` | Kinesis Data Stream name |
| `KinesisShardCount` | `1` | Number of Kinesis shards |
| `LogRetentionDays` | `7` | CloudWatch log retention period |
| `KinesisProducerUserName` | `sunny0524` | IAM user for Kinesis producer |

### Outputs

The template exports these values for use by the CI/CD pipeline:

- `StreamingAppBucketName` - S3 bucket for JAR files
- `DataBucketName` - S3 bucket for output data
- `KinesisStreamArn` - ARN of the Kinesis stream
- `FlinkRoleArn` - IAM role ARN for Flink
- `FlinkLogGroupName` - CloudWatch log group
- `TableName` - S3 table name

### Custom Deployment

Override parameters during deployment:

```bash
export APP_NAME="my-custom-app"
export KINESIS_STREAM_NAME="my-stream"
export LOG_RETENTION_DAYS=30
./deploy-infrastructure.sh
```

## CI/CD Pipeline Template (`cicd-pipeline.yaml`)

### Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `ApplicationName` | `datahose-app` | Application name |
| `InfrastructureStackName` | `datahose-app-infrastructure` | Infrastructure stack name |
| `GitHubOwner` | *required* | GitHub owner |
| `GitHubRepo` | `datahose-app` | GitHub repository |
| `GitHubBranch` | `main` | Branch to build |
| `GitHubTokenSecretName` | `github/personal-access-token` | Secret name |
| `BuildComputeType` | `BUILD_GENERAL1_SMALL` | CodeBuild instance size |

### Pipeline Stages

1. **Source Stage**: Pulls code from GitHub
2. **Build Stage**: 
   - Maven clean and package
   - Upload JAR to S3
   - Create/update Flink application
   - Start Flink application

## Build Specification (`buildspec.yml`)

The CodeBuild project follows these phases:

### Install Phase
- Sets up Java 11 (Corretto)
- Verifies Maven and AWS CLI

### Pre-Build Phase
- Loads configuration from infrastructure stack
- Verifies AWS credentials
- Sets environment variables

### Build Phase
- Runs `mvn clean package -DskipTests`
- Validates JAR file size (< 512MB)

### Post-Build Phase
- Uploads JAR to S3
- Creates/updates Flink application
- Configures application properties (Kinesis, S3)
- Starts the application

### Build Cache
Maven dependencies are cached in S3 to speed up subsequent builds.

## Environment Variables

The pipeline uses these environment variables (auto-configured):

```bash
APP_NAME=datahose-app
INFRASTRUCTURE_STACK_NAME=datahose-app-infrastructure
AWS_DEFAULT_REGION=us-east-2
STREAMING_APP_BUCKET=tm-streaming-app-bucket-{AccountId}-{Region}
DATA_BUCKET=tm-data-bucket-{AccountId}-{Region}
KINESIS_STREAM_ARN=arn:aws:kinesis:{Region}:{AccountId}:stream/tm-input-stream
FLINK_ROLE_ARN=arn:aws:iam::{AccountId}:role/datahose-app-flink-role
S3_TABLE=datafall
```

## Monitoring and Logs

### CloudWatch Logs

View Flink application logs:
```bash
aws logs tail /aws/kinesis-analytics/datahose-app --follow
```

View CodeBuild logs:
```bash
aws logs tail /aws/codebuild/datahose-app-build --follow
```

### CodePipeline Console

Monitor pipeline execution:
```
https://us-east-2.console.aws.amazon.com/codesuite/codepipeline/pipelines/datahose-app-pipeline/view
```

### CodeBuild Console

View build details:
```
https://us-east-2.console.aws.amazon.com/codesuite/codebuild/projects/datahose-app-build
```

### Flink Application Status

```bash
aws kinesisanalyticsv2 describe-application \
  --application-name datahose-app \
  --region us-east-2
```

## Data Output

View processed data in S3:
```bash
aws s3 ls s3://tm-data-bucket-{AccountId}-{Region}/datafall/ --recursive
```

Download output:
```bash
aws s3 sync s3://tm-data-bucket-{AccountId}-{Region}/datafall/ ./output/
```

## Managing the Infrastructure

### Update Infrastructure

Modify parameters in the CloudFormation template or export environment variables, then run:

```bash
cd awsnative
./deploy-infrastructure.sh
```

CloudFormation will perform an update-in-place.

### Update Pipeline

```bash
cd awsnative
./deploy-pipeline.sh
```

### Destroy Everything

**Warning**: This will delete all resources and data!

```bash
# Stop the Flink application first (if running)
aws kinesisanalyticsv2 stop-application \
  --application-name datahose-app \
  --region us-east-2 \
  --force

# Delete the pipeline stack (optional)
aws cloudformation delete-stack \
  --stack-name datahose-app-pipeline \
  --region us-east-2

# Destroy infrastructure
cd awsnative
./destroy-infrastructure.sh

# Or use force mode (no confirmation)
./destroy-infrastructure.sh --force
```

## Comparison: Shell Scripts vs CloudFormation

| Feature | Shell Scripts | CloudFormation |
|---------|--------------|----------------|
| **Reproducibility** | Manual, prone to drift | Declarative, version-controlled |
| **Rollback** | Manual cleanup | Automatic rollback on failure |
| **Change Management** | No history | Full change sets and history |
| **Parallel Creation** | Sequential | Parallel resource creation |
| **Error Handling** | Manual intervention | Built-in retry and error handling |
| **Dependencies** | Manual ordering | Automatic dependency resolution |
| **Cross-Region** | Manual adaptation | Easy multi-region deployment |
| **Cost Tracking** | Manual tagging | Automatic stack-level tagging |

### What the Shell Scripts Do

- **`iac_create.sh`**: Creates resources sequentially using AWS CLI
- **`iac_destroy.sh`**: Deletes resources in reverse order
- **`cicd.sh`**: Builds and deploys manually

### What CloudFormation Does

- **`deploy-infrastructure.sh`**: Deploys entire stack declaratively
- **`destroy-infrastructure.sh`**: Deletes stack with automatic cleanup
- **CodePipeline**: Automates build/deploy on every commit

## Troubleshooting

### Stack Creation Failed

View stack events:
```bash
aws cloudformation describe-stack-events \
  --stack-name datahose-app-infrastructure \
  --region us-east-2 \
  --query 'StackEvents[?ResourceStatus==`CREATE_FAILED`]'
```

### Pipeline Fails to Start

Check pipeline execution:
```bash
aws codepipeline get-pipeline-state \
  --name datahose-app-pipeline \
  --region us-east-2
```

### Build Failures

View CodeBuild logs:
```bash
aws codebuild batch-get-builds \
  --ids $(aws codepipeline get-pipeline-state \
    --name datahose-app-pipeline \
    --query 'stageStates[?stageName==`Build`].latestExecution.externalExecutionId' \
    --output text) \
  --region us-east-2
```

### Flink Application Not Starting

Check application status:
```bash
aws kinesisanalyticsv2 describe-application \
  --application-name datahose-app \
  --region us-east-2 \
  --query 'ApplicationDetail.{Status:ApplicationStatus,StatusReason:ApplicationDescription}'
```

View application logs:
```bash
aws logs tail /aws/kinesis-analytics/datahose-app --follow --since 10m
```

## Cost Optimization

### Development Environment
- Use `BUILD_GENERAL1_SMALL` for CodeBuild
- Set Kinesis to 1 shard
- Configure short log retention (7 days)

### Production Environment
- Increase Kinesis shards based on throughput
- Use `BUILD_GENERAL1_MEDIUM` or larger
- Extend log retention (30-90 days)
- Enable S3 lifecycle policies

### S3 Lifecycle Policy Example

Add to `infrastructure.yaml`:
```yaml
LifecycleConfiguration:
  Rules:
    - Id: ArchiveOldData
      Status: Enabled
      Transitions:
        - TransitionInDays: 90
          StorageClass: GLACIER
      ExpirationInDays: 365
```

## Security Best Practices

1. **Secrets Management**: Store sensitive data in AWS Secrets Manager
2. **Least Privilege**: IAM roles have minimum required permissions
3. **Encryption**: All S3 buckets use AES256 encryption
4. **Public Access**: Blocked on all S3 buckets
5. **Versioning**: Enabled on all S3 buckets
6. **VPC**: Can be configured for Flink application isolation

## Advanced Configuration

### Multi-Environment Deployment

Deploy to different environments:

```bash
# Development
export STACK_NAME="datahose-app-infrastructure-dev"
export KINESIS_SHARD_COUNT=1
./deploy-infrastructure.sh

# Production
export STACK_NAME="datahose-app-infrastructure-prod"
export KINESIS_SHARD_COUNT=3
./deploy-infrastructure.sh
```

### Custom Build Image

Modify `cicd-pipeline.yaml`:
```yaml
Parameters:
  BuildImage:
    Type: String
    Default: aws/codebuild/amazonlinux2-x86_64-standard:5.0
```

### Flink Parallelism

Update in `buildspec.yml`:
```json
"ParallelismConfiguration": {
  "ConfigurationType": "CUSTOM",
  "Parallelism": 4,
  "ParallelismPerKPU": 2,
  "AutoScalingEnabled": true
}
```

## References

- [AWS CloudFormation Documentation](https://docs.aws.amazon.com/./)
- [AWS CodePipeline Documentation](https://docs.aws.amazon.com/codepipeline/)
- [AWS CodeBuild Documentation](https://docs.aws.amazon.com/codebuild/)
- [Managed Service for Apache Flink](https://docs.aws.amazon.com/kinesisanalytics/)
- [Original Shell Scripts](./iac_create.sh)

## Support

For issues or questions:
1. Check CloudFormation stack events
2. Review CodeBuild logs
3. Examine Flink application logs in CloudWatch
4. Verify IAM permissions and roles

## License

Same as the main project.
