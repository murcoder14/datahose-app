# DataHose App - Enterprise Flink Streaming Platform

[![Terraform](https://img.shields.io/badge/Terraform-1.0+-623CE4?logo=terraform)](https://www.terraform.io/)
[![AWS](https://img.shields.io/badge/AWS-Managed_Flink-FF9900?logo=amazon-aws)](https://aws.amazon.com/managed-service-apache-flink/)
[![Python](https://img.shields.io/badge/Python-3.13-3776AB?logo=python)](https://www.python.org/)
[![Java](https://img.shields.io/badge/Java-11-007396?logo=java)](https://www.java.com/)

Real-time streaming analytics platform built with Apache Flink on AWS, featuring enterprise-grade infrastructure as code (Terraform) and fully automated CI/CD pipelines.

## 🚀 Overview

This project provides a **production-ready streaming analytics platform** that processes CSV data in real-time using Apache Flink on AWS.

### ✨ Key Features

- **Infrastructure as Code**: Modular Terraform for S3, IAM, CloudWatch, Lambda, CodePipeline
- **Automated CI/CD**: GitHub → CodeBuild → Lambda → Flink (fully automated)
- **Lifecycle Management**: Python 3.13 Lambda function manages Flink operations
- **Developer Tooling**: Makefile with 20+ commands for deployment, monitoring, testing
- **Production Ready**: Least-privilege IAM, CloudWatch logging, error handling, retries

### 📊 Architecture

```
GitHub Push → CodePipeline → CodeBuild (Maven) → S3 (JAR) 
                                ↓
                            Lambda (Lifecycle)
                                ↓
                        Managed Flink App
                        ↓               ↓
                S3 Input (CSV)    S3 Output (Results)
```

**Detailed Architecture:**
```
┌─────────────┐
│   GitHub    │ (webhook on push)
└──────┬──────┘
       ↓
┌──────────────────────────────────────┐
│      AWS CodePipeline                │
│  ┌─────────┐  ┌──────────┐  ┌──────┐│
│  │ Source  │→ │  Build   │→ │Deploy││
│  │(GitHub) │  │(CodeBuild)│ │(Lambda)│
│  └─────────┘  └────┬─────┘  └───┬──┘│
└──────────────────┼──────────────┼────┘
                   ↓              ↓
           ┌───────────┐   ┌──────────────┐
           │ S3 JAR    │   │ Lambda       │
           │ Bucket    │   │ Lifecycle    │
           └───────────┘   └──────┬───────┘
                                  ↓
                      ┌────────────────────┐
                      │ Managed Flink App  │
                      │ (Apache Flink 1.20)│
                      └─────┬────────┬─────┘
                            ↓        ↓
                    ┌───────────┐  ┌────────────┐
                    │S3 Input   │  │S3 Output   │
                    │(datafall/)│  │(datalake/) │
                    └───────────┘  └────────────┘
```

**IAM Roles:**
- **Flink Role**: Read JAR, read input data, write output data, CloudWatch logs
- **Lambda Role**: Manage Flink lifecycle (KDA API), PassRole, read JAR metadata
- **CodeBuild Role**: Upload JAR to S3, CloudWatch logs
- **CodePipeline Role**: GitHub access, trigger CodeBuild, invoke Lambda

## 🎯 Quick Start

### Prerequisites
```bash
# Install Terraform
brew install terraform  # macOS
# Or download from https://www.terraform.io/downloads

# Install AWS CLI
brew install awscli
aws configure  # Set credentials and region (us-east-2)
```

### Deploy in 3 Steps

**1. Configure GitHub Integration**

Create a CodeStar Connection:
```bash
# AWS Console → Developer Tools → Settings → Connections
# Create connection → GitHub → Authorize
# Copy the Connection ARN
```

**2. Configure and Deploy**

```bash
# Clone repository
git clone https://github.com/murcoder14/datahose-app.git
cd datahose-app

# Run interactive setup
./setup.sh

# Or manually:
cd terraform/
cp terraform.tfvars.example terraform.tfvars
vim terraform.tfvars  # Add GitHub connection ARN

# Deploy
make deploy
```

> **🔒 Security Note:** The `terraform.tfvars` file contains sensitive information (GitHub token ARN) and is already in `.gitignore`. **Never commit this file** to version control!

**3. Test the Pipeline**

```bash
# Trigger pipeline
make pipeline-start

# Monitor
make logs-flink

# Upload test data
make test-upload

# Check output
make test-check-output
```

## 📁 Project Structure

```
datahose-app/
├── terraform/                    # Infrastructure as Code
│   ├── main.tf                  # Root module orchestration
│   ├── variables.tf             # Input variables
│   ├── outputs.tf               # Output values
│   ├── terraform.tfvars.example # Configuration template
│   └── modules/
│       ├── s3/                  # S3 buckets (app, input, output)
│       ├── iam/                 # IAM roles & policies
│       ├── cloudwatch/          # Log groups
│       ├── lambda/              # Flink lifecycle function
│       └── cicd/                # CodePipeline + CodeBuild
│
├── lambda/
│   └── flink_lifecycle.py       # Python 3.13 Lambda function
│
├── src/main/java/               # Flink application (Java 11)
│   └── org/muralis/datahose/
│       └── StreamingApp.java    # Main Flink job
│
├── buildspec.yml                # CodeBuild specification
├── Makefile                     # Developer commands
├── DEPLOYMENT.md                # Deployment guide (300+ lines)
├── ARCHITECTURE.md              # Architecture details
└── TERRAFORM_README.md          # Terraform implementation notes
```

## 🛠️ Make Commands

```bash
# Deployment
make deploy              # Full deployment (init + plan + apply)
make init                # Initialize Terraform
make plan                # Show execution plan
make apply               # Apply changes
make destroy             # Destroy all resources

# Pipeline
make pipeline-start      # Trigger pipeline manually
make pipeline-status     # Show pipeline status

# Flink Management
make flink-status        # Show Flink app status
make flink-start         # Start Flink application
make flink-stop          # Stop Flink application

# Monitoring
make logs-flink          # Tail Flink logs
make logs-lambda         # Tail Lambda logs
make logs-codebuild      # Tail CodeBuild logs

# Testing
make test-upload         # Upload sample CSV
make test-check-output   # List output files
make test-download-output # Download and view results

# Utilities
make list-buckets        # List S3 buckets
make outputs             # Show Terraform outputs
make costs               # Estimate monthly costs
make help                # Show all commands
```

## 📖 Additional Documentation

See [DEPLOYMENT.md](./DEPLOYMENT.md) for detailed step-by-step deployment instructions.

## 🔧 How It Works

### CI/CD Flow

1. **Source Stage**: GitHub webhook triggers CodePipeline on push
2. **Build Stage**: CodeBuild compiles Maven project (Java 11)
   - Runs `mvn clean package`
   - Uploads JAR to S3 with versioning
3. **Deploy Stage**: Lambda function invoked with artifact metadata
   - Checks if Flink app exists
   - Creates new or updates existing app
   - Configures environment properties
   - Starts the application

### Flink Application

- **Input**: Reads CSV files from `s3://input-bucket/datafall/`
- **Processing**: Aggregates visit counts by name using Flink Table API
- **Output**: Writes results to `s3://output-bucket/datalake/`

Example:
```csv
# Input (visits.csv)
name,visits
Alice,5
Bob,3
Alice,2

# Output (results)
Alice visited the gym 7 times
Bob visited the gym 3 times
```

### Lambda Lifecycle Management

Python function handles complete Flink lifecycle:

```python
# Actions supported
deploy    # Create or update + start
start     # Start application
stop      # Stop application
delete    # Delete application
describe  # Get application details
```

Includes:
- Status polling with timeouts
- Automatic retries
- Error handling
- CloudWatch logging
- CodePipeline integration

## � Security

### Best Practices

- **Secrets Management**: 
  - `terraform.tfvars` is in `.gitignore` - **never commit it**
  - GitHub tokens stored in AWS Secrets Manager or CodeStar Connections
  - No hardcoded credentials in code
  
- **IAM Roles**: 
  - Least-privilege policies for each service
  - Separate roles: Flink, Lambda, CodeBuild, CodePipeline
  - No wildcard (`*`) permissions in policies
  
- **S3 Security**:
  - All buckets have public access blocked by default
  - Versioning enabled for JAR and data buckets
  - At-rest encryption enabled
  
- **Network**:
  - Private subnets for Flink application (if VPC configured)
  - CloudWatch logs for audit trail
  
### What's Protected in .gitignore

```gitignore
terraform/terraform.tfvars      # Contains GitHub token ARN
terraform/*.tfstate             # Contains infrastructure state
terraform/.terraform/           # Terraform plugins and cache
.aws/                          # AWS credentials
lambda/*.zip                   # Built Lambda packages
```

### Checking for Exposed Secrets

```bash
# Verify terraform.tfvars is ignored
git check-ignore -v terraform/terraform.tfvars

# Check what's staged before committing
git status

# Scan for accidentally committed secrets (optional)
git secrets --scan
```

## �💰 Cost Estimation

**Monthly costs (us-east-2, light usage):**
- Flink (1 KPU, 24/7): ~$160.00
- S3 storage (10 GB): ~$0.23
- Lambda (100 invocations): ~$0.00
- CodeBuild (10 builds): ~$0.10
- CloudWatch Logs (1 GB): ~$0.50
- CodePipeline (1 pipeline): ~$1.00
- **Total: ~$162/month**

**Cost optimization:**
```bash
# Stop Flink when not in use
make flink-stop  # Saves ~$160/month
```

## 🔒 Security

- **Least-privilege IAM**: Separate roles for Flink, Lambda, CodeBuild, CodePipeline
- **S3 encryption**: At-rest encryption for all buckets
- **Private buckets**: Public access blocked by default
- **CloudWatch logging**: Audit trail for all operations
- **Versioned buckets**: JAR and data versioning enabled

## 🧪 Testing

```bash
# 1. Deploy infrastructure
make deploy

# 2. Trigger pipeline
make pipeline-start

# 3. Upload sample data
make test-upload

# 4. Monitor processing
make logs-flink

# 5. Verify output
make test-check-output
```

## 🚨 Troubleshooting

### Pipeline fails at Build
```bash
make logs-codebuild
# Common issues: Maven errors, Java version mismatch, S3 permissions
```

### Pipeline fails at Deploy
```bash
make logs-lambda
# Common issues: Flink app stuck, IAM permissions, JAR not found
```

### Flink app fails to start
```bash
make logs-flink
# Common issues: RuntimeExecutionMode (use STREAMING), S3 permissions
```

See [DEPLOYMENT.md](./DEPLOYMENT.md) for detailed troubleshooting.

## 🔄 Modernization Journey

This project has been **completely modernized** from shell script-based infrastructure to enterprise-grade Terraform + CI/CD:

**Previous Approach (Shell Scripts - Removed):**
- Manual deployment via `iac_create.sh` and `cicd.sh`
- Imperative infrastructure with no state management
- Manual monitoring and lifecycle management
- Difficult team collaboration

**Current Approach (Terraform + CI/CD):**
- **Automated**: Git push triggers entire pipeline
- **Declarative**: Terraform manages infrastructure state
- **Scalable**: CodePipeline orchestrates build and deployment
- **Observable**: Integrated CloudWatch logging
- **Collaborative**: Terraform state enables team workflows
- **Reliable**: Lambda-based lifecycle management with retries

## 🤝 Contributing

Contributions welcome! This is a reference implementation for:
- Terraform best practices
- AWS CI/CD automation
- Flink application deployment
- Lambda-based lifecycle management

## 📄 License

MIT License - see LICENSE file for details

## 🙏 Acknowledgments

- Built for real-time streaming analytics at scale
- Inspired by AWS best practices and 12-factor app methodology
- Designed for enterprise production workloads

## 📞 Support

- **Issues**: File an issue on GitHub
- **Logs**: Use `make logs-flink`, `make logs-lambda`, `make logs-codebuild`
- **Status**: Use `make flink-status`, `make pipeline-status`
- **Documentation**: See DEPLOYMENT.md, ARCHITECTURE.md

---

**Quick Links:**
- [Deployment Guide](./DEPLOYMENT.md) - Detailed step-by-step setup
- [Quick Start](#-quick-start) - Get started in 3 steps
- [Makefile Commands](#-make-commands) - All available commands
- [Troubleshooting](#-troubleshooting) - Common issues and fixes
