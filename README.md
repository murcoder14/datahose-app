# DataHose App - Flink Streaming Platform

[![Terraform](https://img.shields.io/badge/Terraform-1.0+-623CE4?logo=terraform)](https://www.terraform.io/)
[![AWS](https://img.shields.io/badge/AWS-Managed_Flink-FF9900?logo=amazon-aws)](https://aws.amazon.com/managed-service-apache-flink/)
[![Python](https://img.shields.io/badge/Python-3.13-3776AB?logo=python)](https://www.python.org/)
[![Java](https://img.shields.io/badge/Java-11-007396?logo=java)](https://www.java.com/)

A real-time streaming analytics platform built with Apache Flink on AWS, featuring enterprise-grade infrastructure as code (Terraform) and a fully automated CI/CD pipeline.

## 🚀 Overview

This project provides a production-ready streaming application that processes data in real-time. The Flink application reads text data from an **AWS Kinesis Data Stream**, converts the text to **uppercase**, and writes the results to an **S3 bucket**.

### ✨ Key Features

- **Infrastructure as Code**: Modular Terraform for all AWS resources.
- **Automated CI/CD**: A CodePipeline workflow automates builds and deployments on every `git push`.
- **Serverless Lifecycle Management**: A Python Lambda function manages the Flink application's lifecycle (deploy, start, stop).
- **Developer Tooling**: A `Makefile` provides simple commands for deployment, monitoring, and testing.

### 📊 Architecture

**Data Flow:**
```
[Kinesis Data Stream]──> [AWS Flink Application] ──> [S3 Output Bucket]
```

**CI/CD and Deployment Flow:**
```
[GitHub Push] ──> [CodePipeline] ──> [CodeBuild] ──> [S3 (JAR)]
                                                         │
                                                         ▼
                                     [Lambda] ──> [Deploy to Flink]
```

## 🚀 Deployment Guide

Follow these steps to deploy the entire infrastructure and application.

### 1. Prerequisites

- **Install Tools**: Ensure you have [Terraform](https://www.terraform.io/downloads) (>= 1.0) and the [AWS CLI](https://aws.amazon.com/cli/) installed.
- **Configure AWS Credentials**: Run `aws configure` to set up your access key, secret key, and default region (e.g., `us-east-2`).
- **Set up GitHub Connection**:
    1.  In the AWS Console, navigate to **Developer Tools > CodePipeline > Settings > Connections**.
    2.  Click **Create connection**, select **GitHub**, and complete the authorization flow.
    3.  Copy the **Connection ARN** for the new connection.

### 2. Configure the Project

1.  Clone the repository:
    ```bash
    git clone https://github.com/murcoder14/datahose-app.git
    cd datahose-app
    ```
2.  Create a `terraform.tfvars` file from the example:
    ```bash
    cp terraform/terraform.tfvars.example terraform/terraform.tfvars
    ```
3.  Edit `terraform/terraform.tfvars` and provide the required values, especially your `github_token_secret_arn` (the Connection ARN from the previous step).

### 3. Deploy the Infrastructure

Run the following command to initialize Terraform, plan the changes, and apply them:

```bash
make deploy
```
When prompted, type `yes` to approve the deployment. This will create all the necessary AWS resources (IAM roles, S3 buckets, Kinesis stream, Lambda, and CodePipeline).

### 4. Run the CI/CD Pipeline

The infrastructure is now ready, but the Flink application itself has not been deployed yet. Trigger the pipeline to build the JAR and deploy it.

```bash
make pipeline-start
```
This command manually starts the pipeline. Alternatively, any `git push` to the configured branch will also trigger it. You can monitor the progress in the AWS CodePipeline console or by using `make pipeline-status`.

### 5. Test the Application

Once the pipeline has successfully completed the "Deploy" stage, the Flink application will be running.

1.  **Send test data** to the Kinesis stream:
    ```bash
    make test-upload
    ```
2.  **Check the output** in the S3 bucket:
    ```bash
    make test-check-output
    ```
You should see the uppercase version of your test data in the output files.

## 🛠️ Make Commands

A `Makefile` provides shortcuts for common operations.

```bash
# Deployment
make deploy              # Full deployment (init + plan + apply)
make destroy             # Destroy all resources

# Pipeline & Flink
make pipeline-start      # Trigger pipeline manually
make flink-status        # Show Flink app status
make flink-stop          # Stop Flink application

# Monitoring & Testing
make logs-flink          # Tail Flink logs
make test-upload         # Upload sample data to Kinesis
make test-check-output   # List output files in S3

# Show all commands
make help
```

## 🔐 Security

- **Secrets Management**: The `terraform.tfvars` file is ignored by Git. GitHub tokens are managed via AWS CodeStar Connections.
- **Least-Privilege IAM**: Each service has a dedicated IAM role with minimal required permissions.
- **S3 Security**: All S3 buckets have public access blocked, versioning enabled, and at-rest encryption enabled.

## 💰 Cost Estimation

**Monthly costs (us-east-2, light usage):**
- Flink (1 KPU, 24/7): ~$160.00
- S3, Lambda, CodeBuild, CloudWatch, CodePipeline: ~$2.00
- **Total: ~$162/month**

To save costs, stop the Flink application when not in use: `make flink-stop`.

## 🚨 Troubleshooting

- **Pipeline fails at Build stage?** Check the CodeBuild logs: `make logs-codebuild`.
- **Pipeline fails at Deploy stage?** Check the Lambda logs: `make logs-lambda`.
- **Flink app fails to start or runs with errors?** Check the Flink application logs: `make logs-flink`.

## 📄 License

MIT License. See the `LICENSE` file for details.
