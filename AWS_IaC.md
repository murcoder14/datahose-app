# AWS Infrastructure as Code (IaC) Guide

This document describes the AWS infrastructure setup, configuration, and management for the Flink streaming application.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Infrastructure Components](#infrastructure-components)
- [Scripts Reference](#scripts-reference)
- [Configuration Management](#configuration-management)
- [Resource Naming](#resource-naming)
- [IAM Permissions](#iam-permissions)
- [Troubleshooting](#troubleshooting)
- [Cost Estimation](#cost-estimation)

---

## Prerequisites

1. **AWS CLI** (v2+) - [Installation Guide](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
2. **jq** (JSON processor) - `sudo apt install jq` or `brew install jq`
3. **AWS Credentials** configured with appropriate permissions
4. **AWS Region** set in your AWS CLI profile

### Configure AWS Region

```bash
# Set your preferred region
aws configure set region us-east-2

# Verify configuration
aws configure get region
```

---

## Infrastructure Components

### S3 Buckets

**Dynamic Naming Convention:** `tm-<purpose>-<YYYYMMDD>-<epoch>`

1. **Application Bucket** - Stores Flink JAR files
   - Pattern: `tm-streaming-app-bucket-20251122-1763855370`
   - Versioning: Enabled
   - Purpose: Flink application code storage

2. **Default Output Bucket** - Unknown/unrouted messages
   - Pattern: `tm-output-20251122-1763855370`
   - Versioning: Enabled
   - Purpose: Error handling and debugging

3. **Iceberg Warehouse Bucket** - Main data lake storage
   - Pattern: `tm-iceberg-warehouse-20251122-1763855370`
   - Versioning: Enabled
   - Purpose: Stores all Iceberg table data (Avro format) and metadata
   - Contains: `claims/` and `leave_requests/` tables

### Kinesis Data Stream

- **Name:** `datahose-app-stream`
- **Shard Count:** 1 (adjustable for scale)
- **Retention:** 24 hours (default)
- **Purpose:** Real-time message ingestion

### AWS Glue Data Catalog

- **Database:** `tm_data_lake`
- **Tables:** Auto-created by Flink application
  - `claims` - Partitioned by year/month/day/hour
  - `leave_requests` - Partitioned by year/month/day/hour
- **Purpose:** Iceberg table metadata management

### IAM Resources

**Role:** `datahose-app-flink-role`
- **Trust Policy:** Allows `kinesisanalytics.amazonaws.com` to assume role
- **Attached Policy:** `datahose-app-flink-policy`

**Policy Permissions:**
- S3: Read/Write to all application buckets
- Kinesis: Read from data stream
- CloudWatch: Write logs and metrics
- Glue: Manage catalog and tables (CreateTable, UpdateTable, GetTable, etc.)
- VPC: Network interface management (if VPC deployment)

**User Policy:** `datahose-app-s3-upload-policy`
- **Attached To:** User `sunny0524` (configurable)
- **Purpose:** Allow manual file uploads to input bucket

### CloudWatch Resources

- **Log Group:** `/aws/kinesis-analytics/datahose-app`
- **Retention:** 7 days
- **Log Stream:** `flink-application` (auto-created)
- **Purpose:** Application logs and debugging

---

## Scripts Reference

### `iac_create.sh` - Infrastructure Creation

**Purpose:** Creates all AWS infrastructure resources with dynamic naming

**Usage:**
```bash
./iac_create.sh
```

**What It Creates:**

1. **S3 Buckets** (with versioning enabled)
   - Streaming application bucket
   - Default output bucket (unknown messages)
   - Iceberg warehouse bucket

2. **Kinesis Data Stream**
   - Stream name: `datahose-app-stream`
   - Shard count: 1

3. **AWS Glue Database**
   - Database: `tm_data_lake`
   - Location: Points to Iceberg warehouse bucket

4. **IAM Resources**
   - Role with trust policy
   - Policy with S3, Kinesis, CloudWatch, Glue, and VPC permissions
   - User policy for file uploads

5. **CloudWatch Resources**
   - Log group with 7-day retention
   - Log stream (auto-created on first write)

**Configuration Output:**

The script saves configuration to `/tmp/flink-config.env`:

```bash
export FLINK_ROLE_ARN="arn:aws:iam::ACCOUNT_ID:role/datahose-app-flink-role"
export STREAMING_APP_BUCKET="tm-streaming-app-bucket-20251122-1763855370"
export KINESIS_STREAM_NAME="datahose-app-stream"
export DEFAULT_OUTPUT_BUCKET="tm-output-20251122-1763855370"
export ICEBERG_WAREHOUSE_BUCKET="tm-iceberg-warehouse-20251122-1763855370"
export GLUE_DATABASE_NAME="tm_data_lake"
export LOG_GROUP="/aws/kinesis-analytics/datahose-app"
export REGION="us-east-2"
export BUCKET_SUFFIX="20251122-1763855370"
```

**Idempotency:**
- Script checks for existing resources before creation
- Warns if resources already exist
- Safe to run multiple times

---

### `iac_destroy.sh` - Infrastructure Cleanup

**Purpose:** Destroys all AWS resources created by `iac_create.sh`

**Usage:**
```bash
# Interactive mode (with confirmation)
./iac_destroy.sh

# Force mode (skip confirmation)
./iac_destroy.sh --force
```

**What It Destroys:**

1. Stops and deletes Flink application
2. Deletes all S3 buckets (including all versions and delete markers)
3. Deletes Kinesis data stream (with consumer enforcement)
4. Deletes Glue database and all tables
5. Deletes Athena results bucket
6. Detaches and deletes IAM policies
7. Deletes IAM role
8. Deletes CloudWatch log group
9. Removes configuration file

**Safety Features:**
- Requires explicit "yes" confirmation (unless `--force` used)
- Shows list of resources before deletion
- Handles versioned S3 objects properly
- Gracefully handles missing resources

**⚠️ Warning:** This operation is destructive and cannot be undone!

---

### `verify.sh` - Health Check

**Purpose:** Verifies all resources and application health

**Features:**
- Self-contained (no configuration file needed)
- Auto-detects latest dynamic bucket names
- Checks AWS credentials, S3 buckets, IAM role, CloudWatch logs
- Verifies Flink application status
- Displays recent log entries

**Usage:**
```bash
./verify.sh
```

**Example Output:**
```
╔════════════════════════════════════════════════════════╗
║     Flink Application Verification Report             ║
╚════════════════════════════════════════════════════════╝

=== AWS Credentials ===
[✓] Account ID: 047472788728
[✓] User/Role: arn:aws:iam::047472788728:user/username

=== S3 Buckets ===
[✓] Application bucket exists: tm-streaming-app-bucket-20251122-1763855370
[✓] JAR files in bucket: 1
[✓] Iceberg warehouse exists: tm-iceberg-warehouse-20251122-1763855370

=== Glue Catalog ===
[✓] Database exists: tm_data_lake
[✓] Tables: claims, leave_requests

=== IAM Resources ===
[✓] IAM Role exists: datahose-app-flink-role
[✓] Attached policies: 1

=== CloudWatch Logs ===
[✓] Log group exists: /aws/kinesis-analytics/datahose-app
[✓] Recent log entries found

=== Flink Application ===
[✓] Application exists: datahose-app
[✓] Status: RUNNING
[✓] Version: 2
[✓] Runtime: FLINK-1_20

=== Summary ===
Health Score: 7/7 checks passed
[✓] All systems operational!
```

---

## Configuration Management

### Environment Variables

Load configuration after running `iac_create.sh`:

```bash
source /tmp/flink-config.env
```

### Configuration File Location

- **Path:** `/tmp/flink-config.env`
- **Created By:** `iac_create.sh`
- **Used By:** `cicd.sh`, `iac_destroy.sh`
- **Auto-detected By:** `verify.sh` (doesn't need sourcing)

### Updating Configuration

If you need to manually update configuration:

```bash
# Edit the file
vi /tmp/flink-config.env

# Reload configuration
source /tmp/flink-config.env
```

---

## Resource Naming

### Bucket Naming Convention

**Format:** `tm-<purpose>-<date>-<epoch>`

- **`tm`:** Prefix (customize as needed)
- **`<purpose>`:** Describes bucket purpose (e.g., `streaming-app-bucket`, `iceberg-warehouse`)
- **`<date>`:** YYYYMMDD format (e.g., `20251122`)
- **`<epoch>`:** Unix timestamp (e.g., `1763855370`)

**Benefits:**
- Globally unique S3 bucket names
- Chronological sorting
- Easy identification of deployment date
- No manual name updates needed

### Auto-Detection

Scripts automatically find the latest bucket by:
1. Filtering by prefix (e.g., `tm-streaming-app-bucket-*`)
2. Sorting by creation date
3. Selecting most recent

```bash
# Example auto-detection
LATEST_BUCKET=$(aws s3api list-buckets \
  --query 'Buckets[?starts_with(Name, `tm-streaming-app-bucket-`)] | sort_by(@, &CreationDate)[-1].Name' \
  --output text)
```

---

## IAM Permissions

### Minimum Required Permissions

To run the IaC scripts, your AWS user/role needs:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:CreateBucket",
        "s3:DeleteBucket",
        "s3:ListBucket",
        "s3:PutBucketVersioning",
        "s3:GetBucketLocation",
        "s3:PutObject",
        "s3:DeleteObject",
        "kinesis:CreateStream",
        "kinesis:DeleteStream",
        "kinesis:DescribeStream",
        "glue:CreateDatabase",
        "glue:DeleteDatabase",
        "glue:GetDatabase",
        "glue:CreateTable",
        "glue:DeleteTable",
        "iam:CreateRole",
        "iam:DeleteRole",
        "iam:AttachRolePolicy",
        "iam:DetachRolePolicy",
        "iam:CreatePolicy",
        "iam:DeletePolicy",
        "iam:PutRolePolicy",
        "logs:CreateLogGroup",
        "logs:DeleteLogGroup",
        "logs:PutRetentionPolicy",
        "kinesisanalytics:CreateApplication",
        "kinesisanalytics:DeleteApplication",
        "kinesisanalytics:DescribeApplication"
      ],
      "Resource": "*"
    }
  ]
}
```

### Flink Application Role Permissions

The `datahose-app-flink-role` has these permissions:

- **S3:** Read/Write to all application buckets and Iceberg warehouse
- **Kinesis:** Read from `datahose-app-stream`
- **CloudWatch:** Write logs and metrics
- **Glue:** Full catalog and table management
- **VPC:** Network interface management

---

## Troubleshooting

### Region Mismatch

**Symptom:** Resources not found or "Access Denied" errors

**Fix:**
```bash
# Check current region
aws configure get region

# Set correct region
aws configure set region us-east-2

# Verify
aws sts get-caller-identity
```

### Bucket Already Exists (Global)

**Symptom:** `BucketAlreadyExists` error during creation

**Cause:** Bucket names must be globally unique across all AWS accounts

**Fix:**
- Use the dynamic naming with epoch suffix (automatic)
- Script generates unique names each run

### IAM Permission Errors

**Symptom:** `AccessDenied` or `UnauthorizedException`

**Fix:**
```bash
# Check current user/role
aws sts get-caller-identity

# Verify IAM permissions
aws iam list-attached-user-policies --user-name YOUR_USERNAME
```

### Configuration File Missing

**Symptom:** `cicd.sh` or `iac_destroy.sh` complains about missing variables

**Fix:**
```bash
# Re-run infrastructure creation
./iac_create.sh

# Source configuration
source /tmp/flink-config.env

# Or manually set variables
export FLINK_ROLE_ARN="arn:aws:iam::ACCOUNT:role/datahose-app-flink-role"
export STREAMING_APP_BUCKET="tm-streaming-app-bucket-20251122-1763855370"
# ... etc
```

### Glue Database/Table Conflicts

**Symptom:** `AlreadyExistsException` when creating tables

**Fix:**
```bash
# Delete existing database (WARNING: destroys all tables)
aws glue delete-database --name tm_data_lake

# Or manually delete specific table
aws glue delete-table --database-name tm_data_lake --name claims
```

---

## Cost Estimation

### Monthly Cost Breakdown

| Service | Configuration | Estimated Cost |
|---------|--------------|----------------|
| **Managed Flink** | 1 KPU, 24/7 | ~$45/month |
| **Kinesis Data Stream** | 1 shard | ~$15/month |
| **S3 Storage** | ~100 GB | ~$2.30/month |
| **S3 Requests** | ~1M requests | ~$0.50/month |
| **CloudWatch Logs** | ~10 GB | ~$0.50/month |
| **CloudWatch Metrics** | Custom metrics | ~$0.30/month |
| **Glue Data Catalog** | 2 tables | ~$2/month |
| **Data Transfer** | Minimal | ~$0.50/month |
| **Total** | | **~$66/month** |

### Cost Optimization Tips

1. **Stop Flink Application** when not in use (saves ~$45/month)
   ```bash
   aws kinesisanalyticsv2 stop-application \
     --application-name datahose-app \
     --region us-east-2
   ```

2. **Reduce Kinesis Retention** from 24h to 1h (default is free)

3. **Enable S3 Lifecycle Policies** to transition old data to cheaper storage classes

4. **Use S3 Intelligent-Tiering** for automatic cost optimization

5. **Set CloudWatch Log Retention** to 1-3 days for development

### Free Tier Considerations

- **S3:** 5 GB free storage, 20,000 GET, 2,000 PUT requests/month (12 months)
- **CloudWatch:** 5 GB logs, 10 metrics, 1M API requests/month (always free)
- **Glue:** 1M requests/month (always free)

---

## Best Practices

### 1. Use Separate Environments

Create separate stacks for dev/staging/production:

```bash
# Modify APP_NAME in iac_create.sh
APP_NAME="datahose-app-dev"    # Development
APP_NAME="datahose-app-staging" # Staging
APP_NAME="datahose-app-prod"    # Production
```

### 2. Tag Resources

Add tags for cost allocation and resource management:

```bash
aws s3api put-bucket-tagging \
  --bucket tm-streaming-app-bucket-20251122-1763855370 \
  --tagging 'TagSet=[{Key=Environment,Value=dev},{Key=Project,Value=datahose}]'
```

### 3. Enable Versioning

Already enabled by default for:
- All S3 buckets (keeps historical versions)
- IAM policies (allows rollback)

### 4. Monitor Costs

```bash
# Check current month costs
aws ce get-cost-and-usage \
  --time-period Start=$(date +%Y-%m-01),End=$(date +%Y-%m-%d) \
  --granularity MONTHLY \
  --metrics UnblendedCost \
  --group-by Type=SERVICE
```

### 5. Regular Backups

Iceberg tables support snapshots and time travel:

```sql
-- Query data as of specific snapshot
SELECT * FROM tm_data_lake.claims VERSION AS OF 1234567890123;

-- List all snapshots
SELECT * FROM tm_data_lake.claims.snapshots;
```

**Note:** This project uses Avro format for Iceberg data files (not Parquet). See README.md for detailed warehouse structure.

---

## References

- [AWS Managed Service for Apache Flink Documentation](https://docs.aws.amazon.com/managed-flink/)
- [AWS Glue Data Catalog](https://docs.aws.amazon.com/glue/latest/dg/catalog-and-crawler.html)
- [Amazon S3 Best Practices](https://docs.aws.amazon.com/AmazonS3/latest/userguide/best-practices.html)
- [AWS IAM Best Practices](https://docs.aws.amazon.com/IAM/latest/UserGuide/best-practices.html)
- [Amazon Kinesis Data Streams](https://docs.aws.amazon.com/streams/latest/dev/introduction.html)

---

**Last Updated:** November 22, 2025  
**Version:** 1.0
