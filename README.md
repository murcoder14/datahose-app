
# AWS Kinesis Streaming Solution (Flink, Kinesis, S3)

This project provides a complete, production-ready AWS solution for real-time streaming using Apache Flink 1.20 (AWS Managed Flink), Kinesis Data Streams, and S3. It includes all scripts, code, and documentation for end-to-end deployment, testing, and troubleshooting.

**Status:** ✅ Production-ready  
**Region:** us-east-2  
**Last Updated:** October 10, 2025

---

## Table of Contents

- [Quick Start](#quick-start)
- [Solution Architecture](#solution-architecture)
- [Components](#components)
- [How Kinesis ARN is Passed](#how-kinesis-arn-is-passed)
- [Scripts Reference](#scripts-reference)
- [Application Details](#application-details)
- [Data Structure](#data-structure)
- [Testing & Monitoring](#testing--monitoring)
- [Troubleshooting](#troubleshooting)
- [Clean Up](#clean-up)
- [Cost Estimation](#cost-estimation)
- [Project Structure](#project-structure)
- [Technologies Used](#technologies-used)
- [References](#references)

---


## Solution Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                     AWS Cloud (us-east-2)                                   │
├──────────────────────────────────────────────────────────────────────────────┤
│ User (test.sh) → Kinesis (lowercase) → Flink (UPPERCASE) → S3 (UPPERCASE)   │
│                                                                              │
│  ┌───────────────┐   ┌───────────────┐   ┌───────────────┐   ┌────────────┐  │
│  │ test.sh      │   │ Kinesis       │   │ Flink         │   │ S3         │  │
│  │ (producer)   │   │ tm-input-stream│  │ datahose-app  │   │ datafall/  │  │
│  └───────────────┘   └───────────────┘   └───────────────┘   └────────────┘  │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

---


## Components


### Flink Application (`datahose-app`)
- Apache Flink 1.20 (STREAMING mode)
- Java 11 (SDKMAN: 11.0.28-amzn)
- Reads from Kinesis Data Stream (`tm-input-stream`)
- Transforms all data to UPPERCASE
- Writes to S3 (`datafall/` table)
- Checkpointing: 60s
- Rolling Policy: 30s inactivity / 2min max

### S3 Buckets
- **Dynamic Naming:** Buckets are created with the current date and Unix epoch for uniqueness, e.g. `tm-streaming-app-bucket-20251010-1760143646` and `tm-data-bucket-20251010-1760143646`.
- Application Bucket: `tm-streaming-app-bucket-<date>-<epoch>` (stores JAR)
- Data Bucket: `tm-data-bucket-<date>-<epoch>` (stores output)
  - Buckets are auto-detected by scripts; no need to manually update names after each deployment.

### Kinesis Data Stream
- Name: `tm-input-stream` (1 shard)
- Receives lowercase words from `test.sh`

### IAM Resources
- Role: `datahose-app-flink-role` (for Flink)
- Policy: `datahose-app-flink-policy` (S3, Kinesis, CloudWatch)
- User Policy: `datahose-app-kinesis-producer-policy` (for test.sh user)

### CloudWatch
- Log Group: `/aws/kinesis-analytics/datahose-app`
- Retention: 7 days

---

## How Kinesis ARN is Passed

The Kinesis Data Stream ARN is passed from the infrastructure to the Flink application using **AWS Managed Flink's Application Properties** feature. This is the recommended and secure way to pass runtime configuration to Flink applications.

**Process:**
1. `iac_create.sh` creates the Kinesis stream and saves the ARN to `/tmp/flink-config.env`.
2. You run `source /tmp/flink-config.env` to load the ARN as an environment variable.
3. `cicd.sh` passes the ARN to AWS via Application Properties (PropertyGroups).
4. Flink app reads the ARN at runtime using `KinesisAnalyticsRuntime.getApplicationProperties()`.

**Key Java code:**
```java
Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
Properties kinesisProps = applicationProperties.getOrDefault("KinesisSource", new Properties());
String streamArn = kinesisProps.getProperty("stream.arn");
```

**Benefits:**
- No ARN in source code or Git
- Change stream without rebuilding
- AWS best practice

---

---


## Prerequisites

1. **AWS CLI** (v2+)
2. **Maven** (3.x+)
3. **Java 11** (via SDKMAN recommended)
4. **jq** (for JSON parsing)
5. **AWS credentials** with permissions for IAM, S3, Kinesis, CloudWatch
6. **Region:** us-east-2

---


## 🚀 Quick Start

### 1. Set Java Version (REQUIRED in every new terminal)
```bash
sdk use java 11.0.28-amzn
```

### 2. Create Infrastructure (Dynamic Buckets)
```bash
./iac_create.sh
# No need to manually update bucket names; scripts will auto-detect the latest.
```

### 3. Build & Deploy Application
```bash
./cicd.sh
```

### 4. Verify Deployment (Self-Contained)
```bash
./verify.sh
# No need to source config; always checks the latest buckets.
```

### 5. Send Test Data
```bash
./test.sh
# Sends 5 random lowercase words every 5 seconds
# Press Ctrl+C to stop
```

### 6. Monitor Logs & Output
```bash
aws logs tail /aws/kinesis-analytics/datahose-app --follow
aws s3 ls s3://<latest-tm-data-bucket-*>/datafall/ --recursive
aws s3 cp s3://<latest-tm-data-bucket-*>/datafall/2025-10-10--XX/part-0-0 - | head -20
# Output should be UPPERCASE
```

---

## Scripts Reference

### `setup-env.sh` - Environment Setup

**Purpose:** Initialize development environment

**What it does:**
- Loads SDKMAN and sets Java 11
- Loads Flink configuration from `/tmp/flink-config.env`
- Configures AWS region from CLI profile
- Displays available commands

**Usage:**
```bash
./setup-env.sh
```

**Output:**
- Environment variables loaded
- Java version confirmed
- AWS region confirmed

---

### `iac_create.sh` - Infrastructure Creation

**Purpose:** Create all AWS infrastructure

**What it does:**
1. Creates S3 bucket for application JAR with versioning
2. Creates S3 bucket for data sink with versioning
3. Creates CloudWatch log group with 7-day retention
4. Creates IAM role with trust policy for Kinesis Analytics
5. Creates IAM policy with S3, CloudWatch, and VPC permissions
6. Attaches policy to role
7. Saves configuration to `/tmp/flink-config.env`

**Usage:**
```bash
./iac_create.sh
```

**Configuration saved:**
```bash
export APP_NAME="datahose-app"
export STREAMING_APP_BUCKET="tm-streaming-app-bucket-20251010"
export DATA_BUCKET="tm-data-bucket-20251010"
export REGION="us-east-2"
export FLINK_ROLE_ARN="arn:aws:iam::ACCOUNT_ID:role/datahose-app-flink-role"
```

**Resources Created:**
- S3 buckets (versioned)
- IAM role and policy
- CloudWatch log group
- Configuration file

---

### `cicd.sh` - Build and Deploy

**Purpose:** Build application and deploy to Managed Flink

**What it does:**
1. Initializes Java 11 via SDKMAN
2. Builds Maven project (`mvn clean package`)
3. Uploads JAR to S3 with versioning
4. Creates Flink application (if first deployment)
5. Updates Flink application (if already exists)
6. Stops application if running before update
7. Starts application in STREAMING mode
8. Monitors deployment status
9. Displays initial logs

**Usage:**
```bash
# Load configuration first
source /tmp/flink-config.env

# Run CI/CD
./cicd.sh
```

**Build Output:**
- JAR file: `target/datahose-app.jar` (31 MB)
- Uploaded to: `s3://tm-streaming-app-bucket-20251010/datahose-app.jar`

**Application Versions:**
- Each deployment increments version number
- Previous versions retained in S3 (versioning enabled)

---

### `verify.sh` - Health Check (Self-Contained)

**Purpose:** Verify all resources and application health

**Key Features:**
- **Self-contained:** No need to run `source /tmp/flink-config.env` or set environment variables.
- **Auto-detects** the latest dynamic S3 bucket names for both application and data buckets by creation date and prefix.
- Checks AWS credentials, S3 buckets, IAM role, CloudWatch log group, Flink application status, and recent logs.

**Usage:**
```bash
./verify.sh
```

**How it works:**
- Finds the most recently created `tm-streaming-app-bucket-*` and `tm-data-bucket-*` buckets automatically.
- Lists JAR files and recent data files.
- No manual configuration needed after each deployment.

**Example Output:**
```
╔════════════════════════════════════════════════════════╗
║     Flink Application Verification Report             ║
╚════════════════════════════════════════════════════════╝

=== AWS Credentials ===
[✓] Account ID: 047472788728
[✓] User/Role: arn:aws:iam::047472788728:user/username

=== S3 Buckets ===
[✓] Application bucket exists: tm-streaming-app-bucket-20251010-1760143646
[✓] JAR files in bucket: 1
[✓] Data bucket exists: tm-data-bucket-20251010-1760143646
[✓] Table folder exists: datafall
[✓] Files in table: 4

  Recent files:
  2025-10-10 20:47:40 tm-data-bucket-20251010-1760143646
  ...

=== Kinesis Data Stream ===
[✓] Kinesis stream exists: tm-input-stream
[✓] Stream status: ACTIVE ✓
[✓] Shard count: 1
[✓] Stream ARN: arn:aws:kinesis:us-east-2:047472788728:stream/tm-input-stream

=== IAM Resources ===
[✓] IAM Role exists: datahose-app-flink-role
[✓] Role ARN: arn:aws:iam::047472788728:role/datahose-app-flink-role
[✓] Attached policies: 1
[✓] User policy exists: datahose-app-kinesis-producer-policy
[✓] Policy attached to user sunny0524 ✓

=== CloudWatch Logs ===
[✓] Log group exists: /aws/kinesis-analytics/datahose-app
[✓] Retention period: 7 days
[✓] Log streams: 1

=== Flink Application ===
[✓] Application exists: datahose-app
[✓] Status: RUNNING ✓
[✓] Version: 1
[✓] Runtime: FLINK-1_20
[✓] Created: 2025-10-10T20:48:53-04:00
[✓] Last Updated: 2025-10-10T20:50:42-04:00

[✓] Recent log entries (last 5 minutes):
...

=== Summary ===
Health Score: 8/8 checks passed
[✓] All systems operational! ✓
```

---

### `iac_destroy.sh` - Infrastructure Cleanup

**Purpose:** Destroy all AWS resources (with confirmation)

**What it does:**
1. Stops Flink application if running
2. Deletes Flink application
3. Deletes all S3 objects (including versions)
4. Deletes S3 buckets
5. Detaches and deletes IAM policy
6. Deletes IAM role
7. Deletes CloudWatch log group
8. Removes configuration file

**Usage:**
```bash
# Interactive mode (with confirmation prompt)
./iac_destroy.sh

# Force mode (skip confirmation)
./iac_destroy.sh --force
```

**Safety Features:**
- Requires explicit "yes" confirmation
- Shows list of resources before deletion
- Handles versioned S3 objects properly
- Gracefully handles missing resources

**Warning:** This is destructive and cannot be undone!

---


## Application Details

**File:** `src/main/java/org/muralis/datahose/StreamingApp.java`

**Key Features:**
- Reads from Kinesis Data Stream (`tm-input-stream`)
- Transforms all data to UPPERCASE using MapFunction
- Writes to S3 (`datafall/` table)
- Checkpointing: 60s
- Rolling Policy: 30s inactivity / 2min max

**Transformation Example:**
```java
DataStream<String> transformedStream = inputStream.map(
  value -> value.toUpperCase()
);
```

---

---

datafall/

## Data Structure

### S3 Table: `datafall`

**Location:** `s3://tm-data-bucket-<date>-<epoch>/datafall/` (auto-detected by scripts)

**Schema:**
| Column | Type    | Description                        |
|--------|---------|------------------------------------|
| foams  | STRING  | Streaming data (UPPERCASE)         |

**Directory Structure:**
```
datafall/
├── 2025-10-10--17/
│   ├── part-0-0                    # Finalized file
│   ├── part-0-1                    # Finalized file
│   └── .part-0-2.inprogress.xyz    # In-progress
├── ...
```

**Querying Data:**
```bash
# List files
aws s3 ls s3://<latest-tm-data-bucket-*>/datafall/ --recursive
# View sample
aws s3 cp s3://<latest-tm-data-bucket-*>/datafall/2025-10-10--17/part-0-0 - | head -10
# Should be UPPERCASE
```

---

---


## Testing & Monitoring

### Send Test Data
```bash
./test.sh
# Sends 5 random lowercase words every 5 seconds
# Press Ctrl+C to stop
```

### Monitor Logs
```bash
aws logs tail /aws/kinesis-analytics/datahose-app --follow
```

### Check S3 Output
```bash
aws s3 ls s3://<latest-tm-data-bucket-*>/datafall/ --recursive
aws s3 cp s3://<latest-tm-data-bucket-*>/datafall/2025-10-10--XX/part-0-0 - | head -20
# Output should be UPPERCASE
```

---


## Troubleshooting

### Application won't start
```bash
aws logs tail /aws/kinesis-analytics/datahose-app --since 10m | grep -i error
aws kinesisanalyticsv2 describe-application --application-name datahose-app
```
**Common Causes:**
- S3 path or bucket name incorrect
- IAM permissions missing
- JAR file corrupt

### No data in S3
```bash
aws s3 ls s3://<latest-tm-data-bucket-*>/
aws logs tail /aws/kinesis-analytics/datahose-app --since 30m | grep -i "s3\|error"
# Wait at least 2 minutes for first file
```

### test.sh fails
```bash
aws kinesis describe-stream --stream-name tm-input-stream
aws iam list-attached-user-policies --user-name <your-user>
```

### Data not uppercase
- Check application logs for transformation errors
- Rebuild and redeploy: `./cicd.sh`

### Region or Java version issues
- Always set region to us-east-2
- Always run `sdk use java 11.0.28-amzn` in every new terminal

---

### Application Won't Start

**Symptom:** Status stuck in `STARTING` or transitions to `RESTARTING`

**Diagnosis:**
```bash
# Check recent logs
aws logs tail /aws/kinesis-analytics/datahose-app --since 10m | grep -i error

# Check application details
aws kinesisanalyticsv2 describe-application \
  --application-name datahose-app
```

**Common Causes:**
1. **S3 Path Issue:** Bucket name or path incorrect in source code
   - Fix: Update `StreamingApp.java` with correct bucket name
   - Rebuild and redeploy

2. **IAM Permissions:** Role doesn't have S3 write access
   - Check policy: `aws iam get-role-policy --role-name datahose-app-flink-role --policy-name datahose-app-flink-policy`
   - Verify S3 permissions are present

3. **JAR File Corrupt:** Upload failed or build issue
   - Rebuild: `mvn clean package`
   - Re-upload: `./cicd.sh`

### No Data in S3

**Symptom:** Application running but no files in S3

**Diagnosis:**
```bash
# Check if data bucket exists
aws s3 ls s3://<latest-tm-data-bucket-*>/

# Check application logs for errors
aws logs tail /aws/kinesis-analytics/datahose-app --since 30m | grep -i "s3\|error"

# Verify rolling policy timing
# Files appear after 30s inactivity OR 2min max interval
```

**Possible Causes:**
1. **Timing:** Wait at least 2 minutes after start for first file
2. **Path Issue:** Check application logs for S3 write errors
3. **Permissions:** Verify IAM role has PutObject permission

### Region Mismatch

**Symptom:** Resources not found or "Access Denied" errors

**Diagnosis:**
```bash
# Check configured region
aws configure get region

# Should output: us-east-2
```

**Fix:**
```bash
# Set correct region
aws configure set region us-east-2

# Re-run scripts
./iac_create.sh
./cicd.sh
```

### Java Version Issues

**Symptom:** Build fails with Java version errors or "wrong version" messages

**Diagnosis:**
```bash
java -version
# Should show: openjdk version "11.x.x"

# Check which Java is being used
which java
```

**Fix:**
```bash
# RECOMMENDED: Use SDKMAN to set Java 11 (required in EVERY new shell session)
sdk use java 11.0.28-amzn

# Verify it's set correctly
java -version

# Alternative: Set JAVA_HOME manually
export JAVA_HOME=$(dirname $(dirname $(which java)))

# OR: Run the setup script which does this automatically
./setup-env.sh
```

**Prevention:**
- **Always run `sdk use java 11.0.28-amzn` when opening a new terminal**
- Or add this to your `~/.bashrc` or `~/.zshrc`:
  ```bash
  # Auto-initialize SDKMAN and Java 11
  export SDKMAN_DIR="$HOME/.sdkman"
  [[ -s "$SDKMAN_DIR/bin/sdkman-init.sh" ]] && source "$SDKMAN_DIR/bin/sdkman-init.sh"
  sdk use java 11.0.28-amzn 2>/dev/null
  ```

### Maven Build Fails

**Symptom:** `mvn clean package` fails

**Diagnosis:**
```bash
# Check Maven version
mvn --version

# Check pom.xml exists
ls -l pom.xml
```

**Fix:**
```bash
# Clean Maven cache
mvn clean

# Rebuild
mvn package -DskipTests

# If dependencies fail, update Maven
sdk install maven 3.9.11
```

### Script Permission Denied

**Symptom:** `bash: ./script.sh: Permission denied`

**Fix:**
```bash
# Make scripts executable
chmod +x *.sh

# Or run with bash
bash iac_create.sh
```

---


## Clean Up

```bash
# Interactive mode with confirmation
./iac_destroy.sh
# Or force without confirmation
./iac_destroy.sh --force
```

This removes:
- Flink application
- S3 buckets (all objects including versions)
- IAM role and policy
- CloudWatch log group

---

---


## Cost Estimation

| Service              | Usage         | Estimated Cost |
|----------------------|--------------|----------------|
| Managed Flink        | 1 KPU, 24/7  | ~$45           |
| Kinesis Data Stream  | 1 shard      | ~$15           |
| S3 Storage           | ~100 GB      | ~$2.30         |
| CloudWatch           | Logs/metrics | ~$2            |
| Data Transfer        | Minimal      | ~$0.50         |
| **Total**            |              | **~$65/month** |

---

---

datahose-app/

## Project Structure

```
datahose-app/
├── src/main/java/org/muralis/datahose/StreamingApp.java
├── src/test/java/org/muralis/datahose/
├── target/datahose-app.jar
├── pom.xml
├── iac_create.sh
├── iac_destroy.sh
├── cicd.sh
├── verify.sh
├── setup-env.sh
├── test.sh
└── README.md
```

---

---


## Technologies Used

- Apache Flink 1.20
- Java 11 (SDKMAN: 11.0.28-amzn)
- Maven 3.x
- AWS Managed Service for Apache Flink
- Amazon S3
- AWS IAM
- Amazon CloudWatch
- AWS CLI
- Bash

---

---


## References

- [Apache Flink Documentation](https://nightlies.apache.org/flink/flink-docs-release-1.20/)
- [AWS Managed Service for Apache Flink](https://docs.aws.amazon.com/kinesisanalytics/)
- [Flink Kinesis Connector](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/datastream/kinesis/)
- [AWS Kinesis Data Streams](https://docs.aws.amazon.com/streams/latest/dev/fundamental-stream.html)

---

**License:** For educational and demonstration purposes.

**Last Updated:** October 10, 2025  
**Version:** 1.0  
**Region:** us-east-2
