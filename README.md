# AWS Flink Streaming Application

Complete AWS solution for deploying an Apache Flink 1.20 streaming application using Amazon Managed Service for Apache Flink. The application continuously generates streaming data at 2 records per second and writes it to an S3 table.

**Status:** ✅ Production-ready  
**Region:** us-east-2 (configured via AWS CLI default profile)  
**Last Updated:** October 10, 2025

---

## Table of Contents

- [Solution Architecture](#solution-architecture)
- [Components](#components)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Scripts Reference](#scripts-reference)
- [Application Details](#application-details)
- [Data Structure](#data-structure)
- [Monitoring & Operations](#monitoring--operations)
- [Troubleshooting](#troubleshooting)
- [Clean Up](#clean-up)

---

## Solution Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     AWS Cloud (us-east-2)                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Amazon Managed Service for Apache Flink                │   │
│  │  ┌─────────────────────────────────────────────┐        │   │
│  │  │  Flink Application (datahose-app)          │        │   │
│  │  │  - Apache Flink 1.20                       │        │   │
│  │  │  - Java 11 (SDKMAN: 11.0.28-amzn)         │        │   │
│  │  │  - DataStream API                          │        │   │
│  │  │  - Streaming Mode (Continuous)             │        │   │
│  │  │  - Data Generator: 2 records/sec           │        │   │
│  │  │  - Rolling Policy: 30s inactivity / 2min   │        │   │
│  │  │  - Checkpointing: 60s                      │        │   │
│  │  └─────────────────────────────────────────────┘        │   │
│  └─────────────────────────────────────────────────────────┘   │
│                            │                                     │
│                            │ writes data continuously            │
│                            ▼                                     │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  S3 Data Bucket: tm-data-bucket-20251010               │   │
│  │  └── datafall/ (S3 Table with 'foams' column)         │   │
│  │      └── 2025-10-10--17/                               │   │
│  │          ├── part-xxx-0 (finalized)                    │   │
│  │          ├── part-xxx-1 (finalized)                    │   │
│  │          └── .part-xxx.inprogress (in-progress)        │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  S3 Application Bucket: tm-streaming-app-bucket-20251010 │
│  │  └── datahose-app.jar (31 MB, versioned)              │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  IAM Role: datahose-app-flink-role                     │   │
│  │  └── Policy: datahose-app-flink-policy                 │   │
│  │      - S3 Read/Write                                   │   │
│  │      - CloudWatch Logs/Metrics                         │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  CloudWatch Logs: /aws/kinesis-analytics/datahose-app │   │
│  │  - Retention: 7 days                                   │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

---

## Components

### 1. Flink Application (`datahose-app`)
- **Runtime:** Apache Flink 1.20 (STREAMING mode)
- **Language:** Java 11 (managed via SDKMAN)
- **Source:** DataGeneratorSource (unbounded, 2 records/second)
- **Sink:** S3 FileSink with rolling policy
- **Checkpointing:** Every 60 seconds
- **Data Format:** Text records with timestamp
- **Parallelism:** 1 KPU (Kinesis Processing Unit)

### 2. S3 Buckets
- **Application Bucket:** `tm-streaming-app-bucket-20251010`
  - Stores versioned JAR file (31 MB)
  - Versioning enabled for rollback capability
  
- **Data Bucket:** `tm-data-bucket-20251010`
  - Stores streaming output in `datafall/` table
  - Versioning enabled
  - Rolling files based on inactivity (30s) or time (2min)

### 3. IAM Resources
- **Role:** `datahose-app-flink-role`
  - Service: `kinesisanalytics.amazonaws.com`
  - Trust relationship configured for Managed Flink
  
- **Policy:** `datahose-app-flink-policy`
  - S3: GetObject, PutObject, ListBucket
  - CloudWatch: CreateLogGroup, CreateLogStream, PutLogEvents, PutMetricData
  - EC2/VPC: DescribeVpcs, DescribeSubnets, etc. (for VPC access)

### 4. CloudWatch Resources
- **Log Group:** `/aws/kinesis-analytics/datahose-app`
- **Retention:** 7 days
- **Log Types:** Application logs, checkpoint logs, error logs

---

## Prerequisites

### Required Software

1. **AWS CLI** (v2 or later)
   ```bash
   aws --version
   # If not installed: https://aws.amazon.com/cli/
   ```

2. **Maven** (3.x or later)
   ```bash
   mvn --version
   # If not installed: https://maven.apache.org/install.html
   ```

3. **Java 11** (via SDKMAN recommended)
   ```bash
   # Install SDKMAN
   curl -s "https://get.sdkman.io" | bash
   source "$HOME/.sdkman/bin/sdkman-init.sh"
   
   # Install Java 11
   sdk install java 11.0.28-amzn
   sdk use java 11.0.28-amzn
   ```
   
   > **⚠️ CRITICAL:** You must run `sdk use java 11.0.28-amzn` in **every new shell/terminal session** before running Maven commands or building the application. The `./setup-env.sh` and `./cicd.sh` scripts do this automatically.

4. **jq** (for JSON parsing)
   ```bash
   # Fedora/RHEL
   sudo dnf install jq
   
   # Ubuntu/Debian
   sudo apt-get install jq
   
   # macOS
   brew install jq
   ```

### AWS Configuration

1. **Configure AWS Credentials**
   ```bash
   aws configure
   ```
   
   You'll need:
   - AWS Access Key ID
   - AWS Secret Access Key
   - Default region: **us-east-2**
   - Default output format: json

2. **Set Region** (critical!)
   ```bash
   aws configure set region us-east-2
   ```
   
   Verify:
   ```bash
   aws configure get region
   # Should output: us-east-2
   ```

3. **Required AWS Permissions**
   
   Your AWS user/role needs:
   - IAM: `CreateRole`, `DeleteRole`, `CreatePolicy`, `DeletePolicy`, `AttachRolePolicy`, `DetachRolePolicy`
   - S3: `CreateBucket`, `DeleteBucket`, `PutObject`, `GetObject`, `ListBucket`
   - Kinesis Analytics: `CreateApplication`, `DeleteApplication`, `StartApplication`, `StopApplication`
   - CloudWatch: `CreateLogGroup`, `DeleteLogGroup`, `PutRetentionPolicy`

---

## Quick Start

> **⚠️ IMPORTANT:** Every time you open a new shell/terminal, you must set Java 11:
> ```bash
> sdk use java 11.0.28-amzn
> ```
> Or run `./setup-env.sh` to initialize the environment automatically.

### 1. Clone and Setup Environment

```bash
cd datahose-app
./setup-env.sh
```

This script:
- Initializes SDKMAN and Java 11 (run this in every new shell session!)
- Loads Flink configuration
- Sets up AWS region from your CLI profile
- Displays available commands

**Note:** The `cicd.sh` script automatically initializes Java 11, but for manual Maven commands, always ensure Java 11 is active first.

### 2. Create Infrastructure

```bash
./iac_create.sh
```

This creates:
- S3 buckets (application + data)
- IAM role and policy
- CloudWatch log group
- Configuration file at `/tmp/flink-config.env`

**Expected output:**
```
[INFO] Infrastructure Creation Complete!
[INFO] Resources Created:
  ✓ S3 Bucket (Application): tm-streaming-app-bucket-20251010
  ✓ S3 Bucket (Data): tm-data-bucket-20251010
  ✓ IAM Role: datahose-app-flink-role
  ✓ CloudWatch Log Group: /aws/kinesis-analytics/datahose-app
```

### 3. Build and Deploy

```bash
# Load configuration
source /tmp/flink-config.env

# Build and deploy
./cicd.sh
```

This script:
1. Initializes Java 11 via SDKMAN
2. Builds the application with Maven (creates 31 MB JAR)
3. Uploads JAR to S3
4. Creates/updates Flink application
5. Starts the application in STREAMING mode
6. Displays initial logs

**Expected output:**
```
[INFO] Build complete: target/datahose-app.jar (31 MB)
[INFO] JAR uploaded to S3
[INFO] Flink application created/updated
[INFO] Application starting...
[INFO] Application status: RUNNING
```

### 4. Verify Deployment

```bash
./verify.sh
```

Performs 6 health checks:
1. ✓ AWS credentials
2. ✓ S3 buckets exist
3. ✓ IAM role exists
4. ✓ CloudWatch log group exists
5. ✓ Flink application status (RUNNING)
6. ✓ Recent logs available

### 5. Monitor Data Output

```bash
# Watch application logs
aws logs tail /aws/kinesis-analytics/datahose-app --follow

# Check S3 data files
aws s3 ls s3://tm-data-bucket-20251010/datafall/ --recursive

# View sample data
aws s3 cp s3://tm-data-bucket-20251010/datafall/2025-10-10--17/part-0-0 - | head -10
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

### `verify.sh` - Health Check

**Purpose:** Verify all resources and application health

**What it does:**
Performs 6 checks:
1. AWS credentials configured
2. S3 buckets exist and accessible
3. IAM role exists
4. CloudWatch log group exists
5. Flink application status
6. Recent logs available

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
[✓] Application bucket: tm-streaming-app-bucket-20251010
[✓] Data bucket: tm-data-bucket-20251010

=== IAM Resources ===
[✓] IAM Role: datahose-app-flink-role

=== CloudWatch Logs ===
[✓] Log group: /aws/kinesis-analytics/datahose-app

=== Flink Application ===
[✓] Application: datahose-app
[✓] Status: RUNNING
[✓] Version: 3

=== Recent Logs ===
[✓] Found 150 log entries in the last 10 minutes

=== Summary ===
Health Score: 6/6 checks passed
[✓] System is healthy
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

### Source Code

**File:** `src/main/java/org/muralis/datahose/StreamingApp.java`

**Key Features:**

1. **Unbounded Data Generator**
   ```java
   DataGeneratorSource<String> source = new DataGeneratorSource<>(
       index -> String.format("Record-%d: Data from foams column at %s", 
           index, Instant.now()),
       Long.MAX_VALUE,  // Unbounded stream
       RateLimiterStrategy.perSecond(2)  // 2 records/second
   );
   ```

2. **S3 File Sink**
   ```java
   FileSink<String> sink = FileSink
       .forRowFormat(new Path("s3://tm-data-bucket-20251010/datafall"), 
           new SimpleStringEncoder<String>("UTF-8"))
       .withRollingPolicy(
           DefaultRollingPolicy.builder()
               .withRolloverInterval(Duration.ofMinutes(2))
               .withInactivityInterval(Duration.ofSeconds(30))
               .withMaxPartSize(1024 * 1024)  // 1 MB
               .build()
       )
       .build();
   ```

3. **Checkpointing**
   ```java
   env.enableCheckpointing(60000);  // Every 60 seconds
   ```

### Data Flow

```
DataGeneratorSource (2 rec/sec)
         ↓
    Stream<String>
         ↓
   S3 FileSink (rolling every 30s-2min)
         ↓
  s3://tm-data-bucket-20251010/datafall/
```

### Record Format

Each record contains:
```
Record-{index}: Data from foams column at {ISO-8601 timestamp}
```

**Example:**
```
Record-0: Data from foams column at 2025-10-10T17:15:23.456Z
Record-1: Data from foams column at 2025-10-10T17:15:23.956Z
Record-2: Data from foams column at 2025-10-10T17:15:24.456Z
```

---

## Data Structure

### S3 Table: `datafall`

**Location:** `s3://tm-data-bucket-20251010/datafall/`

**Schema:**
| Column | Type | Description |
|--------|------|-------------|
| foams  | VARCHAR | Streaming data content with timestamp |

**Directory Structure:**
```
datafall/
├── 2025-10-10--17/
│   ├── part-0-0                    # Finalized file (~7.2 KB)
│   ├── part-0-1                    # Finalized file (~7.2 KB)
│   ├── part-0-2                    # Finalized file (~7.2 KB)
│   └── .part-0-3.inprogress.xyz    # Currently being written
├── 2025-10-10--18/
│   └── ...
```

**File Properties:**
- **Naming:** `part-{subtask}-{fileIndex}`
- **In-Progress:** `.part-{subtask}-{fileIndex}.inprogress.{uuid}`
- **Format:** Plain text (UTF-8)
- **Rolling:** New file every 30s (inactivity) or 2min (max time)
- **Size:** ~7.2 KB per finalized file (at 2 rec/sec for 30-120s)

### Querying Data

**Method 1: Direct S3 read**
```bash
# List all data files
aws s3 ls s3://tm-data-bucket-20251010/datafall/ --recursive

# Download and view specific file
aws s3 cp s3://tm-data-bucket-20251010/datafall/2025-10-10--17/part-0-0 - | head -20

# Count total records in a file
aws s3 cp s3://tm-data-bucket-20251010/datafall/2025-10-10--17/part-0-0 - | wc -l
```

**Method 2: S3 Select (SQL-like)**
```bash
# Query data using S3 Select
aws s3api select-object-content \
  --bucket tm-data-bucket-20251010 \
  --key "datafall/2025-10-10--17/part-0-0" \
  --expression "SELECT * FROM S3Object[*][*] s LIMIT 10" \
  --expression-type SQL \
  --input-serialization '{"CSV": {"FileHeaderInfo": "NONE"}}' \
  --output-serialization '{"CSV": {}}' \
  output.csv
  
cat output.csv
```

**Method 3: AWS Athena** (for larger datasets)
```sql
-- Create external table
CREATE EXTERNAL TABLE datafall (
  foams STRING
)
LOCATION 's3://tm-data-bucket-20251010/datafall/';

-- Query data
SELECT COUNT(*) FROM datafall;
SELECT * FROM datafall LIMIT 10;
```

---

## Monitoring & Operations

### Application Status

```bash
# Check application status
aws kinesisanalyticsv2 describe-application \
  --application-name datahose-app \
  --query 'ApplicationDetail.ApplicationStatus'

# Possible statuses: READY, STARTING, RUNNING, STOPPING, DELETING
```

### View Logs

```bash
# Tail logs in real-time
aws logs tail /aws/kinesis-analytics/datahose-app --follow

# Get last 100 log entries
aws logs tail /aws/kinesis-analytics/datahose-app --since 10m

# Filter for errors
aws logs filter-log-events \
  --log-group-name /aws/kinesis-analytics/datahose-app \
  --filter-pattern "ERROR"
```

### Check Data Output

```bash
# Count files in S3
aws s3 ls s3://tm-data-bucket-20251010/datafall/ --recursive | wc -l

# Show recent files
aws s3 ls s3://tm-data-bucket-20251010/datafall/ --recursive | tail -10

# Calculate total data size
aws s3 ls s3://tm-data-bucket-20251010/datafall/ --recursive --summarize | grep "Total Size"
```

### Stop Application

```bash
# Stop gracefully
aws kinesisanalyticsv2 stop-application \
  --application-name datahose-app

# Force stop
aws kinesisanalyticsv2 stop-application \
  --application-name datahose-app \
  --force
```

### Start Application

```bash
# Start application (after stopping)
aws kinesisanalyticsv2 start-application \
  --application-name datahose-app \
  --run-configuration '{"ApplicationRestoreConfiguration":{"ApplicationRestoreType":"RESTORE_FROM_LATEST_SNAPSHOT"}}'
```

### Update Application

```bash
# After modifying code, rebuild and redeploy
mvn clean package
./cicd.sh
```

### Checkpoints

```bash
# List checkpoints (stored in S3)
aws s3 ls s3://tm-streaming-app-bucket-20251010/checkpoints/ --recursive

# Application automatically restores from last checkpoint on restart
```

---

## Troubleshooting

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
aws s3 ls s3://tm-data-bucket-20251010/

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

### Option 1: Use Destroy Script (Recommended)

```bash
# Interactive mode with confirmation
./iac_destroy.sh

# Force mode (skip confirmation)
./iac_destroy.sh --force
```

This safely removes:
- Flink application
- S3 buckets (all objects including versions)
- IAM role and policy
- CloudWatch log group

### Option 2: Manual Cleanup

If the destroy script fails:

```bash
# 1. Stop and delete Flink application
CREATE_TS=$(aws kinesisanalyticsv2 describe-application \
  --application-name datahose-app \
  --query 'ApplicationDetail.CreateTimestamp' --output text)

aws kinesisanalyticsv2 stop-application --application-name datahose-app --force
aws kinesisanalyticsv2 delete-application \
  --application-name datahose-app \
  --create-timestamp "$CREATE_TS"

# 2. Delete S3 buckets (including all versions)
aws s3 rb s3://tm-streaming-app-bucket-20251010 --force
aws s3 rb s3://tm-data-bucket-20251010 --force

# 3. Delete IAM resources
aws iam detach-role-policy \
  --role-name datahose-app-flink-role \
  --policy-arn arn:aws:iam::$(aws sts get-caller-identity --query Account --output text):policy/datahose-app-flink-policy

aws iam delete-policy \
  --policy-arn arn:aws:iam::$(aws sts get-caller-identity --query Account --output text):policy/datahose-app-flink-policy

aws iam delete-role --role-name datahose-app-flink-role

# 4. Delete CloudWatch log group
aws logs delete-log-group --log-group-name /aws/kinesis-analytics/datahose-app

# 5. Clean up local files
rm -f /tmp/flink-config.env
```

---

## Cost Estimation

**Estimated Monthly Cost (us-east-2):**

| Service | Usage | Estimated Cost |
|---------|-------|----------------|
| Managed Flink | 1 KPU, 24/7 | ~$45/month |
| S3 Storage | ~100 GB/month (at 2 rec/sec) | ~$2.30/month |
| S3 Requests | PUT/GET operations | ~$0.50/month |
| CloudWatch Logs | 7-day retention, moderate logs | ~$2/month |
| Data Transfer | Minimal (S3 same-region) | ~$0.50/month |
| **Total** | | **~$50/month** |

**Cost Optimization Tips:**
1. Stop application when not needed: `aws kinesisanalyticsv2 stop-application --application-name datahose-app`
2. Reduce log retention: Modify `LOG_RETENTION_DAYS` in `iac_create.sh`
3. Clean up old S3 data: Set up lifecycle policies
4. Monitor via AWS Cost Explorer

---

## Project Structure

```
datahose-app/
├── src/
│   ├── main/
│   │   └── java/
│   │       └── org/
│   │           └── muralis/
│   │               └── datahose/
│   │                   └── StreamingApp.java      # Main application
│   └── test/
│       └── java/
│           └── org/
│               └── muralis/
│                   └── datahose/
├── target/
│   └── datahose-app.jar                          # Built artifact (31 MB)
├── pom.xml                                        # Maven configuration
├── iac_create.sh                                  # Create infrastructure
├── iac_destroy.sh                                 # Destroy infrastructure
├── cicd.sh                                        # Build and deploy
├── verify.sh                                      # Health check
├── setup-env.sh                                   # Environment setup
└── README.md                                      # This file
```

---

## Technologies Used

- **Apache Flink 1.20** - Stream processing framework
- **Java 11** - Programming language (SDKMAN: 11.0.28-amzn)
- **Maven 3.x** - Build tool
- **AWS Managed Service for Apache Flink** - Serverless Flink runtime
- **Amazon S3** - Object storage for code and data
- **AWS IAM** - Identity and access management
- **Amazon CloudWatch** - Logging and monitoring
- **AWS CLI** - Infrastructure management
- **Bash** - Automation scripts

---

## Support & Documentation

### Official Documentation
- [Apache Flink Documentation](https://nightlies.apache.org/flink/flink-docs-release-1.20/)
- [AWS Managed Service for Apache Flink](https://docs.aws.amazon.com/kinesisanalytics/)
- [Flink DataStream API](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/overview/)

### Useful Commands Quick Reference

```bash
# Environment
./setup-env.sh                                    # Initialize environment
source /tmp/flink-config.env                      # Load configuration

# Infrastructure
./iac_create.sh                                   # Create all resources
./iac_destroy.sh                                  # Destroy all resources

# Deployment
./cicd.sh                                         # Build and deploy
./verify.sh                                       # Health check

# Monitoring
aws kinesisanalyticsv2 describe-application --application-name datahose-app
aws logs tail /aws/kinesis-analytics/datahose-app --follow
aws s3 ls s3://tm-data-bucket-20251010/datafall/ --recursive

# Operations
aws kinesisanalyticsv2 stop-application --application-name datahose-app
aws kinesisanalyticsv2 start-application --application-name datahose-app
```

---

## License

This project is for educational and demonstration purposes.

---

**Last Updated:** October 10, 2025  
**Version:** 1.0  
**Region:** us-east-2
