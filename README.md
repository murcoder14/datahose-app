
# AWS Streaming Data Analytics Solution (Flink, S3)

This project provides a complete, production-ready AWS solution for streaming data analytics using Apache Flink 1.19 (AWS Managed Flink) and S3. It demonstrates **batch-style aggregation** on bounded streams using stateful processing with `KeyedProcessFunction` to emit only final aggregated results.

**Status:** ✅ Production-ready  
**Region:** Configurable (uses your AWS CLI profile region)  
**Last Updated:** November 16, 2025

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
```
┌──────────────────────────────────────────────────────────────────────────────┐
│                     AWS Cloud (Your Configured Region)                      │
├──────────────────────────────────────────────────────────────────────────────┤
│ S3 Input (gymvisits.csv) → Flink (Batch Aggregation) → S3 Output           │
│                                                                              │
│  ┌───────────────┐   ┌───────────────┐   ┌───────────────┐   ┌────────────┐  │
│  │ S3 Input     │   │ Flink         │   │ Keyed         │   │ S3 Output  │  │
│  │ gymvisits.csv│──→│ FileSource    │──→│ ProcessFunc   │──→│ FileSink   │  │
│  │              │   │ (Read CSV)    │   │ (Accumulate)  │   │ (Results)  │  │
│  └───────────────┘   └───────────────┘   └───────────────┘   └────────────┘  │
│                                                                              │
│  Batch-Style Aggregation:                                                   │
│  • Reads gym visit data from S3 (bounded stream)                           │
│  • Accumulates visit counts per person in keyed state                      │
│  • Uses KeyedProcessFunction with event-time timer at Long.MAX_VALUE       │
│  • Emits only final aggregated totals when bounded input completes         │
│  • No intermediate outputs - pure batch aggregation in streaming mode      │
│  • Writes formatted output to S3                                           │
└──────────────────────────────────────────────────────────────────────────────┘
```
```

---

---


## Components


### Flink Application (`datahose-app`)
- Apache Flink 1.19 (STREAMING mode)
- Java 11 (SDKMAN: 11.0.29-amzn)
- **Input:** Reads CSV files from S3 using FileSource (bounded streams)
- **Processing:** Batch-style aggregation using KeyedProcessFunction with stateful accumulation
- **Key Innovation:** Event-time timer at Long.MAX_VALUE ensures emission only when bounded input completes
- **Output:** Writes final aggregated results to S3 using FileSink (no intermediate outputs)
- Checkpointing: 60s
- Rolling Policy: 5s rollover / 3s inactivity

### S3 Buckets
- **Dynamic Naming:** Buckets are created with the current date and Unix epoch for uniqueness, e.g. `tm-streaming-app-bucket-20251010-1760143646` and `tm-data-bucket-20251010-1760143646`.
- Application Bucket: `tm-streaming-app-bucket-<date>-<epoch>` (stores JAR)
- Data Bucket: `tm-data-bucket-<date>-<epoch>` (stores input CSV and output results)
  - Buckets are auto-detected by scripts; no need to manually update names after each deployment.

### Input Data
- **Format:** CSV file (`gymvisits.csv`)
- **Schema:** name (STRING), date (STRING)
- **Location:** `inputs/` directory (local) or S3 input bucket
- **Example Data:** Gym visit records for aggregation

### IAM Resources
- Role: `datahose-app-flink-role` (for Flink)
- Policy: `datahose-app-flink-policy` (S3, CloudWatch)

### CloudWatch
- Log Group: `/aws/kinesis-analytics/datahose-app`
- Retention: 7 days

---

## How Application Configuration Works

The Flink application reads S3 paths and configuration directly from the code or environment variables. For S3-to-S3 processing:

1. **Input Path:** Configured in `StreamingApp.java` to read from S3 bucket
2. **Output Path:** Configured in `StreamingApp.java` to write to S3 bucket
3. **Environment Variables:** Can be passed via AWS Managed Flink Application Properties if needed

**Key Java code:**
```java
// S3 paths configured in the application
String s3InputPath = "s3a://your-input-bucket/gymvisits.csv";
String s3OutputPath = "s3a://your-output-bucket/results/";

// Create FileSource to read from S3
FileSource<String> source = FileSource
    .forRecordStreamFormat(new TextLineInputFormat(), new Path(s3InputPath))
    .build();
```

**Benefits:**
- No external dependencies like Kinesis
- Simple file-based processing
- Easy to test locally with file paths
- AWS best practice for batch/bounded stream processing

---

---


## Prerequisites

1. **AWS CLI** (v2+)
2. **Maven** (3.x+)
3. **Java 11** (via SDKMAN recommended)
4. **jq** (for JSON parsing)
5. **AWS credentials** with permissions for IAM, S3, CloudWatch
6. **AWS Region** configured in your AWS CLI profile (e.g., via `aws configure`)

---


## 🚀 Quick Start

### 1. Set Java Version (REQUIRED in every new terminal)
```bash
sdk use java 11.0.29-amzn
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

### 5. Upload Input Data (Optional for testing)
```bash
# Upload sample CSV to S3 input bucket
aws s3 cp inputs/gymvisits.csv s3://<your-input-bucket>/
```

### 6. Monitor Logs & Output
```bash
aws logs tail /aws/kinesis-analytics/datahose-app --follow
aws s3 ls s3://<latest-tm-data-bucket-*>/results/ --recursive
aws s3 cp s3://<latest-tm-data-bucket-*>/results/part-0-0 - | head -20
# Output should show aggregated gym visits per person
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
export REGION="<your-aws-region>"  # Detected from AWS CLI profile
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

=== IAM Resources ===
[✓] IAM Role exists: datahose-app-flink-role
[✓] Role ARN: arn:aws:iam::047472788728:role/datahose-app-flink-role
[✓] Attached policies: 1

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
Health Score: 7/7 checks passed
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
- **S3-to-S3 Analytical Processing:** Reads CSV data from S3, performs aggregations, writes results to S3
- **Stateful Batch Aggregation:** Uses KeyedProcessFunction with ValueState to accumulate counts
- **Event-Time Timer Pattern:** Registers timer at Long.MAX_VALUE - 1 to detect end of bounded input
- **Single Emission per Key:** Outputs only final aggregated totals (no intermediate results)
- **Checkpointing:** 60s intervals for fault tolerance
- **Rolling Policy:** 5s rollover, 3s inactivity for output files

### Batch-Style Aggregation in Streaming Mode

#### The Challenge: AWS Managed Flink and BATCH Mode

Multiple approaches were attempted to achieve proper batch aggregation:

1. **Table API with GROUP BY:** Produces changelog streams with UPDATE_BEFORE/UPDATE_AFTER rows
   - `toChangelogStream().filter(INSERT)` only captures first occurrence
   - `toDataStream()` rejected by planner for updating tables
   - SQL projections maintain update semantics

2. **Windowing Approaches:** GlobalWindows with CountTrigger fire on every element (incremental outputs)

3. **RuntimeExecutionMode.BATCH:** AWS Managed Flink throws `UnsupportedOperationException`:
   ```
   ResultPartition.getAllDataProcessedFuture not supported
   ```

#### The Solution: KeyedProcessFunction with State

**Core Implementation:**
```java
.keyBy(value -> value.f0)
.process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
    private ValueState<Integer> countState;
    private ValueState<Boolean> timerRegistered;
    
    @Override
    public void open(Configuration parameters) {
        countState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("count", Types.INT));
        timerRegistered = getRuntimeContext().getState(
            new ValueStateDescriptor<>("timer", Types.BOOLEAN));
    }
    
    @Override
    public void processElement(Tuple2<String, Integer> value, Context ctx, 
                               Collector<Tuple2<String, Integer>> out) throws Exception {
        // Accumulate count in state
        Integer currentCount = countState.value();
        countState.update((currentCount == null ? 0 : currentCount) + value.f1);
        
        // Register timer on first element for this key
        if (timerRegistered.value() == null) {
            ctx.timerService().registerEventTimeTimer(Long.MAX_VALUE - 1);
            timerRegistered.update(true);
        }
    }
    
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, 
                       Collector<Tuple2<String, Integer>> out) throws Exception {
        // Emit final aggregated count when bounded input completes
        out.collect(new Tuple2<>(ctx.getCurrentKey(), countState.value()));
    }
})
```

#### How It Works

1. **Stateful Accumulation:** Each key maintains a `ValueState<Integer>` accumulating visit counts
2. **Timer Registration:** On first element per key, registers event-time timer at `Long.MAX_VALUE - 1`
3. **End-of-Input Detection:** When bounded stream completes, watermark advances to Long.MAX_VALUE, firing timer
4. **Single Emission:** Timer callback emits final aggregated count (no intermediate outputs)

#### Why This Approach?

- **AWS Compatibility:** Works in STREAMING mode (required by AWS Managed Flink)
- **Batch Semantics:** Achieves batch-style aggregation without true BATCH mode
- **Correctness:** Emits only final totals after all input processed
- **Simplicity:** No changelog handling, no windowing complexity

#### Verification

Input: 150 gym visits across 8 people in `gymvisits.csv`

Output (final totals only):
```
Dan,7
Kate,31
Mark,31
Peter,30
Joe,20
Len,19
Jill,7
Nick,5
```

#### References
- [Flink ProcessFunction Documentation](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/datastream/operators/process_function/)
- [Flink State Documentation](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/datastream/fault-tolerance/state/)

---

## Data Structure

### Input: S3 CSV File

**Location:** `s3://tm-input-data-bucket-<date>-<epoch>/datafall/gymvisits.csv`

**Format:** CSV with person name and timestamp
```csv
person,timestamp
Dan,2024-10-21 10:15:00
Kate,2024-10-21 10:30:00
Joe,2024-10-21 11:00:00
...
```

**Sample Data:** 150 gym visit records across 8 unique people

### Output: S3 Aggregated Results

**Location:** `s3://tm-output-data-bucket-<date>-<epoch>/datalake/`

**Format:** CSV with person name and total visit count
```csv
Dan,7
Kate,31
Mark,31
Peter,30
Joe,20
Len,19
Jill,7
Nick,5
```

**Characteristics:**
- One row per unique person (8 total)
- Final aggregated counts only (no intermediate outputs)
- Results match actual CSV data (100% accuracy verified)

**Example Output:**
```
Alice visited the gym 25 times
Bob visited the gym 18 times
Carol visited the gym 30 times
```

**Directory Structure:**
```
results/  (or datafall/)
├── 2025-11-14--17/
│   ├── part-0-0                    # Finalized file with aggregated results
│   ├── part-0-1                    # Finalized file
│   └── .part-0-2.inprogress.xyz    # In-progress
├── ...
```

**Querying Data:**
```bash
# List files
aws s3 ls s3://<latest-tm-data-bucket-*>/results/ --recursive
# View sample
aws s3 cp s3://<latest-tm-data-bucket-*>/results/part-0-0 - | head -10
# Should show aggregated visit counts per person
```

---

---


## Testing & Monitoring

### Upload Test Data
```bash
# Upload sample CSV to S3 input bucket
aws s3 cp inputs/gymvisits.csv s3://tm-input-data-bucket-<date>-<epoch>/datafall/gymvisits.csv
```

### Monitor Logs
```bash
aws logs tail /aws/kinesis-analytics/datahose-app --follow
```

### Check S3 Output
```bash
# List output files
aws s3 ls s3://tm-output-data-bucket-<date>-<epoch>/datalake/ --recursive

# View aggregated results
aws s3 cp s3://tm-output-data-bucket-<date>-<epoch>/datalake/part-0-0 - | head -20

# Expected output (CSV format):
# Dan,7
# Kate,31
# Mark,31
# Peter,30
# Joe,20
# Len,19
# Jill,7
# Nick,5
```

### Verify Results
After deployment, expect exactly 8 output rows (one per unique person) with final aggregated counts matching input data.

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

### No data in S3 output
```bash
aws s3 ls s3://tm-output-data-bucket-<date>-<epoch>/datalake/
aws logs tail /aws/kinesis-analytics/datahose-app --since 30m | grep -i "s3\|error"
# Wait at least 5 seconds for first file (rolling policy)
```
**Common Causes:**
- Input file missing or incorrect path
- Timer not firing (check watermark advancement)
- FileSink configuration issue

### Incorrect aggregation counts
- Verify input CSV format: `person,timestamp` with header row
- Check logs for parsing errors: `grep -i "flatmap\|parse" in application logs`
- Ensure bounded stream: `WatermarkStrategy.forMonotonousTimestamps()`
- Rebuild and redeploy: `./cicd.sh`

### Timer not firing (no output)
- **Symptom:** Application runs but produces no output
- **Cause:** Event-time watermark not advancing to Long.MAX_VALUE
- **Check:** Input must be bounded (file-based) with proper watermark strategy
- **Solution:** Verify `WatermarkStrategy.forMonotonousTimestamps()` is used

### Region or Java version issues
- Verify your AWS region is configured: `aws configure get region`
- Always run `sdk use java 11.0.29-amzn` in every new terminal

---

### Application Won't Start (Detailed)

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

2. **IAM Permissions:** Role doesn't have S3 read/write access
   - Check policy: `aws iam get-role-policy --role-name datahose-app-flink-role --policy-name datahose-app-flink-policy`
   - Verify S3 permissions are present

3. **JAR File Corrupt:** Upload failed or build issue
   - Rebuild: `mvn clean package`
   - Re-upload: `./cicd.sh`

### No Data in S3 (Detailed)

**Symptom:** Application running but no files in S3 output bucket

**Diagnosis:**
```bash
# Check if output bucket exists
aws s3 ls s3://tm-output-data-bucket-<date>-<epoch>/datalake/

# Check application logs for errors
aws logs tail /aws/kinesis-analytics/datahose-app --since 30m | grep -i "s3\|error\|timer"

# Verify rolling policy timing
# Files appear after 3s inactivity OR 5s max interval
```

**Possible Causes:**
1. **Input Missing:** Ensure `gymvisits.csv` exists in input bucket
2. **Timer Not Firing:** Watermark not advancing (check bounded stream configuration)
3. **Permissions:** Verify IAM role has GetObject and PutObject permissions
4. **Path Issue:** Check application logs for S3 read/write errors

### KeyedProcessFunction Debugging

**Symptom:** No output or unexpected aggregation behavior

**Debug Steps:**
1. **Verify State Updates:** Add logging in `processElement()` to confirm counts accumulating
2. **Check Timer Registration:** Log when timer registered (should be once per key)
3. **Monitor Watermark:** Ensure watermark advances to Long.MAX_VALUE for bounded streams
4. **Validate Input Parsing:** Check FlatMap output (should produce Tuple2<String, 1> per row)

### Region Mismatch

**Symptom:** Resources not found or "Access Denied" errors

**Diagnosis:**
```bash
# Check configured region
aws configure get region

# Should output your intended region (e.g., us-east-1, us-west-2, etc.)
```

**Fix:**
```bash
# Set your desired region
aws configure set region <your-preferred-region>

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
sdk use java 11.0.29-amzn

# Verify it's set correctly
java -version

# Alternative: Set JAVA_HOME manually
export JAVA_HOME=$(dirname $(dirname $(which java)))

# OR: Run the setup script which does this automatically
./setup-env.sh
```

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
| S3 Storage           | ~100 GB      | ~$2.30         |
| CloudWatch           | Logs/metrics | ~$2            |
| Data Transfer        | Minimal      | ~$0.50         |
| **Total**            |              | **~$50/month** |

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
- Java 11 (SDKMAN: 11.0.29-amzn)
- Maven 3.x
- AWS Managed Service for Apache Flink
- Amazon S3 (FileSource & FileSink)
- Flink Table API & SQL
- AWS IAM
- Amazon CloudWatch
- AWS CLI
- Bash

---

---


## References

- [Apache Flink Documentation](https://nightlies.apache.org/flink/flink-docs-release-1.20/)
- [AWS Managed Service for Apache Flink](https://docs.aws.amazon.com/kinesisanalytics/)
- [Flink DataStream API](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/overview/)
- [Flink Table API & Changelog Streams](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/data_stream_api/#handling-of-changelog-streams)

---

**License:** For educational and demonstration purposes.

**Last Updated:** November 14, 2025  
**Version:** 2.0  
**Region:** Configurable (uses your AWS CLI profile region)
