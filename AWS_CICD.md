# AWS CI/CD Deployment Guide

This document describes the build, package, and deployment process for the Flink streaming application to AWS Managed Service for Apache Flink.

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Build Process](#build-process)
- [Deployment Script](#deployment-script)
- [Application Configuration](#application-configuration)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)
- [Best Practices](#best-practices)

---

## Overview

The CI/CD process automates:
1. **Build:** Maven compilation and uber JAR packaging
2. **Upload:** JAR upload to S3 application bucket
3. **Deploy:** Flink application creation/update on AWS
4. **Verify:** Health checks and log monitoring

### Deployment Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────────────────┐
│  Maven      │────▶│  S3 Bucket  │────▶│  Managed Flink Service  │
│  Build      │     │  (JAR)      │     │  (Application)          │
└─────────────┘     └─────────────┘     └─────────────────────────┘
      ▲                                             │
      │                                             ▼
      │                                    ┌─────────────────┐
      └────────────────────────────────────│  CloudWatch     │
                (Rollback on failure)       │  Logs/Metrics   │
                                            └─────────────────┘
```

---

## Prerequisites

### Required Tools

1. **Java 11** (via SDKMAN)
   ```bash
   sdk install java 11.0.29-amzn
   sdk use java 11.0.29-amzn
   ```

2. **Maven 3.9+**
   ```bash
   mvn --version
   ```

3. **AWS CLI v2**
   ```bash
   aws --version
   ```

4. **jq** (JSON processor)
   ```bash
   sudo apt install jq  # Ubuntu/Debian
   brew install jq      # macOS
   ```

### AWS Configuration

```bash
# Set region
aws configure set region us-east-2

# Verify credentials
aws sts get-caller-identity

# Source infrastructure config
source /tmp/flink-config.env
```

### Infrastructure Requirements

Run infrastructure creation first:
```bash
./iac_create.sh
source /tmp/flink-config.env
```

---

## Build Process

### Maven Configuration (`pom.xml`)

**Key Plugins:**

1. **Maven Compiler Plugin**
   - Source/Target: Java 11
   - Encoding: UTF-8

2. **Avro Maven Plugin**
   - Generates Java classes from Avro schemas (`.avsc`)
   - Source: `src/main/resources/avro/`
   - Output: `target/generated-sources/avro/`

3. **Maven Shade Plugin**
   - Creates uber JAR with all dependencies
   - Relocates conflicting packages (Jackson, Guava, etc.)
   - Transforms services (Hadoop FileSystem, Flink components)
   - Excludes AWS SDK JARs (provided by Flink runtime)
   - Output: `target/datahose-app-1.0-SNAPSHOT.jar` (~221 MB)

### Build Workflow

**Manual Build:**
```bash
# Clean and compile
mvn clean compile

# Run tests (if any)
mvn test

# Package uber JAR
mvn package

# Skip tests
mvn clean package -DskipTests

# Verify output
ls -lh target/datahose-app-1.0-SNAPSHOT.jar
```

**Generated Sources:**
- **Avro Classes:** `target/generated-sources/avro/org/muralis/datahose/avro/`
  - `Claim.java` (SpecificRecord)
  - `LeaveRequest.java` (SpecificRecord)
  - `FileMetadata.java` (SpecificRecord)

### JAR Size Considerations

| Component | Size |
|-----------|------|
| Compiled Classes | ~20 KB |
| Iceberg Dependencies | ~180 MB |
| Hadoop & Parquet | ~30 MB |
| Flink Connectors | ~8 MB |
| Other Libraries | ~3 MB |
| **Total** | **~221 MB** |

**AWS Limits:**
- Max Application Code Size: **512 MB**
- Current Usage: **221 MB (43%)**

---

## Deployment Script

### `cicd.sh` - Automated Deployment

**Purpose:** Builds, uploads, and deploys Flink application to AWS

**Usage:**
```bash
# First-time deployment (creates new application)
./cicd.sh

# Update existing application (increments version)
./cicd.sh

# Force rebuild even if JAR exists
./cicd.sh --force
```

### Deployment Steps

The script performs these operations:

#### 1. **Configuration Validation**
```bash
# Loads environment variables
source /tmp/flink-config.env

# Validates required variables
- FLINK_ROLE_ARN
- STREAMING_APP_BUCKET
- KINESIS_STREAM_NAME
- DEFAULT_OUTPUT_BUCKET
- ICEBERG_WAREHOUSE_BUCKET
- GLUE_DATABASE_NAME
```

#### 2. **Maven Build**
```bash
# Cleans previous build
mvn clean

# Compiles and packages
mvn package -DskipTests

# Verifies JAR exists
ls target/datahose-app-1.0-SNAPSHOT.jar
```

#### 3. **S3 Upload**
```bash
# Uploads JAR to application bucket
aws s3 cp target/datahose-app-1.0-SNAPSHOT.jar \
  s3://${STREAMING_APP_BUCKET}/datahose-app-1.0-SNAPSHOT.jar

# Verifies upload
aws s3 ls s3://${STREAMING_APP_BUCKET}/
```

#### 4. **Application Deployment**

**For New Application:**
```bash
aws kinesisanalyticsv2 create-application \
  --application-name datahose-app \
  --runtime-environment FLINK-1_20 \
  --service-execution-role ${FLINK_ROLE_ARN} \
  --application-configuration '{
    "ApplicationCodeConfiguration": {
      "CodeContent": {
        "S3ContentLocation": {
          "BucketARN": "arn:aws:s3:::tm-streaming-app-bucket-20251122-1763855370",
          "FileKey": "datahose-app-1.0-SNAPSHOT.jar"
        }
      },
      "CodeContentType": "ZIPFILE"
    },
    "EnvironmentProperties": {
      "PropertyGroups": [
        {
          "PropertyGroupId": "FlinkApplicationProperties",
          "PropertyMap": {
            "kinesis.stream.name": "datahose-app-stream",
            "s3.default.output.bucket": "tm-output-20251122-1763855370",
            "iceberg.warehouse.bucket": "tm-iceberg-warehouse-20251122-1763855370",
            "glue.database.name": "tm_data_lake"
          }
        }
      ]
    },
    "FlinkApplicationConfiguration": {
      "CheckpointConfiguration": {
        "ConfigurationType": "DEFAULT"
      },
      "MonitoringConfiguration": {
        "ConfigurationType": "CUSTOM",
        "MetricsLevel": "APPLICATION",
        "LogLevel": "INFO"
      },
      "ParallelismConfiguration": {
        "ConfigurationType": "CUSTOM",
        "Parallelism": 1,
        "ParallelismPerKPU": 1,
        "AutoScalingEnabled": false
      }
    }
  }'
```

**For Existing Application (Update):**
```bash
# Get current version
CURRENT_VERSION=$(aws kinesisanalyticsv2 describe-application \
  --application-name datahose-app \
  --query 'ApplicationDetail.ApplicationVersionId' \
  --output text)

# Update application code
aws kinesisanalyticsv2 update-application \
  --application-name datahose-app \
  --current-application-version-id ${CURRENT_VERSION} \
  --application-configuration-update '{
    "ApplicationCodeConfigurationUpdate": {
      "CodeContentUpdate": {
        "S3ContentLocationUpdate": {
          "BucketARNUpdate": "arn:aws:s3:::tm-streaming-app-bucket-20251122-1763855370",
          "FileKeyUpdate": "datahose-app-1.0-SNAPSHOT.jar"
        }
      }
    }
  }'
```

#### 5. **Application Start**
```bash
# Start application in RESTORE mode (recovers from checkpoint)
aws kinesisanalyticsv2 start-application \
  --application-name datahose-app \
  --run-configuration '{
    "ApplicationRestoreConfiguration": {
      "ApplicationRestoreType": "RESTORE_FROM_LATEST_SNAPSHOT"
    }
  }'
```

#### 6. **Health Verification**
```bash
# Wait for application to reach RUNNING state
while true; do
  STATUS=$(aws kinesisanalyticsv2 describe-application \
    --application-name datahose-app \
    --query 'ApplicationDetail.ApplicationStatus' \
    --output text)
  
  if [ "$STATUS" == "RUNNING" ]; then
    echo "✓ Application is RUNNING"
    break
  elif [ "$STATUS" == "FAILED" ]; then
    echo "✗ Application FAILED to start"
    exit 1
  fi
  
  sleep 10
done

# Fetch recent logs
aws logs tail /aws/kinesis-analytics/datahose-app --follow
```

---

## Application Configuration

### Runtime Configuration

**Flink Version:** 1.20.0  
**Java Version:** 11  
**Execution Mode:** STREAMING

### Parallelism Settings

```json
{
  "ParallelismConfiguration": {
    "ConfigurationType": "CUSTOM",
    "Parallelism": 1,
    "ParallelismPerKPU": 1,
    "AutoScalingEnabled": false
  }
}
```

**KPU (Kinesis Processing Unit):**
- 1 KPU = 1 vCPU + 4 GB RAM
- Current: 1 KPU (adjust for scale)

### Checkpoint Configuration

```json
{
  "CheckpointConfiguration": {
    "ConfigurationType": "DEFAULT",
    "CheckpointingEnabled": true,
    "CheckpointInterval": 60000,
    "MinPauseBetweenCheckpoints": 5000
  }
}
```

**Default Checkpointing:**
- Interval: 60 seconds
- Storage: S3 (managed by AWS)
- Recovery: Automatic on failure

### Monitoring Configuration

```json
{
  "MonitoringConfiguration": {
    "ConfigurationType": "CUSTOM",
    "MetricsLevel": "APPLICATION",
    "LogLevel": "INFO"
  }
}
```

**Log Levels:**
- `DEBUG` - Verbose logging (use sparingly)
- `INFO` - Standard operational logs (recommended)
- `WARN` - Warnings and errors only
- `ERROR` - Errors only

### Environment Properties

**Property Group:** `FlinkApplicationProperties`

```json
{
  "kinesis.stream.name": "datahose-app-stream",
  "s3.default.output.bucket": "tm-output-20251122-1763855370",
  "iceberg.warehouse.bucket": "tm-iceberg-warehouse-20251122-1763855370",
  "glue.database.name": "tm_data_lake"
}
```

**Access in Code:**
```java
ParameterTool parameters = ParameterTool.fromSystemProperties();
String streamName = parameters.get("kinesis.stream.name");
String warehouseBucket = parameters.get("iceberg.warehouse.bucket");
```

---

## Monitoring

### CloudWatch Logs

**Log Group:** `/aws/kinesis-analytics/datahose-app`

**View Logs:**
```bash
# Tail logs in real-time
aws logs tail /aws/kinesis-analytics/datahose-app --follow

# Get last 100 lines
aws logs tail /aws/kinesis-analytics/datahose-app --since 1h

# Filter by pattern
aws logs tail /aws/kinesis-analytics/datahose-app \
  --filter-pattern "ERROR" \
  --follow
```

### CloudWatch Metrics

**Available Metrics:**
- `KPUs` - Number of Kinesis Processing Units
- `Uptime` - Application uptime
- `DownTime` - Cumulative downtime
- `FullRestarts` - Number of full restarts
- `NumRecordsIn` - Records consumed from Kinesis
- `NumRecordsOut` - Records written to sinks
- `CheckpointDuration` - Time to complete checkpoints
- `LastCheckpointDuration` - Duration of last checkpoint
- `CurrentInputWatermark` - Event time progress

**View Metrics:**
```bash
# Get metrics via CLI
aws cloudwatch get-metric-statistics \
  --namespace AWS/KinesisAnalytics \
  --metric-name NumRecordsIn \
  --dimensions Name=Application,Value=datahose-app \
  --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 300 \
  --statistics Sum
```

### Flink Dashboard

**Access URL:**
```bash
# Get dashboard URL
aws kinesisanalyticsv2 describe-application \
  --application-name datahose-app \
  --query 'ApplicationDetail.ApplicationConfigurationDescription.FlinkApplicationConfigurationDescription.JobPlanDescription' \
  --output text
```

**Dashboard Features:**
- Job graph visualization
- Task manager metrics
- Checkpoint statistics
- Back pressure monitoring
- Exception history

---

## Troubleshooting

### Build Failures

**Issue:** Maven build fails with compilation errors

**Fix:**
```bash
# Clean Maven cache
mvn clean

# Update dependencies
mvn dependency:resolve

# Check Java version
java -version  # Should be 11.x

# Rebuild with verbose output
mvn clean package -X
```

---

### JAR Upload Failures

**Issue:** S3 upload fails or times out

**Fix:**
```bash
# Check bucket exists
aws s3 ls s3://${STREAMING_APP_BUCKET}/

# Check IAM permissions
aws s3api get-bucket-location --bucket ${STREAMING_APP_BUCKET}

# Manual upload with progress
aws s3 cp target/datahose-app-1.0-SNAPSHOT.jar \
  s3://${STREAMING_APP_BUCKET}/ \
  --storage-class STANDARD \
  --no-progress
```

---

### Checkpoint Failures

**Issue:** Application keeps restarting due to checkpoint failures

**Symptoms:**
- Frequent restarts
- Logs show "Checkpoint expired before completing"
- High checkpoint duration

**Fix:**
```bash
# Increase checkpoint interval
aws kinesisanalyticsv2 update-application \
  --application-name datahose-app \
  --current-application-version-id <VERSION> \
  --application-configuration-update '{
    "FlinkApplicationConfigurationUpdate": {
      "CheckpointConfigurationUpdate": {
        "CheckpointIntervalUpdate": 120000,
        "MinPauseBetweenCheckpointsUpdate": 10000
      }
    }
  }'
```

---

### Out of Memory Errors

**Issue:** `java.lang.OutOfMemoryError` in logs

**Fix:**
```bash
# Increase parallelism (adds more KPUs)
aws kinesisanalyticsv2 update-application \
  --application-name datahose-app \
  --current-application-version-id <VERSION> \
  --application-configuration-update '{
    "FlinkApplicationConfigurationUpdate": {
      "ParallelismConfigurationUpdate": {
        "ParallelismUpdate": 2,
        "ParallelismPerKPUUpdate": 1
      }
    }
  }'
```

---

### Version Conflicts

**Issue:** Application update fails with "Version mismatch"

**Fix:**
```bash
# Get current version
CURRENT_VERSION=$(aws kinesisanalyticsv2 describe-application \
  --application-name datahose-app \
  --query 'ApplicationDetail.ApplicationVersionId' \
  --output text)

echo "Current version: $CURRENT_VERSION"

# Use correct version in update command
aws kinesisanalyticsv2 update-application \
  --application-name datahose-app \
  --current-application-version-id ${CURRENT_VERSION} \
  --application-configuration-update '{...}'
```

---

## Best Practices

### 1. Version Control

Tag each deployment:
```bash
# Tag source code
git tag -a v1.0.0 -m "Production release 1.0.0"
git push origin v1.0.0

# Track JAR version in S3
aws s3 cp target/datahose-app-1.0-SNAPSHOT.jar \
  s3://${STREAMING_APP_BUCKET}/datahose-app-v1.0.0.jar
```

### 2. Blue-Green Deployment

For zero-downtime updates:
1. Deploy new version as `datahose-app-blue`
2. Test thoroughly
3. Switch traffic (update Kinesis consumer)
4. Delete old version `datahose-app-green`

### 3. Automated Testing

Add Maven tests:
```bash
# Unit tests
mvn test

# Integration tests
mvn verify

# Skip tests only for emergency deployments
mvn package -DskipTests
```

### 4. Configuration Management

Use AWS Systems Manager Parameter Store:
```bash
# Store configuration
aws ssm put-parameter \
  --name /datahose-app/config/kinesis-stream \
  --value datahose-app-stream \
  --type String

# Read in application
String streamName = ssmClient.getParameter(
  GetParameterRequest.builder()
    .name("/datahose-app/config/kinesis-stream")
    .build()
).parameter().value();
```

### 5. Monitoring Alerts

Set up CloudWatch alarms:
```bash
# Alert on application failures
aws cloudwatch put-metric-alarm \
  --alarm-name datahose-app-down \
  --metric-name Uptime \
  --namespace AWS/KinesisAnalytics \
  --statistic Average \
  --period 300 \
  --threshold 0 \
  --comparison-operator LessThanThreshold \
  --evaluation-periods 1 \
  --alarm-actions arn:aws:sns:us-east-2:ACCOUNT:alerts
```

### 6. Rollback Strategy

Keep previous JAR versions:
```bash
# List versions
aws s3 ls s3://${STREAMING_APP_BUCKET}/ --recursive

# Rollback to previous version
aws kinesisanalyticsv2 update-application \
  --application-name datahose-app \
  --current-application-version-id ${CURRENT_VERSION} \
  --application-configuration-update '{
    "ApplicationCodeConfigurationUpdate": {
      "CodeContentUpdate": {
        "S3ContentLocationUpdate": {
          "FileKeyUpdate": "datahose-app-v1.0.0.jar"
        }
      }
    }
  }'
```

---

## References

- [AWS Managed Flink Developer Guide](https://docs.aws.amazon.com/managed-flink/latest/java/what-is.html)
- [Apache Flink Documentation](https://nightlies.apache.org/flink/flink-docs-release-1.20/)
- [Maven Shade Plugin](https://maven.apache.org/plugins/maven-shade-plugin/)
- [AWS CLI Reference - Kinesis Analytics V2](https://awscli.amazonaws.com/v2/documentation/api/latest/reference/kinesisanalyticsv2/index.html)

---

**Last Updated:** November 22, 2025  
**Version:** 1.0
