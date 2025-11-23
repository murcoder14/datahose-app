# Datahose Streaming Application

A real-time data streaming application built with **Apache Flink** that processes messages from **Amazon Kinesis Data Streams** and writes them to **Apache Iceberg** tables in an S3-based data lake. The application uses **Flink Side Outputs** to route different message types (Claims and Leave Requests) to separate processing pipelines.

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Key Technologies](#key-technologies)
- [Local Development Setup](#local-development-setup)
- [Running the Application](#running-the-application)
- [Technical Deep Dive](#technical-deep-dive)
  - [Flink Side Outputs Pattern](#flink-side-outputs-pattern)
  - [Avro Schema Evolution](#avro-schema-evolution)
  - [Iceberg Integration](#iceberg-integration)
  - [Checkpointing and State](#checkpointing-and-state)
- [Testing](#testing)
- [AWS Deployment](#aws-deployment)
- [Monitoring and Troubleshooting](#monitoring-and-troubleshooting)

---

## Architecture Overview

```
┌─────────────────┐
│  Kinesis Data   │
│     Stream      │  JSON messages (Claims, Leave Requests)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Flink Source   │  SimpleStringSchema deserialization
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ MessageRouter   │  Side Outputs: routes by messageType field
└────────┬────────┘
         │
    ┌────┴────┬──────────────────┬───────────────┐
    ▼         ▼                  ▼               ▼
┌───────┐ ┌────────┐      ┌──────────┐   ┌──────────┐
│Claims │ │ Leave  │      │ Unknown  │   │  Parse   │
│  TAG  │ │  TAG   │      │   TAG    │   │  Error   │
└───┬───┘ └───┬────┘      └─────┬────┘   └────┬─────┘
    │         │                  │             │
    ▼         ▼                  ▼             ▼
┌────────┐ ┌────────┐      ┌──────────────────────┐
│Claims  │ │Leave   │      │   Unknown Messages   │
│Process │ │Process │      │   S3 FileSink        │
└───┬────┘ └───┬────┘      └──────────────────────┘
    │          │
    ▼          ▼
┌────────────────────────┐
│  Iceberg Sink Builder  │
│  (SpecificRecord →     │
│   RowData Conversion)  │
└───────────┬────────────┘
            │
            ▼
┌────────────────────────┐
│  Apache Iceberg Tables │
│  (Avro, Partitioned)   │
│  - tm_data_lake.claims │
│  - tm_data_lake.leave  │
│    _requests           │
└───────────┬────────────┘
            │
            ▼
┌────────────────────────┐
│   AWS Glue Catalog     │
│   (Metadata Storage)   │
└────────────────────────┘
            │
            ▼
┌────────────────────────┐
│   Amazon Athena        │
│   (SQL Queries)        │
└────────────────────────┘
```

**Data Flow:**

1. **Ingestion:** JSON messages sent to Kinesis Data Stream
2. **Routing:** Flink reads from Kinesis and routes messages using Side Outputs based on `messageType` field
3. **Processing:** Each pipeline (Claims, Leave Requests) converts JSON → Avro SpecificRecord
4. **Storage:** Iceberg sink converts SpecificRecord → RowData and writes to partitioned Avro data files
5. **Cataloging:** Glue tracks table schemas and partitions via Iceberg metadata files
6. **Querying:** Athena provides SQL interface to query Iceberg tables

---

## Key Technologies

### Apache Flink 1.20.0
- **Streaming Engine:** Processes unbounded data streams with low latency
- **Checkpointing:** Exactly-once semantics via distributed snapshots (30-second interval)
- **Side Outputs:** Splits single stream into multiple typed outputs without duplication
- **Parallelism:** Currently set to 1 (adjustable for scale)

### Amazon Kinesis Data Streams
- **Message Ingestion:** Durable, scalable message queue
- **Retention:** 24 hours (default)
- **Shards:** 1 shard (adjustable for throughput)
- **Consumer:** Flink KinesisStreamsSource with at-least-once delivery

### Apache Avro
- **Schema Definition:** `.avsc` files in `src/main/resources/avro/`
- **Code Generation:** Maven Avro plugin generates Java classes (SpecificRecord)
- **Binary Serialization:** Compact, fast encoding for storage
- **Evolution:** Schema compatibility rules for backwards/forwards compatibility

### Apache Iceberg 1.9.1
- **Table Format:** ACID transactions, schema evolution, time travel, hidden partitioning
- **Storage:** Avro data files organized by year/month/day/hour partitions
- **Catalog:** AWS Glue Data Catalog for metadata management
- **Commits:** Atomically commits on Flink checkpoints (30-second interval)

### AWS Managed Service for Apache Flink
- **Serverless Flink:** No infrastructure management
- **Auto-scaling:** KPU-based scaling (1 KPU = 1 vCPU + 4 GB RAM)
- **Integrated Monitoring:** CloudWatch Logs and Metrics
- **State Management:** Managed checkpoints and savepoints in S3

---

## Local Development Setup

### Prerequisites

1. **Java 11** (via SDKMAN)
2. **Maven 3.9+**
3. **AWS CLI v2** (configured with credentials)
4. **Docker** (optional, for local testing)

### Step 1: Install Dependencies

```bash
# Initialize SDKMAN
curl -s "https://get.sdkman.io" | bash
source "$HOME/.sdkman/bin/sdkman-init.sh"

# Install Java 11
sdk install java 11.0.29-amzn
sdk use java 11.0.29-amzn

# Verify installation
java -version  # Should show 11.0.29

# Install Maven (if needed)
sdk install maven 3.9.9
```

### Step 2: Configure AWS

```bash
# Set region
aws configure set region us-east-2

# Verify credentials
aws sts get-caller-identity
```

### Step 3: Set Up Environment

```bash
# Clone repository
git clone <your-repo-url>
cd datahose-app

# Load environment variables
source ./setup-env.sh
```

The `setup-env.sh` script:
- Initializes SDKMAN and sets Java 11
- Loads Flink configuration from `/tmp/flink-config.env` (created by `iac_create.sh`)
- Sets AWS region

### Step 4: Build the Application

```bash
# Clean and compile
mvn clean compile

# Generate Avro classes (automatically done during compile)
# Output: target/generated-sources/avro/org/muralis/datahose/avro/

# Package uber JAR (includes all dependencies)
mvn package -DskipTests

# Verify JAR size (~221 MB)
ls -lh target/datahose-app-1.0-SNAPSHOT.jar
```

**Maven Build Process:**

1. **Avro Plugin:** Generates Java classes from `.avsc` schemas
   - Input: `src/main/resources/avro/Claim.avsc`, `LeaveRequest.avsc`
   - Output: `Claim.java`, `LeaveRequest.java` (extends SpecificRecord)
   - Adds partition fields: `year`, `month`, `day`, `hour`

2. **Maven Compiler:** Compiles all Java sources (including generated Avro classes)

3. **Shade Plugin:** Creates uber JAR with relocated packages
   - Relocates: Jackson, Guava, Netty (avoids classpath conflicts)
   - Transforms: Hadoop FileSystem services, Flink configuration
   - Excludes: AWS SDK (provided by Flink runtime)

---

## Running the Application

### Local Testing (IDE)

**Option 1: Run from IntelliJ IDEA**

1. Open project in IntelliJ
2. Set VM options in Run Configuration:
   ```
   -Daws.region=us-east-2
   ```
3. Set environment variables:
   ```
   KINESIS_STREAM_ARN=<your-stream-arn>
   DEFAULT_OUTPUT_BUCKET=<your-bucket>
   ICEBERG_WAREHOUSE_BUCKET=<your-iceberg-bucket>
   GLUE_DATABASE_NAME=tm_data_lake
   ```
4. Run `StreamingApp.main()`

**Option 2: Run from Command Line**

```bash
# Requires local Flink cluster
flink run -c org.muralis.datahose.StreamingApp \
  target/datahose-app-1.0-SNAPSHOT.jar
```

### AWS Deployment

See dedicated guides:
- **Infrastructure:** [AWS_IaC.md](./AWS_IaC.md) - Create/destroy AWS resources
- **CI/CD:** [AWS_CICD.md](./AWS_CICD.md) - Build and deploy application

**Quick Start:**

```bash
# 1. Create infrastructure
./iac_create.sh
source /tmp/flink-config.env

# 2. Build and deploy
./cicd.sh

# 3. Verify deployment
./verify.sh

# 4. Send test data
./test.sh
```

---

## Technical Deep Dive

### Flink Side Outputs Pattern

**What are Side Outputs?**

Side Outputs allow a single Flink operator to emit records to multiple named output streams. This is more efficient than using `filter()` or `split()` because:
- Records are only processed once (no duplication)
- Type-safe routing with OutputTag
- Supports different data types per output

**Implementation in MessageRouter:**

```java
public class MessageRouter extends ProcessFunction<String, KinesisMessage> {
    
    // Define output tags (one per message type)
    public static final OutputTag<KinesisMessage> CLAIMS_TAG = 
        new OutputTag<KinesisMessage>("claims-output") {};
    
    public static final OutputTag<KinesisMessage> LEAVE_TAG = 
        new OutputTag<KinesisMessage>("leave-output") {};
    
    public static final OutputTag<KinesisMessage> UNKNOWN_TAG = 
        new OutputTag<KinesisMessage>("unknown-output") {};
    
    @Override
    public void processElement(String value, Context ctx, Collector<KinesisMessage> out) {
        JsonNode jsonNode = objectMapper.readTree(value);
        KinesisMessage message = new KinesisMessage();
        message.setMetadata(value);
        
        // Route based on messageType field
        if (jsonNode.has("messageType")) {
            String messageType = jsonNode.get("messageType").asText();
            
            switch (messageType) {
                case "CLAIM":
                    message.setMessageType("CLAIM");
                    ctx.output(CLAIMS_TAG, message);  // Emit to claims stream
                    break;
                    
                case "LEAVE_REQUEST":
                    message.setMessageType("LEAVE_REQUEST");
                    ctx.output(LEAVE_TAG, message);  // Emit to leave stream
                    break;
                    
                default:
                    ctx.output(UNKNOWN_TAG, message);  // Unknown types
                    break;
            }
        } else {
            // Fallback: detect by field presence
            JsonNode dataNode = jsonNode.has("data") ? jsonNode.get("data") : jsonNode;
            
            if (dataNode.has("claimId")) {
                ctx.output(CLAIMS_TAG, message);
            } else if (dataNode.has("employeeId")) {
                ctx.output(LEAVE_TAG, message);
            } else {
                ctx.output(UNKNOWN_TAG, message);
            }
        }
    }
}
```

**Extracting Side Outputs:**

```java
// Main stream processes and routes messages
SingleOutputStreamOperator<KinesisMessage> mainStream = 
    kinesisStream.process(new MessageRouter())
        .uid("message-router-operator");

// Extract side output streams
DataStream<KinesisMessage> claimsStream = mainStream.getSideOutput(MessageRouter.CLAIMS_TAG);
DataStream<KinesisMessage> leaveStream = mainStream.getSideOutput(MessageRouter.LEAVE_TAG);
DataStream<KinesisMessage> unknownStream = mainStream.getSideOutput(MessageRouter.UNKNOWN_TAG);

// Process each stream independently
configureClaimsPipeline(claimsStream, icebergClaimsProps);
configureLeavePipeline(leaveStream, icebergLeaveProps);
```

**Benefits:**
- **Scalability:** Each pipeline can have different parallelism
- **Maintainability:** Add new message types without modifying existing pipelines
- **Observability:** Separate metrics per pipeline (numRecordsIn, numRecordsOut)
- **Fault Isolation:** Failure in one pipeline doesn't affect others

---

### Avro Schema Evolution

**Schema Definition (Claim.avsc):**

```json
{
  "namespace": "org.muralis.datahose.avro",
  "type": "record",
  "name": "Claim",
  "fields": [
    {"name": "claimId", "type": "string"},
    {"name": "claimAmount", "type": "double"},
    {"name": "claimDate", "type": "string"},
    {"name": "claimStatus", "type": "string"},
    {"name": "eventTimestamp", "type": "long"},
    {"name": "year", "type": "int"},
    {"name": "month", "type": "int"},
    {"name": "day", "type": "int"},
    {"name": "hour", "type": "int"}
  ]
}
```

**Partition Fields:**

The schema includes partition fields (`year`, `month`, `day`, `hour`) calculated from the current event timestamp:

```java
public class ClaimsProcessor extends RichFlatMapFunction<KinesisMessage, Claim> {
    @Override
    public void flatMap(KinesisMessage message, Collector<Claim> out) {
        JsonNode dataNode = objectMapper.readTree(message.getMetadata()).get("data");
        
        // Extract fields from the "data" object
        String claimId = dataNode.get("claimId").asText();
        String claimStatus = dataNode.get("status").asText();
        double claimAmount = dataNode.get("amount").asDouble();
        String claimDate = dataNode.get("processedAt").asText();
        
        // Generate timestamp and partition values from current time
        long eventTimestamp = System.currentTimeMillis();
        ZonedDateTime zdt = Instant.ofEpochMilli(eventTimestamp)
            .atZone(ZoneId.of("UTC"));
        
        Claim claim = Claim.newBuilder()
            .setClaimId(claimId)
            .setClaimAmount(claimAmount)
            .setClaimDate(claimDate)
            .setClaimStatus(claimStatus)
            .setEventTimestamp(eventTimestamp)
            .setYear(zdt.getYear())
            .setMonth(zdt.getMonthValue())
            .setDay(zdt.getDayOfMonth())
            .setHour(zdt.getHour())
            .build();
        
        out.collect(claim);
    }
}
```

**Why Use Avro for Streaming?**

This project uses Avro instead of Parquet or ORC because Avro is specifically optimized for streaming data ingestion scenarios. Here's why:

1. **Row-Based Storage for Fast Writes**
   - **Streaming Optimized:** Avro is a row-based format designed for efficient data serialization, making it ideal for write-heavy use cases like data ingestion from Kinesis
   - **High Write Efficiency:** Unlike columnar formats (Parquet/ORC), Avro can write complete records quickly without buffering for column reorganization
   - **Fast Full Record Reads:** Optimized for reading entire records, which is common in streaming pipelines

2. **Excellent Schema Evolution**
   - **Schema Stored with Data:** Avro stores the schema alongside the data, enabling easier data exchange and evolution over time
   - **Seamless Version Changes:** Add/remove fields without breaking readers
     - Forward Compatibility: Old readers can read new data (with default values)
     - Backward Compatibility: New readers can read old data (ignore unknown fields)
   - **AWS Glue Schema Registry:** Avro integrates with AWS Glue Schema Registry for centralized schema management and validation

3. **Perfect Fit for Apache Kafka and Flink**
   - **Streaming Ecosystem:** Avro is the de facto standard in streaming platforms (Kafka, Flink, NiFi)
   - **Confluent Integration:** Native support in Kafka ecosystem for Avro serialization/deserialization
   - **Kinesis Data Streams:** AWS recommends Avro for schema validation in streaming scenarios

4. **Compact Binary Serialization**
   - **Size Efficiency:** Binary format is ~40% smaller than JSON
   - **Type Safety:** Compile-time validation of field types via SpecificRecord classes
   - **Fast Serialization:** Optimized binary encoding for low-latency streaming

5. **Iceberg Native Support**
   - **One of Three Formats:** Avro is a native Iceberg format (alongside Parquet and ORC)
   - **Compaction Ready:** AWS S3 Tables now supports auto-compaction for Avro files in Iceberg tables
   - **Query Performance:** While not as optimized for analytics as columnar formats, Avro provides acceptable query performance with Iceberg's metadata-driven pruning

**When to Use Each Format:**

| Format | Best For | This Project |
|--------|----------|--------------|
| **Avro** | Data ingestion, streaming, serialization, Kafka/Flink pipelines | ✅ **Used** - Kinesis streaming |
| **Parquet** | Analytics queries across columns, read-heavy workloads | ❌ Not needed - write-heavy use case |
| **ORC** | Hive-based queries, heavy compression, numerical data | ❌ Not needed - not using Hive |

**Trade-offs:**
- **Analytics Performance:** Avro is slower than Parquet for analytical queries (e.g., `SELECT AVG(claimAmount) GROUP BY insurancePlan`) because it must read entire rows instead of just the `claimAmount` column
- **Compression:** Avro has moderate compression (not as aggressive as ORC), but this is acceptable for streaming where write speed matters more
- **Compaction:** AWS S3 Tables auto-compaction now supports Avro (as of 2025), improving query performance by merging small files into larger ones

**Why Not Parquet?**
- Parquet is optimized for analytics (columnar scans), not for streaming ingestion
- Write performance is moderate because it buffers rows to reorganize into column chunks
- Better suited for batch ETL jobs, not real-time Kinesis streams

For this Flink streaming application ingesting from Kinesis, **Avro is the optimal choice** due to its high write efficiency, excellent schema evolution, and strong ecosystem support with Kafka, Flink, and AWS streaming services.

---

### Iceberg Integration

**What is Apache Iceberg?**

Iceberg is a high-performance table format for huge analytic tables. It provides:
- **ACID Transactions:** Atomic commits, isolation, consistency
- **Schema Evolution:** Add/drop/rename columns without rewriting data
- **Hidden Partitioning:** User queries don't need to know partition structure
- **Time Travel:** Query data as of specific snapshots
- **Incremental Reads:** Read only new data since last query

**Iceberg Warehouse Structure:**

The Iceberg warehouse bucket (`s3://tm-iceberg-warehouse-<timestamp>/`) stores all table data and metadata. Here's the actual structure from your deployment:

```
s3://tm-iceberg-warehouse-20251122-1763865696/
├── claims/
│   ├── data/                                    # Actual claim records (Avro format)
│   │   └── year=2025/month=11/day=23/hour=2/
│   │       ├── 00000-0-b61a8294-...-00001.avro  # Data file #1 (1.7 KB)
│   │       └── 00000-0-b61a8294-...-00002.avro  # Data file #2 (1.9 KB)
│   │
│   └── metadata/                                # Iceberg table metadata
│       ├── 00000-5cfa2c72-....metadata.json     # Table metadata v0 (schema, partitions)
│       ├── 00001-c368c6c7-....metadata.json     # Table metadata v1 (after 1st commit)
│       ├── 00002-cef22e28-....metadata.json     # Table metadata v2 (after 2nd commit)
│       ├── 62efaf82-...-m0.avro                 # Manifest file (lists data files)
│       ├── 311918b2-...-m0.avro                 # Manifest file (lists data files)
│       ├── snap-4516515981681948211-1-....avro  # Snapshot manifest list v1
│       └── snap-8038161822972775292-1-....avro  # Snapshot manifest list v2
│
└── leave_requests/
    ├── data/                                    # Actual leave request records
    │   └── year=2025/month=11/day=23/hour=2/
    │       └── 00000-0-534e4f16-...-00001.avro  # Data file (2.2 KB)
    │
    └── metadata/                                # Iceberg table metadata
        ├── 00000-24d0f73c-....metadata.json     # Table metadata v0
        ├── 00001-ce54e781-....metadata.json     # Table metadata v1
        ├── d7bb2a26-...-m0.avro                  # Manifest file
        └── snap-8473476169081210602-1-....avro  # Snapshot manifest list
```

**File Types Explained:**

1. **Data Files (`.avro` in `data/` folder)**
   - **Purpose:** Store actual claim/leave request records in Avro binary format
   - **Naming:** `<task-id>-<attempt>-<UUID>-<file-number>.avro`
   - **Partitioning:** Organized by year/month/day/hour for efficient querying
   - **Size:** Typically a few KB to several MB per file
   - **Example:** `00000-0-b61a8294-6c07-4cfc-bb36-c9eac1c2d942-00001.avro` contains claim records from hour 2 on Nov 23, 2025

2. **Metadata JSON Files (`.metadata.json`)**
   - **Purpose:** Define table schema, partition spec, sort order, and point to current snapshot
   - **Versioning:** Each commit creates a new metadata file (v0, v1, v2, ...)
   - **Contents:**
     - Table schema (columns, types)
     - Partition specification (year/month/day/hour)
     - Current snapshot ID
     - Table properties (format version, write settings)
   - **Example:** `00002-cef22e28-3754-4142-ba99-0361732be958.metadata.json` is the current metadata (version 2)

3. **Manifest Files (`.avro` files like `311918b2-...-m0.avro`)**
   - **Purpose:** List all data files in a snapshot with their statistics
   - **Format:** Avro format containing:
     - Data file paths
     - File size and record count
     - Partition values (year=2025, month=11, etc.)
     - Column-level statistics (min/max values for pruning)
   - **Example:** `311918b2-45ea-4d7d-845b-e5ce9ed22151-m0.avro` lists which data files are part of snapshot v2

4. **Snapshot Manifest Lists (`snap-<snapshot-id>-1-<UUID>.avro`)**
   - **Purpose:** Point to all manifest files for a specific snapshot
   - **Contents:** List of manifest file locations and their metadata
   - **Example:** `snap-8038161822972775292-1-311918b2-45ea-4d7d-845b-e5ce9ed22151.avro` is the manifest list for snapshot 8038161822972775292
   - **Usage:** Query engines read this first to find relevant manifest files

**How Iceberg Uses These Files:**

```
Query: SELECT * FROM claims WHERE year=2025 AND month=11

1. Read latest metadata.json (00002-cef22e28-....json)
   ↓ Get current snapshot ID: 8038161822972775292
   
2. Read snapshot manifest list (snap-8038161822972775292-1-....avro)
   ↓ Get list of manifest files: [311918b2-...-m0.avro]
   
3. Read manifest file (311918b2-...-m0.avro)
   ↓ Filter by partition (year=2025, month=11)
   ↓ Get matching data files:
   ↓   - 00000-0-b61a8294-...-00001.avro (year=2025, month=11, day=23, hour=2)
   ↓   - 00000-0-b61a8294-...-00002.avro (year=2025, month=11, day=23, hour=2)
   
4. Read data files (00001.avro, 00002.avro)
   ↓ Deserialize Avro records
   ↓ Return results to user
```

**Why This Architecture?**

- **ACID Transactions:** New commits create new metadata files atomically (no partial updates)
- **Time Travel:** Old metadata.json files = historical table versions
- **Partition Pruning:** Manifest files contain partition statistics → skip irrelevant data files
- **Schema Evolution:** Add columns by updating metadata.json (no data rewrite needed)
- **Scalability:** Millions of data files tracked efficiently via manifest files

**Partition Pruning Example:**

```sql
-- Query only November 2025, hour 2 data (reads only 3 data files)
SELECT * FROM tm_data_lake.claims
WHERE year = 2025 AND month = 11 AND hour = 2;

-- Athena reads:
--   1. metadata.json (3.9 KB)
--   2. manifest list (4.6 KB)
--   3. manifest file (8.5 KB)
--   4. Only 2 data files (1.7 KB + 1.9 KB = 3.6 KB)
-- Total: ~20 KB instead of scanning entire table
```

---

### Checkpointing and State

**Flink Checkpointing Configuration:**

```java
env.enableCheckpointing(30000);  // 30 seconds
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(15000);  // 15 seconds
env.getCheckpointConfig().setCheckpointTimeout(600000);  // 10 minutes
```

**How Checkpointing Works:**

1. **Coordinator Triggers:** Every 30 seconds, JobManager initiates checkpoint
2. **Barrier Injection:** Special checkpoint barriers flow through the data stream
3. **State Snapshot:** Each operator saves its state to S3 when barrier arrives
   - Kinesis consumer offsets
   - Iceberg pending commits
   - Window aggregations (if any)
4. **Acknowledgment:** All operators report success to JobManager
5. **Commit:** JobManager marks checkpoint as complete
6. **Iceberg Commit:** On successful checkpoint, Iceberg commits pending files

**Exactly-Once Semantics:**

```
Kinesis (at-least-once) + Flink State + Iceberg Commits = End-to-End Exactly-Once

Example:
1. Flink reads message from Kinesis (offset 100)
2. Processes message, writes Avro data file to S3
3. Checkpoint #N saves state: {kinesis_offset: 100, pending_file: abc123.avro}
4. Iceberg commits file abc123.avro (atomic metadata update)
5. Checkpoint completes successfully
6. Application crashes ❌
7. Flink restarts from checkpoint #N
8. Resumes from Kinesis offset 100 (may re-read message)
9. Iceberg detects duplicate file (idempotent commit) ✓
10. Result: Message processed exactly once in Iceberg table
```

---

## Testing

### Send Test Messages to Kinesis

```bash
# Use test.sh script (recommended)
./test.sh

# Or manually with AWS CLI (note: data must be base64-encoded)
# Claim message format:
aws kinesis put-record \
  --stream-name datahose-app-stream \
  --partition-key "test-1" \
  --data "$(echo '{
    "messageType": "CLAIM",
    "timestamp": "2025-11-22T10:30:00Z",
    "data": {
      "claimId": "CLM-2025-001",
      "status": "Approved",
      "amount": 1250.50,
      "processedAt": "2025-11-22T10:30:00Z"
    }
  }' | base64)" \
  --region us-east-2

# Leave request message format:
aws kinesis put-record \
  --stream-name datahose-app-stream \
  --partition-key "test-2" \
  --data "$(echo '{
    "messageType": "LEAVE_REQUEST",
    "timestamp": "2025-11-22T10:35:00Z",
    "data": {
      "employeeId": "EMP-12345",
      "leaveType": "Vacation",
      "startDate": "2025-12-01",
      "endDate": "2025-12-15",
      "approvalStatus": "Approved"
    }
  }' | base64)" \
  --region us-east-2

# Alternative: Use file-based approach for complex JSON
cat > /tmp/claim-test.json <<'EOF'
{
  "messageType": "CLAIM",
  "timestamp": "2025-11-22T10:30:00Z",
  "data": {
    "claimId": "CLM-2025-001",
    "status": "Approved",
    "amount": 1250.50,
    "processedAt": "2025-11-22T10:30:00Z"
  }
}
EOF

aws kinesis put-record \
  --stream-name datahose-app-stream \
  --partition-key "test-1" \
  --data "$(cat /tmp/claim-test.json | base64)" \
  --region us-east-2
```

**Important:** The Claim message structure has these required fields in the `data` object:
- `claimId`: Unique claim identifier (string)
- `status`: Claim status like "Approved", "Pending", "Denied" (string)
- `amount`: Claim amount (number)
- `processedAt`: Processing timestamp in ISO-8601 format (string)

### Verify Data in Iceberg

**Using Athena (SQL):**
```sql
-- Count records
SELECT COUNT(*) FROM tm_data_lake.claims;

-- Query recent claims
SELECT claimId, patientName, claimAmount, claimDate
FROM tm_data_lake.claims
WHERE year = 2025 AND month = 1
ORDER BY claimDate DESC
LIMIT 10;
```

---

## AWS Deployment

### Quick Reference

| Task | Script | Description |
|------|--------|-------------|
| **Create Infrastructure** | `./iac_create.sh` | S3 buckets, Kinesis, Glue, IAM roles |
| **Deploy Application** | `./cicd.sh` | Build JAR, upload to S3, create/update Flink app |
| **Verify Health** | `./verify.sh` | Check all resources, app status, logs |
| **Send Test Data** | `./test.sh` | Send sample claims and leave requests |
| **Destroy Everything** | `./iac_destroy.sh` | Delete all AWS resources |

### Detailed Guides

- **Infrastructure:** [AWS_IaC.md](./AWS_IaC.md) - Complete infrastructure setup and management
- **CI/CD:** [AWS_CICD.md](./AWS_CICD.md) - Build, deployment, and monitoring

---

## Monitoring and Troubleshooting

### CloudWatch Logs

```bash
# Tail logs
aws logs tail /aws/kinesis-analytics/datahose-app --follow --region us-east-2

# Filter by keyword
aws logs tail /aws/kinesis-analytics/datahose-app \
  --filter-pattern "ERROR" \
  --follow
```

### Key Metrics

| Metric | Description | Threshold |
|--------|-------------|-----------|
| `NumRecordsIn` | Records consumed from Kinesis | > 0 (if sending data) |
| `NumRecordsOut` | Records written to sinks | Should match `NumRecordsIn` |
| `CheckpointDuration` | Time to complete checkpoint | < 60 seconds |
| `FullRestarts` | Number of full application restarts | 0 (indicates stability) |

For detailed troubleshooting, see [AWS_CICD.md](./AWS_CICD.md).

---

## Project Structure

```
datahose-app/
├── src/main/java/org/muralis/datahose/
│   ├── StreamingApp.java           # Main Flink application
│   ├── dto/
│   │   ├── KinesisMessage.java     # Wrapper for JSON messages
│   │   └── MessageType.java        # Enum for message types
│   ├── iceberg/
│   │   └── IcebergSinkBuilder.java # Builds Iceberg sinks
│   └── processors/
│       ├── ClaimsProcessor.java    # JSON → Claim conversion
│       ├── LeaveProcessor.java     # JSON → LeaveRequest conversion
│       └── MessageRouter.java      # Side Outputs router
├── src/main/resources/avro/
│   ├── Claim.avsc                  # Claim Avro schema
│   └── LeaveRequest.avsc           # LeaveRequest Avro schema
├── pom.xml                         # Maven build configuration
├── README.md                       # This file
├── AWS_IaC.md                      # Infrastructure guide
├── AWS_CICD.md                     # Deployment guide
└── [deployment scripts]            # iac_create.sh, cicd.sh, verify.sh, etc.
```

---

## References

- [Apache Flink Documentation](https://nightlies.apache.org/flink/flink-docs-release-1.20/)
- [Apache Iceberg Documentation](https://iceberg.apache.org/docs/1.9.1/)
- [Apache Avro Documentation](https://avro.apache.org/docs/1.11.3/)
- [AWS Managed Flink Developer Guide](https://docs.aws.amazon.com/managed-flink/latest/java/what-is.html)
- [Flink Side Outputs](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/side_output/)
- [AWS Prescriptive Guidance: Apache Iceberg on AWS](https://docs.aws.amazon.com/prescriptive-guidance/latest/apache-iceberg-on-aws/getting-started.html) - **Recommended reading for getting started with Iceberg on AWS**

---

**Last Updated:** November 22, 2025  
**Version:** 2.0
