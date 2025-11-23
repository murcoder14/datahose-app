# Flink Streaming Patterns - Learnings

## Topic: `keyBy()` vs `flatMap()` - When to Use Each

### Question 1: Is there a difference between keyBy and flatMap?

**Answer:** Yes, significant difference!

#### `keyBy(keySelector)` - Stream Partitioning
- **Purpose**: Partitions the stream by a key for stateful operations
- **What it does**: Routes records with the same key to the same parallel task instance
- **Returns**: `KeyedStream<T, K>` - enables keyed state and windowing
- **Use cases**:
  - Aggregations (sum, count, reduce by key)
  - Stateful processing (remember previous values per key)
  - Session/tumbling windows per key
  - Joins on a common key

**Example:**
```java
claimsStream
    .keyBy(c -> c.getClaimId())  // Partition by claimId
    .process(new ClaimsProcessor())  // State per claimId
```
This means all claims with the same `claimId` go to the same task instance, allowing you to maintain state (e.g., claim status, history).

---

#### `flatMap(function)` - Stateless Transformation
- **Purpose**: Transforms one record into 0, 1, or many records
- **What it does**: Applies function to each record independently
- **Returns**: `DataStream<T>` - no key-based partitioning
- **Use cases**:
  - Parsing/deserialization (1 JSON → 1 Avro object)
  - Filtering (1 record → 0 or 1 record)
  - Exploding (1 record → N records)
  - Simple transformations without state

**Current code:**
```java
claimsStream
    .flatMap(new ClaimsProcessor())  // Parse JSON → Avro (1:1 transformation)
    .sinkTo(claimsAvroSink);
```
Each record is independently parsed to Avro - no state, no grouping.

---

#### Key Differences

| Aspect | `keyBy()` | `flatMap()` |
|--------|-----------|-------------|
| **Statefulness** | Stateful (per key) | Stateless |
| **Partitioning** | Routes by key | Random/rebalance |
| **Parallelism** | Records with same key → same task | Any record → any task |
| **Performance** | Enables parallel aggregation | Simpler, faster for 1:1 transforms |
| **Use Case** | "Group by X and do Y" | "Transform each record" |

---

#### When Would You Need `keyBy()` in This App?

Add `keyBy()` if you need to:

1. **Track Claim History**
```java
claimsStream
    .keyBy(msg -> msg.get("claimId"))
    .process(new StatefulClaimsProcessor())  // Remember previous claim status
```

2. **Deduplicate Claims**
```java
claimsStream
    .keyBy(msg -> msg.get("claimId"))
    .process(new DeduplicationProcessor())  // Seen this claimId before?
```

3. **Aggregate Employee Leave Requests**
```java
leaveStream
    .keyBy(msg -> msg.get("employeeId"))
    .window(TumblingEventTimeWindows.of(Time.days(30)))
    .aggregate(new CountLeaveDaysAggregator())  // Total days per employee
```

4. **Enrich with Cached Data**
```java
claimsStream
    .keyBy(msg -> msg.get("customerId"))
    .process(new CustomerEnrichmentProcessor())  // Cache customer info per ID
```

---

#### Current Architecture Assessment ✅

Since the processors (`ClaimsProcessor`, `LeaveProcessor`) are doing **stateless 1:1 transformations** (JSON → Avro), `flatMap()` is the **right choice**:

```java
// Current implementation - CORRECT for the use case
claimsStream
    .flatMap(new ClaimsProcessor())  // Parse & emit Avro
    .sinkTo(claimsAvroSink);
```

You'd only add `keyBy()` if you later need:
- State (track claim status changes)
- Aggregation (sum claim amounts by customer)
- Deduplication (ignore duplicate claims)
- Windowing (hourly claim counts)

---

### Question 2: Would keyBy() be appropriate when a single JSON contains multiple Claim IDs with possible duplicates?

**Answer:** Yes, absolutely! But you need **both** `flatMap` and `keyBy` in sequence.

#### Scenario: JSON with Multiple Claims

```json
{
  "messageType": "CLAIM",
  "timestamp": "2025-11-22T10:30:00Z",
  "data": {
    "claims": [
      {"claimId": "CLM-001", "amount": 100.00, "status": "Approved"},
      {"claimId": "CLM-002", "amount": 250.00, "status": "Pending"},
      {"claimId": "CLM-001", "amount": 150.00, "status": "Updated"},  // DUPLICATE!
      {"claimId": "CLM-003", "amount": 300.00, "status": "Approved"}
    ]
  }
}
```

---

#### Solution: Two-Stage Processing

**Stage 1: Explode the JSON (flatMap)**

First, explode one JSON message into multiple individual claim records:

```java
claimsStream
    .flatMap(new ClaimExploderProcessor())  // 1 JSON → N Claim objects
    // Output: 4 separate Claim records (including duplicate CLM-001)
```

**ClaimExploderProcessor:**
```java
public void flatMap(KinesisMessage message, Collector<Claim> out) {
    JsonNode jsonNode = objectMapper.readTree(message.getMetadata());
    JsonNode claimsArray = jsonNode.get("data").get("claims");
    
    for (JsonNode claimNode : claimsArray) {
        Claim claim = Claim.newBuilder()
            .setClaimId(claimNode.get("claimId").asText())
            .setAmount(claimNode.get("amount").asDouble())
            .setStatus(claimNode.get("status").asText())
            .build();
        out.collect(claim);  // Emit each claim separately
    }
}
```

---

**Stage 2: Handle Duplicates (keyBy + deduplicate)**

**Option A: Keep Latest (Most Common)**
```java
claimsStream
    .flatMap(new ClaimExploderProcessor())  // 1 → N claims
    .keyBy(Claim::getClaimId)  // Group by claimId
    .process(new LatestClaimDeduplicator())  // Keep most recent per key
    .sinkTo(claimsAvroSink);
```

**LatestClaimDeduplicator:**
```java
public class LatestClaimDeduplicator extends KeyedProcessFunction<String, Claim, Claim> {
    private ValueState<Claim> latestClaimState;
    
    @Override
    public void open(Configuration params) {
        latestClaimState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("latestClaim", Claim.class));
    }
    
    @Override
    public void processElement(Claim claim, Context ctx, Collector<Claim> out) {
        Claim current = latestClaimState.value();
        
        // Keep the claim with latest timestamp or highest version
        if (current == null || isNewer(claim, current)) {
            latestClaimState.update(claim);
            out.collect(claim);  // Emit only if newer
        }
        // Otherwise discard duplicate
    }
}
```

---

**Option B: Keep All (Audit Trail)**

If you need to track all versions for auditing:

```java
claimsStream
    .flatMap(new ClaimExploderProcessor())
    .keyBy(Claim::getClaimId)
    .process(new ClaimVersionTracker())  // Add version number
    .sinkTo(claimsAvroSink);
```

**ClaimVersionTracker:**
```java
public class ClaimVersionTracker extends KeyedProcessFunction<String, Claim, Claim> {
    private ValueState<Integer> versionState;
    
    @Override
    public void processElement(Claim claim, Context ctx, Collector<Claim> out) {
        Integer version = versionState.value() == null ? 1 : versionState.value() + 1;
        versionState.update(version);
        
        // Add version to claim (requires schema update)
        Claim versionedClaim = Claim.newBuilder(claim)
            .setVersion(version)  // Add version field to schema
            .build();
        
        out.collect(versionedClaim);  // Emit with version number
    }
}
```

---

**Option C: Windowed Deduplication**

If duplicates only occur within a short time window:

```java
claimsStream
    .flatMap(new ClaimExploderProcessor())
    .keyBy(Claim::getClaimId)
    .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
    .reduce((claim1, claim2) -> claim1)  // Keep first in window
    .sinkTo(claimsAvroSink);
```

---

#### When to Add `keyBy()`

| Scenario | Need `keyBy()`? | Reason |
|----------|----------------|---------|
| **1 JSON = 1 Claim** (current) | ❌ No | Simple 1:1 transformation |
| **1 JSON = N Claims** (same ID possible) | ✅ Yes | Need deduplication per claimId |
| **Multiple JSONs, duplicate IDs across JSONs** | ✅ Yes | Need global deduplication |
| **Want to track claim state changes** | ✅ Yes | Need state per claimId |

---

#### Recommended Full Pattern for Multi-Claim Scenario

```java
// In StreamingApp.java
DataStream<KinesisMessage> claimsStream = mainStream.getSideOutput(MessageRouter.CLAIMS_TAG);

// Step 1: Explode JSON into individual claims (flatMap)
DataStream<Claim> individualClaims = claimsStream
    .flatMap(new ClaimExploderProcessor());

// Step 2: Deduplicate by claimId (keyBy)
DataStream<Claim> deduplicatedClaims = individualClaims
    .keyBy(Claim::getClaimId)
    .process(new LatestClaimDeduplicator());

// Step 3: Write to Avro
deduplicatedClaims.sinkTo(claimsAvroSink);
```

---

## Key Takeaways

1. **`flatMap()`** = Stateless transformation (1 → 0..N records)
2. **`keyBy()`** = Partition stream by key for stateful operations
3. **Pattern for multi-record JSONs**: `flatMap` (explode) → `keyBy` (partition) → stateful processing
4. **Current app uses `flatMap()` correctly** for 1:1 JSON → Avro transformation
5. **Add `keyBy()` when you need**: deduplication, aggregation, windowing, or any stateful operation per key

---

## Related Patterns

### Side Outputs Pattern (Currently Implemented)

The codebase uses Flink's Side Output pattern for stream splitting:

```java
// Define OutputTags
public static final OutputTag<KinesisMessage> CLAIMS_TAG = 
    new OutputTag<KinesisMessage>("claims-output") {};

// Route in ProcessFunction
switch (messageType) {
    case "CLAIM": ctx.output(CLAIMS_TAG, message); break;
    case "LEAVE_REQUEST": ctx.output(LEAVE_TAG, message); break;
}

// Extract side outputs
DataStream<KinesisMessage> claimsStream = mainStream.getSideOutput(CLAIMS_TAG);
DataStream<KinesisMessage> leaveStream = mainStream.getSideOutput(LEAVE_TAG);

// Process independently
claimsStream.flatMap(new ClaimsProcessor()).sinkTo(claimsAvroSink);
leaveStream.flatMap(new LeaveProcessor()).sinkTo(leaveAvroSink);
```

This is the **canonical Flink pattern** for multi-type message processing with independent pipelines.

---

*Document created: November 22, 2025*
