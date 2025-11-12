# Understanding the Three YAML Files

This document explains why we have three separate YAML files and the role each one plays in the AWS-native deployment architecture.

## Overview

```
datahose-app/
├── buildspec.yml              ← "HOW to build and deploy"
└── awsnative/
    ├── infrastructure.yaml    ← "WHAT infrastructure to create"
    └── cicd-pipeline.yaml     ← "WHAT automation to create"
```

---

## 1. **`buildspec.yml`** (at project root)

**Purpose**: Build instructions for CodeBuild  
**Used by**: AWS CodeBuild  
**When**: Every time the pipeline runs

### What it does:
- Tells CodeBuild **how to compile** your Java application
- Runs Maven to build the JAR
- Uploads JAR to S3
- Creates/updates the Flink application
- Starts the streaming application

### Key Sections:
```yaml
phases:
  install:     # Install Java, Maven
  pre_build:   # Load configuration, verify AWS access
  build:       # Run Maven, create JAR
  post_build:  # Upload JAR, deploy Flink app
```

**Analogy**: It's like a **recipe** - step-by-step instructions for building your application.

**Example commands:**
```bash
mvn clean package -DskipTests
aws s3 cp target/datahose-app.jar s3://$STREAMING_APP_BUCKET/
aws kinesisanalyticsv2 create-application --application-name datahose-app ...
```

---

## 2. **`awsnative/infrastructure.yaml`** (CloudFormation template)

**Purpose**: Infrastructure as Code - defines AWS resources  
**Used by**: AWS CloudFormation  
**When**: One time (or when you update infrastructure)

### What it creates:
- ✅ **S3 Buckets**
  - Application JAR storage bucket (versioned)
  - Data sink bucket for Flink output (versioned)
- ✅ **Kinesis Data Stream**
  - For streaming data input
- ✅ **IAM Roles and Policies**
  - Flink service role
  - Kinesis producer policy
- ✅ **CloudWatch Resources**
  - Log groups for application monitoring
  - Log streams with retention policies

### Key Resources:
```yaml
Resources:
  StreamingAppBucket:      # S3 for JAR files
  DataBucket:              # S3 for output data
  KinesisDataStream:       # Kinesis stream
  FlinkServiceRole:        # IAM role for Flink
  FlinkServicePolicy:      # IAM permissions
  FlinkLogGroup:           # CloudWatch logs
```

**Analogy**: It's like a **blueprint** for your house - defines what rooms (resources) you need.

**Deployment:**
```bash
cd awsnative
./deploy-infrastructure.sh
```

---

## 3. **`awsnative/cicd-pipeline.yaml`** (CloudFormation template)

**Purpose**: CI/CD pipeline infrastructure  
**Used by**: AWS CloudFormation  
**When**: One time (creates the automation pipeline)

### What it creates:
- ✅ **CodePipeline**
  - Orchestrates the build and deployment process
  - Connects GitHub to CodeBuild
- ✅ **CodeBuild Project**
  - Build environment configuration
  - Uses `buildspec.yml` for build instructions
- ✅ **S3 Artifact Bucket**
  - Stores pipeline artifacts (source code, build outputs)
- ✅ **IAM Roles**
  - CodePipeline service role
  - CodeBuild service role
- ✅ **GitHub Webhook**
  - Automatically triggers pipeline on code push

### Key Resources:
```yaml
Resources:
  ArtifactBucket:           # S3 for pipeline artifacts
  CodeBuildProject:         # Build environment
  Pipeline:                 # CodePipeline orchestration
  GitHubWebhook:            # Auto-trigger on push
  CodeBuildServiceRole:     # IAM for CodeBuild
  CodePipelineServiceRole:  # IAM for CodePipeline
```

**Analogy**: It's like a **factory assembly line** - automates the process of building and deploying.

**Deployment:**
```bash
cd awsnative
./deploy-pipeline.sh
```

---

## Deployment Flow

```
┌─────────────────────────────────────────────────────────┐
│                    DEPLOYMENT FLOW                       │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  1️⃣  infrastructure.yaml (CloudFormation)               │
│      ↓                                                   │
│      Creates: S3, Kinesis, IAM, CloudWatch              │
│      Status: One-time setup                              │
│                                                          │
│  2️⃣  cicd-pipeline.yaml (CloudFormation)                │
│      ↓                                                   │
│      Creates: CodePipeline, CodeBuild, Webhooks         │
│      Status: One-time setup                              │
│                                                          │
│  3️⃣  buildspec.yml (used by CodeBuild)                  │
│      ↓                                                   │
│      Runs: Maven build, JAR upload, Flink deployment    │
│      Status: Runs on every code push                     │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

---

## Why Three Separate Files?

### Separation of Concerns

Each file has a distinct purpose and lifecycle:

| File | Purpose | Changes How Often? | Managed By |
|------|---------|-------------------|------------|
| `infrastructure.yaml` | Define AWS resources | Rarely (when adding resources) | CloudFormation |
| `cicd-pipeline.yaml` | Define CI/CD automation | Rarely (when changing pipeline) | CloudFormation |
| `buildspec.yml` | Define build steps | Occasionally (when changing build) | Git/CodeBuild |

### Different Update Patterns

**Infrastructure Changes** (Rare)
- Adding a new S3 bucket
- Increasing Kinesis shard count
- Changing IAM permissions
- ➡️ Update `infrastructure.yaml`, run `./deploy-infrastructure.sh`

**Pipeline Changes** (Rare)
- Changing build compute size
- Adding new pipeline stages
- Modifying GitHub integration
- ➡️ Update `cicd-pipeline.yaml`, run `./deploy-pipeline.sh`

**Build Logic Changes** (Frequent)
- Changing Maven goals
- Adding build steps
- Modifying Flink configuration
- ➡️ Update `buildspec.yml`, commit, and push (automatic deployment)

---

## Real-World Workflow

### Initial Setup (One Time)

```bash
# Step 1: Deploy infrastructure
cd awsnative
./deploy-infrastructure.sh
# Creates: S3 buckets, Kinesis stream, IAM roles, CloudWatch logs

# Step 2: Create GitHub token secret
aws secretsmanager create-secret \
  --name github/personal-access-token \
  --secret-string '{"token":"YOUR_TOKEN"}' \
  --region us-east-2

# Step 3: Deploy CI/CD pipeline
./deploy-pipeline.sh
# Creates: CodePipeline, CodeBuild, GitHub webhook
```

### Daily Development (Automatic)

```bash
# Make code changes
vim src/main/java/org/muralis/datahose/StreamingApp.java

# Commit and push
git add .
git commit -m "Add new feature"
git push origin feature/kinesis-streaming

# Pipeline automatically:
# 1. Detects the push (via GitHub webhook)
# 2. Downloads code (including buildspec.yml)
# 3. Runs buildspec.yml in CodeBuild
# 4. Builds JAR with Maven
# 5. Deploys to Flink
# 6. Starts the application
```

---

## Could We Combine Them?

**Technically yes, but it's a bad idea. Here's why:**

### ❌ Don't combine infrastructure.yaml + cicd-pipeline.yaml

Even though both use CloudFormation, they should remain separate due to:

#### 1. **Different Lifecycles**

```
Infrastructure Stack:
├─ Created: Once at project start
├─ Updated: Rarely (resource changes)
├─ Lifetime: Entire project duration
└─ Deletion: ⚠️  DATA LOSS

Pipeline Stack:
├─ Created: Once after infrastructure
├─ Updated: Occasionally (build config)
├─ Lifetime: Can be recreated anytime
└─ Deletion: ✅ Safe (no data loss)
```

#### 2. **Blast Radius Control**

When you update a CloudFormation stack, changes can have cascading effects:

```bash
# ❌ Combined Stack (Risky):
aws cloudformation update-stack --stack-name combined-stack \
  --parameters ParameterKey=BuildInstanceType,ParameterValue=BUILD_GENERAL1_MEDIUM

# What happens:
# 1. CloudFormation evaluates ALL resources
# 2. Checks dependencies between resources
# 3. Might replace S3 buckets if dependencies exist
# 4. ⚠️  Potential data loss!
# 5. ⚠️  Application downtime!

# ✅ Separate Stacks (Safe):
aws cloudformation update-stack --stack-name datahose-app-pipeline \
  --parameters ParameterKey=BuildInstanceType,ParameterValue=BUILD_GENERAL1_MEDIUM

# What happens:
# 1. CloudFormation evaluates ONLY pipeline resources
# 2. Infrastructure stack completely untouched
# 3. ✅ Zero risk to data
# 4. ✅ Minimal downtime
```

#### 3. **Independent Deployment Scenarios**

**Scenario A: No CI/CD Needed**
```bash
# Deploy infrastructure only, use manual deployment
./deploy-infrastructure.sh
# Skip pipeline deployment
# Result: Infrastructure ready, manual JAR uploads
```

**Scenario B: Multiple Environments**
```bash
# Production: Full automation
./deploy-infrastructure.sh  # Prod resources
./deploy-pipeline.sh        # Prod pipeline

# Development: Manual deployment
./deploy-infrastructure.sh  # Dev resources
# No pipeline needed
```

**Scenario C: Pipeline Troubleshooting**
```bash
# Something wrong with pipeline? Delete and recreate!
./destroy-pipeline.sh       # Safe - no data impact
# Fix configuration
./deploy-pipeline.sh        # Recreate with fixes
# Infrastructure never affected!
```

#### 4. **Dependency Direction**

```
infrastructure.yaml
    ├─ Exports: StreamingAppBucket
    ├─ Exports: DataBucket  
    ├─ Exports: KinesisDataStream
    └─ Exports: FlinkServiceRoleArn
         ↓
         ↓ (Pipeline imports these)
         ↓
cicd-pipeline.yaml
    ├─ Imports: !ImportValue StreamingAppBucket
    ├─ Uses bucket for artifacts
    └─ Pipeline depends ON infrastructure

Key Point: Pipeline needs infrastructure, but infrastructure 
          doesn't need pipeline. One-way dependency!
```

#### 5. **Real-World Danger Example**

**Combined stack problems:**
```yaml
# ❌ BAD: Combined template
Resources:
  # Infrastructure (critical, persistent data)
  StreamingAppBucket:
    Type: AWS::S3::Bucket
    DeletionPolicy: Retain  # Prevent accidental deletion
    Properties:
      VersioningConfiguration:
        Status: Enabled
  
  # Pipeline (configuration, no data)
  CodeBuildProject:
    Type: AWS::CodeBuild::Project
    Properties:
      Environment:
        ComputeType: BUILD_GENERAL1_SMALL
      Artifacts:
        Type: S3
        Location: !Ref StreamingAppBucket  # ⚠️  Creates dependency!

# Problem: Changing CodeBuildProject properties might trigger
#          StreamingAppBucket replacement due to dependencies!
```

**Separate stacks solution:**
```yaml
# ✅ GOOD: infrastructure.yaml (isolated)
Resources:
  StreamingAppBucket:
    Type: AWS::S3::Bucket
    DeletionPolicy: Retain
    Properties:
      VersioningConfiguration:
        Status: Enabled

Outputs:
  StreamingAppBucketName:
    Value: !Ref StreamingAppBucket
    Export:
      Name: !Sub "${AWS::StackName}-StreamingAppBucket"

---
# ✅ GOOD: cicd-pipeline.yaml (isolated)
Resources:
  CodeBuildProject:
    Type: AWS::CodeBuild::Project
    Properties:
      Environment:
        ComputeType: BUILD_GENERAL1_SMALL
      Artifacts:
        Type: S3
        Location: !ImportValue datahose-app-infrastructure-StreamingAppBucket

# Benefit: No direct dependency! Import is reference-only.
#          Updating pipeline can't affect infrastructure.
```

#### 6. **Update Frequency Mismatch**

```
Infrastructure Updates (Rare):
├─ Add new S3 bucket (once a year?)
├─ Increase Kinesis shards (quarterly?)
├─ Update IAM permissions (as needed)
└─ Impact: HIGH (production data)

Pipeline Updates (Occasional):
├─ Change build instance size (monthly?)
├─ Update GitHub branch (per feature)
├─ Modify build timeout (as needed)
├─ Change Docker image version (regularly)
└─ Impact: LOW (rebuild automation)

Result: Different change velocities mean different 
        risk profiles. Keep them separate!
```

**Example scenario:**
```
❌ Combined Stack:
   Week 1: Update pipeline settings
   Week 2: CloudFormation drift detected
   Week 3: Stack update fails
   Week 4: Emergency rollback needed
   Result: S3 buckets accidentally deleted
          All historical data lost! 💥

✅ Separate Stacks:
   Week 1: Update pipeline settings
   Week 2: Pipeline stack updated successfully
   Week 3: Infrastructure untouched
   Week 4: Data safe, application running
   Result: Zero risk to data! ✅
```

### ❌ Don't put buildspec.yml into CloudFormation

**Problems:**
- Build steps change frequently (CloudFormation updates are slower)
- Can't version control build logic separately
- Harder to test build changes
- CloudFormation doesn't support dynamic build instructions
- Can't leverage CodeBuild's caching and optimization

---

## When SHOULD You Combine CloudFormation Stacks?

While keeping infrastructure and pipeline separate is recommended for this project, there ARE valid scenarios for combining CloudFormation templates:

### ✅ Combine CloudFormation stacks when:

1. **Prototype/Demo Projects**
   - Short-lived infrastructure (< 1 month)
   - No production data
   - Single deployment, no updates expected
   - Easy to destroy and recreate

2. **Tightly Coupled Resources**
   - Resources that must be created/deleted together
   - Circular dependencies that can't be broken
   - Resources that share the same lifecycle
   - Example: Lambda function + API Gateway + DynamoDB table for single microservice

3. **Simple Applications**
   - < 10 resources total
   - All resources change together
   - No data persistence requirements
   - Single-purpose stack

4. **Learning/Tutorial Projects**
   - Educational purposes
   - Simplicity more important than best practices
   - Easy to understand as single unit

### ❌ Keep CloudFormation stacks separate when:

1. **Production Systems** (Your case!)
   - Data persistence matters
   - Different update frequencies
   - Multiple teams managing different parts
   - Need to minimize blast radius

2. **Long-Term Projects**
   - Infrastructure stable, automation evolves
   - Need independent update cycles
   - Risk mitigation is important

3. **Multi-Environment Deployments**
   - Same infrastructure, different pipeline configs
   - Optional components (pipeline might not be needed everywhere)

4. **Enterprise Requirements**
   - Compliance needs separation of concerns
   - Different approval workflows
   - Different teams own different stacks

---

## Your Specific Case: Why Separate is Better

```
Infrastructure Stack (datahose-app-infrastructure):
✓ S3 buckets with versioning (stores JAR files, output data)
✓ Kinesis stream (processes real-time data)
✓ IAM roles (grants Flink permissions)
✓ CloudWatch logs (captures metrics)
→ Update frequency: RARE (maybe quarterly)
→ Data risk: HIGH (deletion = data loss)
→ Owner: Platform/DevOps team
→ Lifetime: Entire project duration

Pipeline Stack (datahose-app-pipeline):
✓ CodeBuild project (build configuration)
✓ CodePipeline (orchestration)
✓ GitHub webhook (automation trigger)
✓ Artifact bucket (temporary build outputs)
→ Update frequency: OCCASIONAL (monthly)
→ Data risk: LOW (can recreate anytime)
→ Owner: Development team
→ Lifetime: Can be destroyed/recreated safely

Conclusion: Different lifecycles + different owners + 
           different risk profiles = KEEP SEPARATE! ✅
```

---

## Analogy: Building a House

Think of the three files like this:

```
🏗️  infrastructure.yaml  = Building your house
    (Foundation, walls, plumbing, electrical)
    Changes: Rarely (major renovations)

🏭  cicd-pipeline.yaml   = Building the factory that makes furniture
    (Assembly line, tools, quality control)
    Changes: Rarely (factory upgrades)

📋  buildspec.yml        = Instructions for making each piece of furniture
    (Step-by-step build process)
    Changes: Often (new designs, improvements)
```

You need:
- **The house** (infrastructure) to live in
- **The factory** (pipeline) to automate production
- **The instructions** (buildspec) to tell the factory what to do

Each serves a distinct purpose and has a different lifecycle!

---

## File Dependencies

```
infrastructure.yaml
    ↓
    Creates: Buckets, Streams, Roles
    Exports: Bucket names, ARNs, etc.
    ↓
cicd-pipeline.yaml
    ↓
    Imports: Infrastructure outputs
    Creates: Pipeline, CodeBuild
    References: buildspec.yml location
    ↓
buildspec.yml
    ↓
    Uses: Infrastructure resources
    Triggered by: Pipeline
    Deploys to: Flink application
```

---

## Summary

### When to Update Each File

| Scenario | File to Update | Action |
|----------|---------------|--------|
| Need a new S3 bucket | `infrastructure.yaml` | `./deploy-infrastructure.sh` |
| Change Kinesis shards | `infrastructure.yaml` | `./deploy-infrastructure.sh` |
| Update IAM permissions | `infrastructure.yaml` | `./deploy-infrastructure.sh` |
| Change build instance size | `cicd-pipeline.yaml` | `./deploy-pipeline.sh` |
| Switch GitHub branch | `cicd-pipeline.yaml` | `./deploy-pipeline.sh` |
| Change Maven build steps | `buildspec.yml` | Commit and push |
| Update Flink configuration | `buildspec.yml` | Commit and push |
| Change Java version | `buildspec.yml` | Commit and push |

### Key Takeaways

1. **`infrastructure.yaml`** = What infrastructure you need (S3, Kinesis, IAM)
2. **`cicd-pipeline.yaml`** = What automation you need (Pipeline, CodeBuild)
3. **`buildspec.yml`** = How to build and deploy your application

All three work together to provide a complete, automated deployment solution! 🚀

---

## Additional Resources

- [Infrastructure Documentation](./AWSNATIVE.md)
- [Deploy Infrastructure](./deploy-infrastructure.sh)
- [Deploy Pipeline](./deploy-pipeline.sh)
- [Destroy Infrastructure](./destroy-infrastructure.sh)
- [Destroy Pipeline](./destroy-pipeline.sh)
