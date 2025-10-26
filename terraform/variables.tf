variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-2"
}

variable "app_name" {
  description = "Application name (used for resource naming)"
  type        = string
  default     = "datahose-app"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "bucket_suffix" {
  description = "Unique suffix for S3 bucket names (date-epoch or custom)"
  type        = string
  default     = ""
}

variable "output_table_name" {
  description = "S3 output table/folder name"
  type        = string
  default     = "datalake"
}

# Kinesis Configuration
variable "kinesis_shard_count" {
  description = "Number of shards for Kinesis Data Stream"
  type        = number
  default     = 1
}

variable "kinesis_retention_period" {
  description = "Data retention period in hours for Kinesis stream (24-8760)"
  type        = number
  default     = 24
}

variable "flink_version" {
  description = "Flink runtime version"
  type        = string
  default     = "FLINK-1_20"
}

variable "flink_parallelism" {
  description = "Flink parallelism configuration"
  type        = number
  default     = 1
}

variable "cloudwatch_log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 7
}

variable "tags" {
  description = "Common tags to apply to all resources"
  type        = map(string)
  default = {
    Project   = "DataHose"
    ManagedBy = "Terraform"
  }
}

variable "github_repo_owner" {
  description = "GitHub repository owner (for CodePipeline source)"
  type        = string
  default     = "murcoder14"
}

variable "github_repo_name" {
  description = "GitHub repository name"
  type        = string
  default     = "datahose-app"
}

variable "github_branch" {
  description = "GitHub branch to monitor"
  type        = string
  default     = "main"
}

variable "github_token_secret_arn" {
  description = "ARN of Secrets Manager secret containing GitHub personal access token"
  type        = string
  default     = ""
}
