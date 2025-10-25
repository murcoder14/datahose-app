terraform {
  required_version = ">= 1.0"
  
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Optional: Configure S3 backend for state management
  # backend "s3" {
  #   bucket = "your-terraform-state-bucket"
  #   key    = "datahose-app/terraform.tfstate"
  #   region = "us-east-2"
  # }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = var.tags
  }
}

# Generate unique bucket suffix if not provided
resource "random_id" "bucket_suffix" {
  count       = var.bucket_suffix == "" ? 1 : 0
  byte_length = 4
}

locals {
  bucket_suffix         = var.bucket_suffix != "" ? var.bucket_suffix : "${formatdate("YYYYMMDD", timestamp())}-${random_id.bucket_suffix[0].hex}"
  streaming_app_bucket  = "tm-streaming-app-bucket-${local.bucket_suffix}"
  input_data_bucket     = "tm-input-data-bucket-${local.bucket_suffix}"
  output_data_bucket    = "tm-output-data-bucket-${local.bucket_suffix}"
  log_group_name        = "/aws/kinesis-analytics/${var.app_name}"
}

# Data source for current AWS account and caller identity
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# Module: S3 Buckets
module "s3_buckets" {
  source = "./modules/s3"

  app_name              = var.app_name
  streaming_app_bucket  = local.streaming_app_bucket
  input_data_bucket     = local.input_data_bucket
  output_data_bucket    = local.output_data_bucket
  input_table_name      = var.input_table_name
  output_table_name     = var.output_table_name
}

# Module: IAM Roles and Policies
module "iam" {
  source = "./modules/iam"

  app_name             = var.app_name
  account_id           = data.aws_caller_identity.current.account_id
  region               = data.aws_region.current.name
  streaming_app_bucket = module.s3_buckets.streaming_app_bucket_name
  input_data_bucket    = module.s3_buckets.input_data_bucket_name
  output_data_bucket   = module.s3_buckets.output_data_bucket_name
  log_group_name       = local.log_group_name
  codebuild_bucket     = module.cicd.artifacts_bucket_name
}

# Module: CloudWatch Logs
module "cloudwatch" {
  source = "./modules/cloudwatch"

  app_name          = var.app_name
  log_group_name    = local.log_group_name
  retention_in_days = var.cloudwatch_log_retention_days
}

# Module: Lambda for Flink Lifecycle Management
module "lambda" {
  source = "./modules/lambda"

  app_name               = var.app_name
  region                 = data.aws_region.current.name
  account_id             = data.aws_caller_identity.current.account_id
  lambda_role_arn        = module.iam.lambda_role_arn
  flink_role_arn         = module.iam.flink_role_arn
  streaming_app_bucket   = module.s3_buckets.streaming_app_bucket_name
  input_data_bucket      = module.s3_buckets.input_data_bucket_name
  output_data_bucket     = module.s3_buckets.output_data_bucket_name
  input_table_name       = var.input_table_name
  output_table_name      = var.output_table_name
  log_group_name         = local.log_group_name
  flink_version          = var.flink_version
  flink_parallelism      = var.flink_parallelism
}

# Module: CI/CD Pipeline (CodePipeline + CodeBuild)
module "cicd" {
  source = "./modules/cicd"

  app_name                 = var.app_name
  region                   = data.aws_region.current.name
  account_id               = data.aws_caller_identity.current.account_id
  streaming_app_bucket     = module.s3_buckets.streaming_app_bucket_name
  codebuild_role_arn       = module.iam.codebuild_role_arn
  codepipeline_role_arn    = module.iam.codepipeline_role_arn
  lambda_function_name     = module.lambda.function_name
  github_repo_owner        = var.github_repo_owner
  github_repo_name         = var.github_repo_name
  github_branch            = var.github_branch
  github_token_secret_arn  = var.github_token_secret_arn
}
