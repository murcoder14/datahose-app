# CI/CD Module - CodePipeline and CodeBuild

variable "app_name" {
  type = string
}

variable "region" {
  type = string
}

variable "account_id" {
  type = string
}

variable "streaming_app_bucket" {
  type = string
}

variable "codebuild_role_arn" {
  type = string
}

variable "codepipeline_role_arn" {
  type = string
}

variable "lambda_function_name" {
  type = string
}

variable "github_repo_owner" {
  type = string
}

variable "github_repo_name" {
  type = string
}

variable "github_branch" {
  type = string
}

variable "github_token_secret_arn" {
  type = string
}

# S3 Bucket for CI/CD Artifacts
resource "aws_s3_bucket" "artifacts" {
  bucket        = "${var.app_name}-cicd-artifacts-${var.account_id}"
  force_destroy = true # Automatically empty bucket before deletion

  tags = {
    Name        = "CI/CD Artifacts Bucket"
    Application = var.app_name
  }
}

resource "aws_s3_bucket_versioning" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# CodeBuild Project
resource "aws_codebuild_project" "build" {
  name          = "${var.app_name}-build"
  description   = "Build Flink application JAR"
  service_role  = var.codebuild_role_arn
  build_timeout = 30

  artifacts {
    type = "CODEPIPELINE"
  }

  environment {
    compute_type                = "BUILD_GENERAL1_SMALL"
    image                       = "aws/codebuild/standard:7.0"
    type                        = "LINUX_CONTAINER"
    image_pull_credentials_type = "CODEBUILD"

    environment_variable {
      name  = "STREAMING_APP_BUCKET"
      value = var.streaming_app_bucket
    }

    environment_variable {
      name  = "APP_NAME"
      value = var.app_name
    }

    environment_variable {
      name  = "AWS_REGION"
      value = var.region
    }
  }

  source {
    type      = "CODEPIPELINE"
    buildspec = "buildspec.yml"
  }

  logs_config {
    cloudwatch_logs {
      status     = "ENABLED"
      group_name = "/aws/codebuild/${var.app_name}-build"
    }
  }

  tags = {
    Name        = "Flink Build Project"
    Application = var.app_name
  }
}

# CodePipeline
resource "aws_codepipeline" "pipeline" {
  name     = "${var.app_name}-pipeline"
  role_arn = var.codepipeline_role_arn

  artifact_store {
    location = aws_s3_bucket.artifacts.bucket
    type     = "S3"
  }

  stage {
    name = "Source"

    action {
      name             = "Source"
      category         = "Source"
      owner            = "AWS"
      provider         = "CodeStarSourceConnection"
      version          = "1"
      output_artifacts = ["source_output"]

      configuration = {
        ConnectionArn    = var.github_token_secret_arn != "" ? var.github_token_secret_arn : null
        FullRepositoryId = "${var.github_repo_owner}/${var.github_repo_name}"
        BranchName       = var.github_branch
      }
    }
  }

  stage {
    name = "Build"

    action {
      name             = "Build"
      category         = "Build"
      owner            = "AWS"
      provider         = "CodeBuild"
      version          = "1"
      input_artifacts  = ["source_output"]
      output_artifacts = ["build_output"]

      configuration = {
        ProjectName = aws_codebuild_project.build.name
      }
    }
  }

  stage {
    name = "Deploy"

    action {
      name            = "DeployFlinkApp"
      category        = "Invoke"
      owner           = "AWS"
      provider        = "Lambda"
      version         = "1"
      input_artifacts = ["build_output"]

      configuration = {
        FunctionName = var.lambda_function_name
        UserParameters = jsonencode({
          action = "deploy"
        })
      }
    }
  }

  tags = {
    Name        = "Flink Deployment Pipeline"
    Application = var.app_name
  }
}

output "pipeline_name" {
  value = aws_codepipeline.pipeline.name
}

output "pipeline_arn" {
  value = aws_codepipeline.pipeline.arn
}

output "codebuild_project_name" {
  value = aws_codebuild_project.build.name
}

output "artifacts_bucket_name" {
  value = aws_s3_bucket.artifacts.id
}

output "artifacts_bucket_arn" {
  value = aws_s3_bucket.artifacts.arn
}
