# IAM Module - Creates roles and policies for Flink, Lambda, CodeBuild, and CodePipeline

variable "app_name" {
  type = string
}

variable "account_id" {
  type = string
}

variable "region" {
  type = string
}

variable "streaming_app_bucket" {
  type = string
}

variable "output_data_bucket" {
  type = string
}

variable "kinesis_stream_arn" {
  type = string
}

variable "log_group_name" {
  type = string
}

# IAM Role for Flink Application
resource "aws_iam_role" "flink" {
  name = "${var.app_name}-flink-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "kinesisanalytics.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Name        = "Flink Execution Role"
    Application = var.app_name
  }
}

# IAM Policy for Flink Application
resource "aws_iam_policy" "flink" {
  name        = "${var.app_name}-flink-policy"
  description = "Policy for Managed Service for Apache Flink - ${var.app_name}"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ReadApplicationJAR"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion"
        ]
        Resource = [
          "arn:aws:s3:::${var.streaming_app_bucket}/*"
        ]
      },
      {
        Sid    = "ListApplicationBucket"
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          "arn:aws:s3:::${var.streaming_app_bucket}"
        ]
      },
      {
        Sid    = "ReadFromKinesisDataStream"
        Effect = "Allow"
        Action = [
          "kinesis:DescribeStream",
          "kinesis:DescribeStreamSummary",
          "kinesis:GetShardIterator",
          "kinesis:GetRecords",
          "kinesis:ListShards"
        ]
        Resource = [
          var.kinesis_stream_arn
        ]
      },
      {
        Sid    = "WriteToOutputDataBucket"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          "arn:aws:s3:::${var.output_data_bucket}",
          "arn:aws:s3:::${var.output_data_bucket}/*"
        ]
      },
      {
        Sid    = "CloudWatchLogsAccess"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogGroups",
          "logs:DescribeLogStreams"
        ]
        Resource = [
          "arn:aws:logs:${var.region}:${var.account_id}:log-group:${var.log_group_name}",
          "arn:aws:logs:${var.region}:${var.account_id}:log-group:${var.log_group_name}:*"
        ]
      },
      {
        Sid    = "CloudWatchMetricsAccess"
        Effect = "Allow"
        Action = [
          "cloudwatch:PutMetricData"
        ]
        Resource = "*"
      },
      {
        Sid    = "VPCAccess"
        Effect = "Allow"
        Action = [
          "ec2:DescribeVpcs",
          "ec2:DescribeSubnets",
          "ec2:DescribeSecurityGroups",
          "ec2:DescribeDhcpOptions",
          "ec2:CreateNetworkInterface",
          "ec2:CreateNetworkInterfacePermission",
          "ec2:DescribeNetworkInterfaces",
          "ec2:DeleteNetworkInterface"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "flink" {
  role       = aws_iam_role.flink.name
  policy_arn = aws_iam_policy.flink.arn
}

# IAM Role for Lambda (Flink Lifecycle Management)
resource "aws_iam_role" "lambda" {
  name = "${var.app_name}-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Name        = "Lambda Flink Lifecycle Management Role"
    Application = var.app_name
  }
}

# IAM Policy for Lambda
resource "aws_iam_policy" "lambda" {
  name        = "${var.app_name}-lambda-policy"
  description = "Policy for Lambda to manage Flink application lifecycle"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ManageFlinkApplication"
        Effect = "Allow"
        Action = [
          "kinesisanalytics:DescribeApplication",
          "kinesisanalytics:CreateApplication",
          "kinesisanalytics:UpdateApplication",
          "kinesisanalytics:DeleteApplication",
          "kinesisanalytics:StartApplication",
          "kinesisanalytics:StopApplication",
          "kinesisanalytics:AddApplicationCloudWatchLoggingOption",
          "kinesisanalytics:DeleteApplicationCloudWatchLoggingOption",
          "kinesisanalytics:ListApplications",
          "kinesisanalytics:ListTagsForResource",
          "kinesisanalytics:TagResource"
        ]
        Resource = [
          "arn:aws:kinesisanalytics:${var.region}:${var.account_id}:application/${var.app_name}"
        ]
      },
      {
        Sid    = "PassFlinkRole"
        Effect = "Allow"
        Action = [
          "iam:PassRole"
        ]
        Resource = aws_iam_role.flink.arn
        Condition = {
          StringEquals = {
            "iam:PassedToService" = "kinesisanalytics.amazonaws.com"
          }
        }
      },
      {
        Sid    = "ReadS3JAR"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:HeadObject"
        ]
        Resource = [
          "arn:aws:s3:::${var.streaming_app_bucket}/*"
        ]
      },
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogStreams"
        ]
        Resource = [
          "arn:aws:logs:${var.region}:${var.account_id}:log-group:/aws/lambda/${var.app_name}-*",
          "arn:aws:logs:${var.region}:${var.account_id}:log-group:${var.log_group_name}:*"
        ]
      },
      {
        Sid    = "CodePipelineIntegration"
        Effect = "Allow"
        Action = [
          "codepipeline:PutJobSuccessResult",
          "codepipeline:PutJobFailureResult"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy_attachment" "lambda_custom" {
  role       = aws_iam_role.lambda.name
  policy_arn = aws_iam_policy.lambda.arn
}

# IAM Role for CodeBuild
resource "aws_iam_role" "codebuild" {
  name = "${var.app_name}-codebuild-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "codebuild.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Name        = "CodeBuild Role"
    Application = var.app_name
  }
}

# IAM Policy for CodeBuild
resource "aws_iam_policy" "codebuild" {
  name        = "${var.app_name}-codebuild-policy"
  description = "Policy for CodeBuild to build and upload JAR"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "CloudWatchLogsAccess"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = [
          "arn:aws:logs:${var.region}:${var.account_id}:log-group:/aws/codebuild/${var.app_name}-*"
        ]
      },
      {
        Sid    = "S3ArtifactsAccess"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:GetObjectVersion"
        ]
        Resource = [
          "arn:aws:s3:::${var.app_name}-cicd-artifacts-${var.account_id}/*"
        ]
      },
      {
        Sid    = "S3ArtifactsBucketList"
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation",
          "s3:GetBucketVersioning"
        ]
        Resource = [
          "arn:aws:s3:::${var.app_name}-cicd-artifacts-${var.account_id}"
        ]
      },
      {
        Sid    = "S3StreamingAppAccess"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:GetObjectVersion"
        ]
        Resource = [
          "arn:aws:s3:::${var.streaming_app_bucket}/*"
        ]
      },
      {
        Sid    = "CodeBuildReports"
        Effect = "Allow"
        Action = [
          "codebuild:CreateReportGroup",
          "codebuild:CreateReport",
          "codebuild:UpdateReport",
          "codebuild:BatchPutTestCases",
          "codebuild:BatchPutCodeCoverages"
        ]
        Resource = [
          "arn:aws:codebuild:${var.region}:${var.account_id}:report-group/${var.app_name}-*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "codebuild" {
  role       = aws_iam_role.codebuild.name
  policy_arn = aws_iam_policy.codebuild.arn
}

# IAM Role for CodePipeline
resource "aws_iam_role" "codepipeline" {
  name = "${var.app_name}-codepipeline-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "codepipeline.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Name        = "CodePipeline Role"
    Application = var.app_name
  }
}

# IAM Policy for CodePipeline
resource "aws_iam_policy" "codepipeline" {
  name        = "${var.app_name}-codepipeline-policy"
  description = "Policy for CodePipeline orchestration"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3Access"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:PutObject",
          "s3:GetBucketLocation",
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::${var.app_name}-cicd-artifacts-${var.account_id}",
          "arn:aws:s3:::${var.app_name}-cicd-artifacts-${var.account_id}/*"
        ]
      },
      {
        Sid    = "CodeBuildAccess"
        Effect = "Allow"
        Action = [
          "codebuild:BatchGetBuilds",
          "codebuild:StartBuild"
        ]
        Resource = [
          "arn:aws:codebuild:${var.region}:${var.account_id}:project/${var.app_name}-build"
        ]
      },
      {
        Sid    = "LambdaInvoke"
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction",
          "lambda:GetFunction"
        ]
        Resource = [
          "arn:aws:lambda:${var.region}:${var.account_id}:function:${var.app_name}-*"
        ]
      },
      {
        Sid    = "CodeStarConnections"
        Effect = "Allow"
        Action = [
          "codestar-connections:UseConnection"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "codepipeline" {
  role       = aws_iam_role.codepipeline.name
  policy_arn = aws_iam_policy.codepipeline.arn
}

output "flink_role_arn" {
  value = aws_iam_role.flink.arn
}

output "flink_role_name" {
  value = aws_iam_role.flink.name
}

output "lambda_role_arn" {
  value = aws_iam_role.lambda.arn
}

output "lambda_role_name" {
  value = aws_iam_role.lambda.name
}

output "codebuild_role_arn" {
  value = aws_iam_role.codebuild.arn
}

output "codepipeline_role_arn" {
  value = aws_iam_role.codepipeline.arn
}
