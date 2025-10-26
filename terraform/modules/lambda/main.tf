# Lambda Module - Python Lambda for Flink Lifecycle Management

variable "app_name" {
  type = string
}

variable "region" {
  type = string
}

variable "account_id" {
  type = string
}

variable "flink_role_arn" {
  type = string
}

variable "streaming_app_bucket" {
  type = string
}

variable "output_data_bucket" {
  type = string
}

variable "output_table_name" {
  type = string
}

variable "kinesis_stream_name" {
  type = string
}

variable "kinesis_stream_arn" {
  type = string
}

variable "log_group_name" {
  type = string
}

variable "flink_version" {
  type = string
}

variable "flink_parallelism" {
  type = number
}

# Lambda function source code will be in ../../../lambda/flink_lifecycle.py
data "archive_file" "lambda" {
  type        = "zip"
  source_dir  = "${path.module}/../../../lambda"
  output_path = "${path.module}/lambda_package.zip"
}

resource "aws_lambda_function" "flink_lifecycle" {
  filename         = data.archive_file.lambda.output_path
  function_name    = "${var.app_name}-flink-lifecycle"
  role             = var.lambda_role_arn
  handler          = "flink_lifecycle.lambda_handler"
  source_code_hash = data.archive_file.lambda.output_base64sha256
  runtime          = "python3.13"
  timeout          = 300
  memory_size      = 256

  environment {
    variables = {
      APP_NAME             = var.app_name
      REGION               = var.region
      FLINK_ROLE_ARN       = var.flink_role_arn
      STREAMING_APP_BUCKET = var.streaming_app_bucket
      KINESIS_STREAM_NAME  = var.kinesis_stream_name
      KINESIS_STREAM_ARN   = var.kinesis_stream_arn
      OUTPUT_DATA_BUCKET   = var.output_data_bucket
      OUTPUT_TABLE_NAME    = var.output_table_name
      LOG_GROUP_NAME       = var.log_group_name
      FLINK_VERSION        = var.flink_version
      FLINK_PARALLELISM    = tostring(var.flink_parallelism)
    }
  }

  tags = {
    Name        = "Flink Lifecycle Management"
    Application = var.app_name
  }
}

# CloudWatch log group for Lambda
resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${aws_lambda_function.flink_lifecycle.function_name}"
  retention_in_days = 7

  tags = {
    Name        = "Lambda Logs"
    Application = var.app_name
  }
}

variable "lambda_role_arn" {
  type = string
}

output "function_name" {
  value = aws_lambda_function.flink_lifecycle.function_name
}

output "function_arn" {
  value = aws_lambda_function.flink_lifecycle.arn
}

output "invoke_arn" {
  value = aws_lambda_function.flink_lifecycle.invoke_arn
}
