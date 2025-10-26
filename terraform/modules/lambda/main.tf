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

variable "cloudwatch_log_retention_days" {
  type    = number
  default = 7
}

# Lambda function source code will be in ../../../lambda/flink_lifecycle.py
data "archive_file" "lambda" {
  type        = "zip"
  source_dir  = "${path.module}/../../../lambda"
  output_path = "${path.module}/lambda_package.zip"
}

# CloudWatch log group for Lambda (must be created BEFORE Lambda function)
resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${var.app_name}-flink-lifecycle"
  retention_in_days = var.cloudwatch_log_retention_days
  skip_destroy      = false # Ensure log group is deleted on terraform destroy

  tags = {
    Name        = "Lambda Logs"
    Application = var.app_name
  }
}

# CloudWatch log group for Flink application
resource "aws_cloudwatch_log_group" "flink" {
  name              = var.log_group_name
  retention_in_days = var.cloudwatch_log_retention_days
  skip_destroy      = false # Ensure log group is deleted on terraform destroy

  tags = {
    Name        = "Flink Application Logs"
    Application = var.app_name
  }
}

# CloudWatch log stream for Flink application
resource "aws_cloudwatch_log_stream" "flink" {
  name           = "flink-application"
  log_group_name = aws_cloudwatch_log_group.flink.name
}

# Lambda function
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

  depends_on = [aws_cloudwatch_log_group.lambda]

  # Destroy provisioner: Clean up Flink application before destroying Lambda
  provisioner "local-exec" {
    when    = destroy
    command = <<-EOT
      echo "Stopping and deleting Flink application ${self.environment[0].variables.APP_NAME}..."
      
      # Check if application exists
      if aws kinesisanalyticsv2 describe-application \
        --application-name ${self.environment[0].variables.APP_NAME} \
        --region ${self.environment[0].variables.REGION} 2>/dev/null; then
        
        echo "Application found. Stopping if running..."
        
        # Get current status
        STATUS=$(aws kinesisanalyticsv2 describe-application \
          --application-name ${self.environment[0].variables.APP_NAME} \
          --region ${self.environment[0].variables.REGION} \
          --query 'ApplicationDetail.ApplicationStatus' \
          --output text)
        
        echo "Current status: $STATUS"
        
        # Stop if running
        if [ "$STATUS" = "RUNNING" ]; then
          echo "Stopping application without snapshot (force stop)..."
          aws kinesisanalyticsv2 stop-application \
            --application-name ${self.environment[0].variables.APP_NAME} \
            --region ${self.environment[0].variables.REGION} \
            --force || true
          
          echo "Waiting for application to stop..."
          for i in {1..30}; do
            STATUS=$(aws kinesisanalyticsv2 describe-application \
              --application-name ${self.environment[0].variables.APP_NAME} \
              --region ${self.environment[0].variables.REGION} \
              --query 'ApplicationDetail.ApplicationStatus' \
              --output text 2>/dev/null || echo "DELETED")
            
            if [ "$STATUS" = "READY" ]; then
              echo "Application stopped."
              break
            fi
            
            echo "Waiting... ($i/30) Status: $STATUS"
            sleep 10
          done
        fi
        
        # Delete the application
        echo "Deleting application..."
        aws kinesisanalyticsv2 delete-application \
          --application-name ${self.environment[0].variables.APP_NAME} \
          --create-timestamp "$(aws kinesisanalyticsv2 describe-application \
            --application-name ${self.environment[0].variables.APP_NAME} \
            --region ${self.environment[0].variables.REGION} \
            --query 'ApplicationDetail.CreateTimestamp' \
            --output text)" \
          --region ${self.environment[0].variables.REGION} || true
        
        echo "Flink application deletion initiated."
      else
        echo "Application ${self.environment[0].variables.APP_NAME} does not exist. Skipping deletion."
      fi
    EOT

    on_failure = continue # Continue even if deletion fails (app might already be gone)
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

output "flink_log_group_name" {
  value = aws_cloudwatch_log_group.flink.name
}

output "flink_log_group_arn" {
  value = aws_cloudwatch_log_group.flink.arn
}

output "flink_log_stream_name" {
  value = aws_cloudwatch_log_stream.flink.name
}
