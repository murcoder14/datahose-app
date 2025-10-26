# CloudWatch Module - Creates log groups for Flink application

variable "app_name" {
  type = string
}

variable "log_group_name" {
  type = string
}

variable "retention_in_days" {
  type = number
}

resource "aws_cloudwatch_log_group" "flink" {
  name              = var.log_group_name
  retention_in_days = var.retention_in_days
  skip_destroy      = false  # Ensure log group is deleted on terraform destroy

  tags = {
    Name        = "Flink Application Logs"
    Application = var.app_name
  }
}

resource "aws_cloudwatch_log_stream" "flink" {
  name           = "flink-application"
  log_group_name = aws_cloudwatch_log_group.flink.name
}

output "log_group_name" {
  value = aws_cloudwatch_log_group.flink.name
}

output "log_group_arn" {
  value = aws_cloudwatch_log_group.flink.arn
}

output "log_stream_name" {
  value = aws_cloudwatch_log_stream.flink.name
}
