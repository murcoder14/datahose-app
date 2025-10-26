# Kinesis Module - Creates Kinesis Data Stream for input data

variable "app_name" {
  type = string
}

variable "stream_name" {
  type = string
}

variable "shard_count" {
  type    = number
  default = 1
}

variable "retention_period" {
  type        = number
  default     = 24
  description = "Data retention period in hours (24-8760)"
}

variable "shard_level_metrics" {
  type = list(string)
  default = [
    "IncomingBytes",
    "IncomingRecords",
    "OutgoingBytes",
    "OutgoingRecords",
    "WriteProvisionedThroughputExceeded",
    "ReadProvisionedThroughputExceeded",
    "IteratorAgeMilliseconds"
  ]
}

# Kinesis Data Stream
resource "aws_kinesis_stream" "input_stream" {
  name             = var.stream_name
  shard_count      = var.shard_count
  retention_period = var.retention_period

  shard_level_metrics = var.shard_level_metrics

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  tags = {
    Name        = "Input Data Stream"
    Application = var.app_name
  }

  # Prevent accidental deletion of the stream with data
  lifecycle {
    prevent_destroy = false # Set to true to prevent accidental deletion
  }
}

# Outputs
output "stream_name" {
  description = "The name of the Kinesis stream"
  value       = aws_kinesis_stream.input_stream.name
}

output "stream_arn" {
  description = "The ARN of the Kinesis stream"
  value       = aws_kinesis_stream.input_stream.arn
}

output "stream_id" {
  description = "The unique identifier of the Kinesis stream"
  value       = aws_kinesis_stream.input_stream.id
}
