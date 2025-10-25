# S3 Module - Creates buckets for application JAR, input data, and output data

variable "app_name" {
  type = string
}

variable "streaming_app_bucket" {
  type = string
}

variable "input_data_bucket" {
  type = string
}

variable "output_data_bucket" {
  type = string
}

variable "input_table_name" {
  type = string
}

variable "output_table_name" {
  type = string
}

# S3 Bucket for Flink Application JAR
resource "aws_s3_bucket" "streaming_app" {
  bucket = var.streaming_app_bucket

  tags = {
    Name        = "Flink Application JAR Bucket"
    Application = var.app_name
  }
}

resource "aws_s3_bucket_versioning" "streaming_app" {
  bucket = aws_s3_bucket.streaming_app.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "streaming_app" {
  bucket = aws_s3_bucket.streaming_app.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# S3 Bucket for Input Data
resource "aws_s3_bucket" "input_data" {
  bucket = var.input_data_bucket

  tags = {
    Name        = "Input Data Bucket"
    Application = var.app_name
  }
}

resource "aws_s3_bucket_versioning" "input_data" {
  bucket = aws_s3_bucket.input_data.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "input_data" {
  bucket = aws_s3_bucket.input_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Create input table folder structure
resource "aws_s3_object" "input_table_folder" {
  bucket = aws_s3_bucket.input_data.id
  key    = "${var.input_table_name}/"
  content_type = "application/x-directory"
}

# S3 Bucket for Output Data
resource "aws_s3_bucket" "output_data" {
  bucket = var.output_data_bucket

  tags = {
    Name        = "Output Data Bucket"
    Application = var.app_name
  }
}

resource "aws_s3_bucket_versioning" "output_data" {
  bucket = aws_s3_bucket.output_data.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "output_data" {
  bucket = aws_s3_bucket.output_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Create output table folder structure
resource "aws_s3_object" "output_table_folder" {
  bucket = aws_s3_bucket.output_data.id
  key    = "${var.output_table_name}/"
  content_type = "application/x-directory"
}

output "streaming_app_bucket_name" {
  value = aws_s3_bucket.streaming_app.id
}

output "streaming_app_bucket_arn" {
  value = aws_s3_bucket.streaming_app.arn
}

output "input_data_bucket_name" {
  value = aws_s3_bucket.input_data.id
}

output "input_data_bucket_arn" {
  value = aws_s3_bucket.input_data.arn
}

output "output_data_bucket_name" {
  value = aws_s3_bucket.output_data.id
}

output "output_data_bucket_arn" {
  value = aws_s3_bucket.output_data.arn
}
