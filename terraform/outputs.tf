output "streaming_app_bucket" {
  description = "S3 bucket for Flink application JAR"
  value       = module.s3_buckets.streaming_app_bucket_name
}

output "kinesis_stream_name" {
  description = "Kinesis Data Stream name for input data"
  value       = module.kinesis.stream_name
}

output "kinesis_stream_arn" {
  description = "Kinesis Data Stream ARN"
  value       = module.kinesis.stream_arn
}

output "output_data_bucket" {
  description = "S3 bucket for output data"
  value       = module.s3_buckets.output_data_bucket_name
}

output "flink_role_arn" {
  description = "IAM role ARN for Flink application"
  value       = module.iam.flink_role_arn
}

output "lambda_function_name" {
  description = "Lambda function name for Flink lifecycle management"
  value       = module.lambda.function_name
}

output "lambda_function_arn" {
  description = "Lambda function ARN for Flink lifecycle management"
  value       = module.lambda.function_arn
}

output "codepipeline_name" {
  description = "CodePipeline name"
  value       = module.cicd.pipeline_name
}

output "codebuild_project_name" {
  description = "CodeBuild project name"
  value       = module.cicd.codebuild_project_name
}

output "log_group_name" {
  description = "CloudWatch log group for Flink application"
  value       = module.cloudwatch.log_group_name
}

output "artifacts_bucket" {
  description = "S3 bucket for CI/CD artifacts"
  value       = module.cicd.artifacts_bucket_name
}

output "bucket_suffix" {
  description = "Generated bucket suffix for reference"
  value       = local.bucket_suffix
}

output "deployment_commands" {
  description = "Useful commands for deployment and monitoring"
  value = <<-EOT
    # View Lambda logs
    aws logs tail /aws/lambda/${module.lambda.function_name} --follow --region ${var.aws_region}
    
    # View Flink application logs
    aws logs tail ${module.cloudwatch.log_group_name} --follow --region ${var.aws_region}
    
    # Describe Flink application
    aws kinesisanalyticsv2 describe-application --application-name ${var.app_name} --region ${var.aws_region}
    
    # Trigger pipeline manually
    aws codepipeline start-pipeline-execution --name ${module.cicd.pipeline_name} --region ${var.aws_region}
    
    # Send test data to Kinesis stream (base64 encoded)
    echo '{"name":"Alice","visits":5}' | base64 | xargs -I {} aws kinesis put-record --stream-name ${module.kinesis.stream_name} --partition-key test --data {} --region ${var.aws_region}
    
    # Or use CLI binary format option (easier)
    aws kinesis put-record --stream-name ${module.kinesis.stream_name} --partition-key test --data '{"name":"Alice","visits":5}' --cli-binary-format raw-in-base64-out --region ${var.aws_region}
    
    # View output data in S3
    aws s3 ls s3://${module.s3_buckets.output_data_bucket_name}/${var.output_table_name}/ --recursive --region ${var.aws_region}
  EOT
}
