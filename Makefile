# Makefile for DataHose App Terraform Deployment

.PHONY: help init plan apply deploy destroy clean test status logs

# Variables
TERRAFORM_DIR := terraform
AWS_REGION := us-east-2
APP_NAME := datahose-app

help: ## Show this help message
	@echo "DataHose App - Terraform Deployment"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'

init: ## Initialize Terraform
	@echo "Initializing Terraform..."
	cd $(TERRAFORM_DIR) && terraform init

plan: ## Show Terraform execution plan
	@echo "Planning infrastructure changes..."
	cd $(TERRAFORM_DIR) && terraform plan

apply: ## Apply Terraform configuration
	@echo "Applying Terraform configuration..."
	cd $(TERRAFORM_DIR) && terraform apply

deploy: init plan apply ## Full deployment: init + plan + apply
	@echo "Deployment complete!"
	@echo ""
	@$(MAKE) outputs

outputs: ## Show Terraform outputs
	@echo "Terraform Outputs:"
	cd $(TERRAFORM_DIR) && terraform output

destroy: ## Destroy all Terraform-managed resources
	@echo "WARNING: This will destroy all resources!"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		cd $(TERRAFORM_DIR) && terraform destroy; \
	fi

clean: ## Clean Terraform cache and state backups
	@echo "Cleaning Terraform files..."
	find $(TERRAFORM_DIR) -type d -name ".terraform" -exec rm -rf {} + 2>/dev/null || true
	find $(TERRAFORM_DIR) -name "*.tfstate.backup" -delete
	find $(TERRAFORM_DIR) -name "lambda_package.zip" -delete

validate: ## Validate Terraform configuration
	@echo "Validating Terraform configuration..."
	cd $(TERRAFORM_DIR) && terraform validate

format: ## Format Terraform files
	@echo "Formatting Terraform files..."
	cd $(TERRAFORM_DIR) && terraform fmt -recursive

# Pipeline operations
pipeline-status: ## Show CodePipeline status
	@aws codepipeline get-pipeline-state \
		--name $(APP_NAME)-pipeline \
		--region $(AWS_REGION) \
		--query 'stageStates[*].[stageName,latestExecution.status]' \
		--output table

pipeline-start: ## Manually trigger CodePipeline
	@echo "Triggering CodePipeline..."
	@aws codepipeline start-pipeline-execution \
		--name $(APP_NAME)-pipeline \
		--region $(AWS_REGION)
	@echo "Pipeline started. Use 'make pipeline-status' to check progress."

# Flink application operations
flink-status: ## Show Flink application status
	@aws kinesisanalyticsv2 describe-application \
		--application-name $(APP_NAME) \
		--region $(AWS_REGION) \
		--query 'ApplicationDetail.{Name:ApplicationName,Status:ApplicationStatus,Version:ApplicationVersionId}' \
		--output table

flink-start: ## Start Flink application
	@echo "Starting Flink application..."
	@aws lambda invoke \
		--function-name $(APP_NAME)-flink-lifecycle \
		--payload '{"action":"start"}' \
		--region $(AWS_REGION) \
		/tmp/lambda-response.json
	@cat /tmp/lambda-response.json | jq .
	@rm /tmp/lambda-response.json

flink-stop: ## Stop Flink application
	@echo "Stopping Flink application..."
	@aws lambda invoke \
		--function-name $(APP_NAME)-flink-lifecycle \
		--payload '{"action":"stop"}' \
		--region $(AWS_REGION) \
		/tmp/lambda-response.json
	@cat /tmp/lambda-response.json | jq .
	@rm /tmp/lambda-response.json

# Logging
logs-flink: ## Tail Flink application logs
	@echo "Tailing Flink logs (Ctrl+C to exit)..."
	@aws logs tail /aws/kinesis-analytics/$(APP_NAME) --follow --region $(AWS_REGION)

logs-lambda: ## Tail Lambda function logs
	@echo "Tailing Lambda logs (Ctrl+C to exit)..."
	@aws logs tail /aws/lambda/$(APP_NAME)-flink-lifecycle --follow --region $(AWS_REGION)

logs-codebuild: ## Tail CodeBuild logs
	@echo "Tailing CodeBuild logs (Ctrl+C to exit)..."
	@aws logs tail /aws/codebuild/$(APP_NAME)-build --follow --region $(AWS_REGION)

# Testing
test-upload: ## Upload sample test data to input bucket
	@echo "Creating sample test data..."
	@echo "name,visits\nAlice,5\nBob,3\nAlice,2\nCharlie,7\nBob,1" > /tmp/test-visits.csv
	@INPUT_BUCKET=$$(cd $(TERRAFORM_DIR) && terraform output -raw input_data_bucket); \
	echo "Uploading to s3://$$INPUT_BUCKET/datafall/"; \
	aws s3 cp /tmp/test-visits.csv s3://$$INPUT_BUCKET/datafall/test-$$(date +%s).csv --region $(AWS_REGION)
	@echo "Test data uploaded. Check output bucket in ~1 minute."

test-check-output: ## List output files
	@OUTPUT_BUCKET=$$(cd $(TERRAFORM_DIR) && terraform output -raw output_data_bucket); \
	echo "Output files in s3://$$OUTPUT_BUCKET/datalake/:"; \
	aws s3 ls s3://$$OUTPUT_BUCKET/datalake/ --recursive --region $(AWS_REGION)

test-download-output: ## Download and display output files
	@OUTPUT_BUCKET=$$(cd $(TERRAFORM_DIR) && terraform output -raw output_data_bucket); \
	rm -rf /tmp/flink-output; \
	mkdir -p /tmp/flink-output; \
	aws s3 sync s3://$$OUTPUT_BUCKET/datalake/ /tmp/flink-output/ --region $(AWS_REGION); \
	echo "Output files downloaded to /tmp/flink-output/"; \
	echo ""; \
	echo "Results:"; \
	cat /tmp/flink-output/* 2>/dev/null || echo "No output files yet"

# Utility
list-buckets: ## List S3 buckets created by Terraform
	@cd $(TERRAFORM_DIR) && \
	echo "Streaming App Bucket: $$(terraform output -raw streaming_app_bucket)"; \
	echo "Input Data Bucket:    $$(terraform output -raw input_data_bucket)"; \
	echo "Output Data Bucket:   $$(terraform output -raw output_data_bucket)"; \
	echo "Artifacts Bucket:     $$(terraform output -raw artifacts_bucket)"

costs: ## Estimate monthly costs (approximate)
	@echo "Estimated Monthly Costs (us-east-2):"
	@echo "  Flink (1 KPU, 24/7):       ~\$$160.00"
	@echo "  S3 Storage (10 GB):        ~\$$0.23"
	@echo "  Lambda (100 invocations):  ~\$$0.00"
	@echo "  CodeBuild (10 builds):     ~\$$0.10"
	@echo "  CloudWatch Logs (1 GB):    ~\$$0.50"
	@echo "  --------------------------------"
	@echo "  Total:                     ~\$$160.83/month"

graph: ## Generate Terraform dependency graph
	@echo "Generating Terraform graph..."
	@cd $(TERRAFORM_DIR) && terraform graph | dot -Tpng > terraform-graph.png
	@echo "Graph saved to $(TERRAFORM_DIR)/terraform-graph.png"

.DEFAULT_GOAL := help
