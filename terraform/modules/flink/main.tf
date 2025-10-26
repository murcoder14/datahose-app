# Flink Application Module - Manages Flink application lifecycle with null_resource

variable "app_name" {
  type = string
}

variable "region" {
  type = string
}

variable "lambda_function_name" {
  type = string
}

# Trigger for application lifecycle
# This ensures the Flink app is deleted before destroying Lambda and other resources
resource "null_resource" "flink_app_lifecycle" {
  # Trigger recreation when Lambda function changes (indicating new deployment)
  triggers = {
    lambda_function_name = var.lambda_function_name
    app_name             = var.app_name
    region               = var.region
  }

  # On destroy, invoke Lambda to delete the Flink application
  provisioner "local-exec" {
    when    = destroy
    command = <<-EOT
      echo "Stopping and deleting Flink application ${self.triggers.app_name}..."
      
      # Check if application exists
      if aws kinesisanalyticsv2 describe-application \
        --application-name ${self.triggers.app_name} \
        --region ${self.triggers.region} 2>/dev/null; then
        
        echo "Application found. Stopping if running..."
        
        # Get current status
        STATUS=$(aws kinesisanalyticsv2 describe-application \
          --application-name ${self.triggers.app_name} \
          --region ${self.triggers.region} \
          --query 'ApplicationDetail.ApplicationStatus' \
          --output text)
        
        echo "Current status: $STATUS"
        
        # Stop if running
        if [ "$STATUS" = "RUNNING" ]; then
          echo "Stopping application without snapshot (force stop)..."
          aws kinesisanalyticsv2 stop-application \
            --application-name ${self.triggers.app_name} \
            --region ${self.triggers.region} \
            --force || true
          
          echo "Waiting for application to stop..."
          for i in {1..30}; do
            STATUS=$(aws kinesisanalyticsv2 describe-application \
              --application-name ${self.triggers.app_name} \
              --region ${self.triggers.region} \
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
          --application-name ${self.triggers.app_name} \
          --create-timestamp "$(aws kinesisanalyticsv2 describe-application \
            --application-name ${self.triggers.app_name} \
            --region ${self.triggers.region} \
            --query 'ApplicationDetail.CreateTimestamp' \
            --output text)" \
          --region ${self.triggers.region} || true
        
        echo "Flink application deletion initiated."
      else
        echo "Application ${self.triggers.app_name} does not exist. Skipping deletion."
      fi
    EOT

    on_failure = continue # Continue even if deletion fails (app might already be gone)
  }
}

output "lifecycle_trigger_id" {
  description = "ID of the null_resource managing Flink app lifecycle"
  value       = null_resource.flink_app_lifecycle.id
}
