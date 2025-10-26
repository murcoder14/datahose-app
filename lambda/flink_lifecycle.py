"""
AWS Lambda Function for Flink Application Lifecycle Management

This Lambda function manages the lifecycle of AWS Managed Service for Apache Flink applications.
It handles creation, update, start, stop, and deletion of Flink applications.

Environment Variables:
    APP_NAME: Name of the Flink application
    REGION: AWS region
    FLINK_ROLE_ARN: IAM role ARN for Flink execution
    STREAMING_APP_BUCKET: S3 bucket containing application JAR
    KINESIS_STREAM_NAME: Kinesis Data Stream name for input data
    KINESIS_STREAM_ARN: Kinesis Data Stream ARN for input data
    OUTPUT_DATA_BUCKET: S3 bucket for output data
    OUTPUT_TABLE_NAME: Output table/folder name
    LOG_GROUP_NAME: CloudWatch log group name
    FLINK_VERSION: Flink runtime version (e.g., FLINK-1_20)
    FLINK_PARALLELISM: Flink parallelism configuration
"""

import json
import os
import time
import logging
from typing import Dict, Any, Optional
import boto3
from botocore.exceptions import ClientError

# Import modular components
from config_builder import FlinkConfigBuilder
from status_manager import FlinkStatusManager

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
kda_client = boto3.client('kinesisanalyticsv2')
s3_client = boto3.client('s3')

# Environment variables
APP_NAME = os.environ['APP_NAME']
REGION = os.environ['REGION']
FLINK_ROLE_ARN = os.environ['FLINK_ROLE_ARN']
STREAMING_APP_BUCKET = os.environ['STREAMING_APP_BUCKET']
KINESIS_STREAM_NAME = os.environ['KINESIS_STREAM_NAME']
KINESIS_STREAM_ARN = os.environ['KINESIS_STREAM_ARN']
OUTPUT_DATA_BUCKET = os.environ['OUTPUT_DATA_BUCKET']
OUTPUT_TABLE_NAME = os.environ['OUTPUT_TABLE_NAME']
LOG_GROUP_NAME = os.environ['LOG_GROUP_NAME']
FLINK_VERSION = os.environ['FLINK_VERSION']
FLINK_PARALLELISM = int(os.environ['FLINK_PARALLELISM'])

JAR_KEY = f"{APP_NAME}.jar"

# Initialize configuration builder and status manager
config_builder = FlinkConfigBuilder({
    'app_name': APP_NAME,
    'streaming_bucket': STREAMING_APP_BUCKET,
    'kinesis_arn': KINESIS_STREAM_ARN,
    'region': REGION,
    'output_bucket': OUTPUT_DATA_BUCKET,
    'output_table': OUTPUT_TABLE_NAME,
    'parallelism': FLINK_PARALLELISM
})

status_manager = FlinkStatusManager(kda_client, APP_NAME)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler for Flink application lifecycle management.
    
    Expected event structure from CodePipeline:
    {
        "CodePipeline.job": {
            "id": "<job-id>",
            "data": {
                "inputArtifacts": [...],
                "outputArtifacts": [...],
                "actionConfiguration": {
                    "configuration": {
                        "UserParameters": "{\"action\": \"deploy\"}"
                    }
                }
            }
        }
    }
    
    Or for direct invocation:
    {
        "action": "deploy|start|stop|delete|describe"
    }
    """
    logger.info(f"Received event: {json.dumps(event)}")
    
    try:
        # Extract action from event
        action = _extract_action(event)
        logger.info(f"Action to perform: {action}")
        
        # Execute action
        if action == "deploy":
            result = deploy_application()
        elif action == "start":
            result = start_application()
        elif action == "stop":
            result = stop_application()
        elif action == "delete":
            result = delete_application()
        elif action == "describe":
            result = describe_application()
        else:
            raise ValueError(f"Invalid action: {action}")
        
        # Report success to CodePipeline if applicable
        if 'CodePipeline.job' in event:
            job_id = event['CodePipeline.job']['id']
            codepipeline = boto3.client('codepipeline')
            codepipeline.put_job_success_result(jobId=job_id)
        
        return {
            'statusCode': 200,
            'body': json.dumps(result)
        }
        
    except Exception as e:
        logger.error(f"Error: {str(e)}", exc_info=True)
        
        # Report failure to CodePipeline if applicable
        if 'CodePipeline.job' in event:
            job_id = event['CodePipeline.job']['id']
            codepipeline = boto3.client('codepipeline')
            codepipeline.put_job_failure_result(
                jobId=job_id,
                failureDetails={
                    'type': 'JobFailed',
                    'message': str(e)
                }
            )
        
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }


def _extract_action(event: Dict[str, Any]) -> str:
    """Extract action from event (CodePipeline or direct invocation)."""
    if 'CodePipeline.job' in event:
        user_params = event['CodePipeline.job']['data']['actionConfiguration']['configuration'].get('UserParameters', '{}')
        params = json.loads(user_params)
        return params.get('action', 'deploy')
    return event.get('action', 'deploy')


def deploy_application() -> Dict[str, Any]:
    """
    Deploy (create or update) the Flink application.
    
    Returns:
        Dict with deployment status and application details
    """
    logger.info(f"Deploying application: {APP_NAME}")
    
    # Get JAR version from S3
    jar_version = _get_jar_version()
    logger.info(f"JAR version: {jar_version}")
    
    # Check if application exists
    app_exists = status_manager.application_exists()
    
    if app_exists:
        # Check if update is needed by comparing JAR versions
        current_jar_version = _get_current_jar_version()
        logger.info(f"Current JAR version in Flink app: {current_jar_version}")
        
        if current_jar_version == jar_version:
            logger.info("JAR version unchanged. Skipping update to avoid unnecessary restart.")
            current_status = status_manager.get_status()
            
            if current_status == 'RUNNING':
                logger.info("Application is already running with correct version.")
                return {'action': 'deploy', 'status': 'already_up_to_date', 'jar_version': jar_version}
            elif current_status == 'READY':
                logger.info("Application is stopped. Starting with existing version...")
                return start_application()
            else:
                logger.info(f"Application is in {current_status} state.")
                return {'action': 'deploy', 'status': f'app_status_{current_status.lower()}', 'jar_version': jar_version}
        else:
            logger.info(f"JAR version changed from {current_jar_version} to {jar_version}. Updating application...")
            result = _update_application(jar_version)
    else:
        logger.info("Application does not exist. Creating...")
        result = _create_application(jar_version)
    
    # Start the application
    logger.info("Starting application...")
    start_result = start_application()
    
    result.update(start_result)
    return result


def _get_jar_version() -> str:
    """Get the S3 object version of the JAR file."""
    try:
        response = s3_client.head_object(
            Bucket=STREAMING_APP_BUCKET,
            Key=JAR_KEY
        )
        version_id = response.get('VersionId', 'null')
        logger.info(f"JAR version ID: {version_id}")
        return version_id
    except ClientError as e:
        logger.error(f"Failed to get JAR version: {e}")
        raise


def _get_current_jar_version() -> str:
    """Get the JAR version currently deployed in the Flink application."""
    try:
        response = kda_client.describe_application(ApplicationName=APP_NAME)
        app_config = response['ApplicationDetail']['ApplicationConfigurationDescription']
        code_content = app_config.get('ApplicationCodeConfigurationDescription', {}).get('CodeContentDescription', {})
        s3_content = code_content.get('S3ApplicationCodeLocationDescription', {})
        current_version = s3_content.get('ObjectVersion', 'unknown')
        return current_version
    except ClientError as e:
        logger.error(f"Failed to get current JAR version: {e}")
        raise


def _create_application(jar_version: str) -> Dict[str, Any]:
    """Create a new Flink application."""
    logger.info("Creating Flink application...")
    
    account_id = boto3.client('sts').get_caller_identity()['Account']
    
    try:
        # Use config_builder to generate configuration
        application_config = config_builder.build_application_config(jar_version)
        
        response = kda_client.create_application(
            ApplicationName=APP_NAME,
            RuntimeEnvironment=FLINK_VERSION,
            ServiceExecutionRole=FLINK_ROLE_ARN,
            ApplicationConfiguration=application_config
        )
        
        logger.info("Application created. Waiting for READY status...")
        status_manager.wait_for_status('READY', max_wait=300)
        
        # Add CloudWatch logging
        logger.info("Adding CloudWatch logging configuration...")
        app_version = status_manager.get_version()
        
        kda_client.add_application_cloud_watch_logging_option(
            ApplicationName=APP_NAME,
            CurrentApplicationVersionId=app_version,
            CloudWatchLoggingOption={
                'LogStreamARN': f'arn:aws:logs:{REGION}:{account_id}:log-group:{LOG_GROUP_NAME}:log-stream:flink-application'
            }
        )
        
        logger.info("Application created successfully")
        return {
            'action': 'create',
            'status': 'success',
            'application_arn': response['ApplicationDetail']['ApplicationARN']
        }
        
    except ClientError as e:
        logger.error(f"Failed to create application: {e}")
        raise


def _update_application(jar_version: str) -> Dict[str, Any]:
    """Update an existing Flink application."""
    logger.info("Updating Flink application...")
    
    # Ensure application is in READY state before updating
    try:
        status_manager.ensure_ready(stop_if_running=True, max_wait=300)
    except Exception as e:
        logger.error(f"Failed to ensure READY state: {e}")
        raise
    
    try:
        app_version = status_manager.get_version()
        
        # Use config_builder to generate update configuration
        application_config_update = config_builder.build_application_config_update(jar_version)
        # Use config_builder to generate update configuration
        application_config_update = config_builder.build_application_config_update(jar_version)
        
        response = kda_client.update_application(
            ApplicationName=APP_NAME,
            CurrentApplicationVersionId=app_version,
            ApplicationConfigurationUpdate=application_config_update
        )
        
        logger.info("Application updated successfully")
        return {
            'action': 'update',
            'status': 'success',
            'application_version': response['ApplicationDetail']['ApplicationVersionId']
        }
        
    except ClientError as e:
        logger.error(f"Failed to update application: {e}")
        raise


def start_application() -> Dict[str, Any]:
    """Start the Flink application."""
    logger.info(f"Starting application: {APP_NAME}")
    
    current_status = status_manager.get_status()
    
    if current_status == 'RUNNING':
        logger.info("Application is already running")
        return {'action': 'start', 'status': 'already_running'}
    
    if current_status == 'STARTING':
        logger.info("Application is already starting")
        return {'action': 'start', 'status': 'already_starting'}
    
    try:
        kda_client.start_application(
            ApplicationName=APP_NAME,
            RunConfiguration={
                'FlinkRunConfiguration': {
                    'AllowNonRestoredState': True
                }
            }
        )
        
        logger.info("Start command issued successfully. Application will start asynchronously.")
        logger.info("Monitor application status with: aws kinesisanalyticsv2 describe-application --application-name datahose-app --region us-east-2")
        return {'action': 'start', 'status': 'starting'}
        
    except ClientError as e:
        logger.error(f"Failed to start application: {e}")
        raise


def stop_application() -> Dict[str, Any]:
    """Stop the Flink application."""
    logger.info(f"Stopping application: {APP_NAME}")
    
    current_status = status_manager.get_status()
    
    if current_status in ['READY', 'STOPPING']:
        logger.info(f"Application is already stopped or stopping: {current_status}")
        return {'action': 'stop', 'status': f'already_{current_status.lower()}'}
    
    try:
        kda_client.stop_application(ApplicationName=APP_NAME)
        
        logger.info("Stop command issued. Waiting for READY status...")
        status_manager.wait_for_status('READY', max_wait=300)
        
        logger.info("Application stopped successfully")
        return {'action': 'stop', 'status': 'success'}
        
    except ClientError as e:
        logger.error(f"Failed to stop application: {e}")
        raise


def delete_application() -> Dict[str, Any]:
    """Delete the Flink application."""
    logger.info(f"Deleting application: {APP_NAME}")
    
    # Stop the application if running
    current_status = status_manager.get_status()
    if current_status == 'RUNNING':
        logger.info("Stopping application before deletion...")
        stop_application()
    
    try:
        create_timestamp = kda_client.describe_application(
            ApplicationName=APP_NAME
        )['ApplicationDetail']['CreateTimestamp']
        
        kda_client.delete_application(
            ApplicationName=APP_NAME,
            CreateTimestamp=create_timestamp
        )
        
        logger.info("Application deleted successfully")
        return {'action': 'delete', 'status': 'success'}
        
    except ClientError as e:
        logger.error(f"Failed to delete application: {e}")
        raise


def describe_application() -> Dict[str, Any]:
    """Get application details."""
    logger.info(f"Describing application: {APP_NAME}")
    
    try:
        response = kda_client.describe_application(ApplicationName=APP_NAME)
        details = response['ApplicationDetail']
        
        return {
            'action': 'describe',
            'status': 'success',
            'application': {
                'name': details['ApplicationName'],
                'arn': details['ApplicationARN'],
                'status': details['ApplicationStatus'],
                'version': details['ApplicationVersionId'],
                'runtime': details['RuntimeEnvironment']
            }
        }
        
    except ClientError as e:
        logger.error(f"Failed to describe application: {e}")
        raise
