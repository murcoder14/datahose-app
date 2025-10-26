"""
Flink Application Status Management

Handles status checking, waiting, and state transitions for Flink applications.
Centralizes status-related logic with smart transition handling.
"""

import time
import logging
from typing import Set
from botocore.exceptions import ClientError


class FlinkStatusManager:
    """Manages Flink application status checks and transitions"""
    
    # Valid transitional states that can lead to target status
    VALID_TRANSITIONS = {
        'READY': {'STOPPING', 'UPDATING'},
        'RUNNING': {'STARTING', 'UPDATING'}
    }
    
    def __init__(self, kda_client, app_name: str):
        """
        Initialize status manager.
        
        Args:
            kda_client: boto3 KinesisAnalyticsV2 client
            app_name: Name of the Flink application
        """
        self.kda_client = kda_client
        self.app_name = app_name
        self.logger = logging.getLogger(__name__)
    
    def get_status(self) -> str:
        """
        Get current application status.
        
        Returns:
            Current status string (e.g., 'RUNNING', 'READY', 'STARTING')
            
        Raises:
            ClientError: If describe_application fails
        """
        try:
            response = self.kda_client.describe_application(ApplicationName=self.app_name)
            return response['ApplicationDetail']['ApplicationStatus']
        except ClientError as e:
            self.logger.error(f"Failed to get application status: {e}")
            raise
    
    def get_version(self) -> int:
        """
        Get current application version.
        
        Returns:
            Application version ID
            
        Raises:
            ClientError: If describe_application fails
        """
        try:
            response = self.kda_client.describe_application(ApplicationName=self.app_name)
            return response['ApplicationDetail']['ApplicationVersionId']
        except ClientError as e:
            self.logger.error(f"Failed to get application version: {e}")
            raise
    
    def application_exists(self) -> bool:
        """
        Check if the Flink application exists.
        
        Returns:
            True if application exists, False otherwise
        """
        try:
            self.kda_client.describe_application(ApplicationName=self.app_name)
            return True
        except self.kda_client.exceptions.ResourceNotFoundException:
            return False
        except ClientError as e:
            self.logger.error(f"Error checking application existence: {e}")
            raise
    
    def wait_for_status(self, target_status: str, max_wait: int = 300, poll_interval: int = 10) -> None:
        """
        Wait for application to reach target status with smart transition handling.
        
        Args:
            target_status: Desired status (e.g., 'RUNNING', 'READY')
            max_wait: Maximum wait time in seconds (default: 300)
            poll_interval: Seconds between status checks (default: 10)
            
        Raises:
            TimeoutError: If target status not reached within max_wait
        """
        start_time = time.time()
        valid_transitions = self.VALID_TRANSITIONS.get(target_status, set())
        
        while (time.time() - start_time) < max_wait:
            current_status = self.get_status()
            elapsed = int(time.time() - start_time)
            
            self.logger.info(
                f"Status: {current_status} → {target_status} "
                f"(elapsed: {elapsed}s, max: {max_wait}s)"
            )
            
            # Check if we've reached the target
            if current_status == target_status:
                self.logger.info(f"Application reached {target_status} status")
                return
            
            # Warn if current status is not a valid transition
            if valid_transitions and current_status not in valid_transitions:
                self.logger.warning(
                    f"Application in unexpected state '{current_status}' "
                    f"while waiting for '{target_status}'"
                )
            
            time.sleep(poll_interval)
        
        # Timeout reached
        final_status = self.get_status()
        raise TimeoutError(
            f"Timeout waiting for application to reach '{target_status}' status. "
            f"Current status: '{final_status}', elapsed: {max_wait}s"
        )
    
    def ensure_ready(self, stop_if_running: bool = True, max_wait: int = 300) -> None:
        """
        Ensure application is in READY state, stopping if necessary.
        
        Args:
            stop_if_running: If True, stop running application (default: True)
            max_wait: Maximum wait time in seconds (default: 300)
            
        Raises:
            Exception: If application is in unexpected state
            TimeoutError: If READY state not reached within max_wait
        """
        current_status = self.get_status()
        self.logger.info(f"Current application status: {current_status}")
        
        if current_status == 'READY':
            self.logger.info("Application is already in READY state")
            return
        
        if current_status == 'RUNNING':
            if not stop_if_running:
                raise Exception("Application is RUNNING and stop_if_running=False")
            
            self.logger.info("Application is RUNNING. Stopping...")
            self._stop_application()
            self.wait_for_status('READY', max_wait=max_wait)
            
        elif current_status == 'STOPPING':
            self.logger.info("Application is already stopping. Waiting for READY...")
            self.wait_for_status('READY', max_wait=max_wait)
            
        elif current_status == 'STARTING':
            self.logger.info("Application is starting. Waiting to complete...")
            self.wait_for_status('RUNNING', max_wait=max_wait)
            if stop_if_running:
                self._stop_application()
                self.wait_for_status('READY', max_wait=max_wait)
        else:
            raise Exception(
                f"Cannot ensure READY state from current state: {current_status}. "
                f"Expected READY, RUNNING, STOPPING, or STARTING."
            )
    
    def _stop_application(self) -> None:
        """
        Internal method to stop the application.
        Does not wait for completion - use wait_for_status() separately.
        """
        try:
            self.kda_client.stop_application(ApplicationName=self.app_name)
            self.logger.info("Stop command issued successfully")
        except ClientError as e:
            self.logger.error(f"Failed to stop application: {e}")
            raise
