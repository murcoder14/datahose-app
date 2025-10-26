"""
Flink Application Configuration Builder

Centralizes configuration logic for creating and updating Flink applications.
Eliminates duplication between create and update operations.
"""

from typing import Dict, Any


class FlinkConfigBuilder:
    """Builder for Flink application configuration"""
    
    def __init__(self, env_vars: Dict[str, Any]):
        """
        Initialize configuration builder with environment variables.
        
        Args:
            env_vars: Dictionary containing:
                - app_name: Application name
                - streaming_bucket: S3 bucket for JAR files
                - kinesis_arn: Kinesis stream ARN
                - region: AWS region
                - output_bucket: S3 output bucket
                - output_table: S3 output table/folder name
                - parallelism: Flink parallelism setting
        """
        self.env_vars = env_vars
    
    def build_application_config(self, jar_version: str) -> Dict[str, Any]:
        """
        Build complete application configuration for CreateApplication.
        
        Args:
            jar_version: S3 object version of the JAR file
            
        Returns:
            Complete ApplicationConfiguration dict
        """
        return {
            'ApplicationCodeConfiguration': self._build_code_config(jar_version),
            'FlinkApplicationConfiguration': self._build_flink_config(),
            'EnvironmentProperties': self._build_env_properties()
        }
    
    def build_application_config_update(self, jar_version: str) -> Dict[str, Any]:
        """
        Build application configuration update for UpdateApplication.
        
        Args:
            jar_version: S3 object version of the JAR file
            
        Returns:
            ApplicationConfigurationUpdate dict
        """
        return {
            'ApplicationCodeConfigurationUpdate': self._build_code_config_update(jar_version),
            'FlinkApplicationConfigurationUpdate': self._build_flink_config_update(),
            'EnvironmentPropertyUpdates': self._build_env_properties()
        }
    
    def _build_code_config(self, jar_version: str) -> Dict[str, Any]:
        """Build ApplicationCodeConfiguration for create operation"""
        return {
            'CodeContent': {
                'S3ContentLocation': {
                    'BucketARN': f'arn:aws:s3:::{self.env_vars["streaming_bucket"]}',
                    'FileKey': f'{self.env_vars["app_name"]}.jar',
                    'ObjectVersion': jar_version
                }
            },
            'CodeContentType': 'ZIPFILE'
        }
    
    def _build_code_config_update(self, jar_version: str) -> Dict[str, Any]:
        """Build ApplicationCodeConfigurationUpdate for update operation"""
        return {
            'CodeContentTypeUpdate': 'ZIPFILE',
            'CodeContentUpdate': {
                'S3ContentLocationUpdate': {
                    'BucketARNUpdate': f'arn:aws:s3:::{self.env_vars["streaming_bucket"]}',
                    'FileKeyUpdate': f'{self.env_vars["app_name"]}.jar',
                    'ObjectVersionUpdate': jar_version
                }
            }
        }
    
    def _build_flink_config(self) -> Dict[str, Any]:
        """Build FlinkApplicationConfiguration for create operation"""
        return {
            'CheckpointConfiguration': {
                'ConfigurationType': 'DEFAULT'
            },
            'MonitoringConfiguration': {
                'ConfigurationType': 'CUSTOM',
                'MetricsLevel': 'APPLICATION',
                'LogLevel': 'INFO'
            },
            'ParallelismConfiguration': {
                'ConfigurationType': 'CUSTOM',
                'Parallelism': self.env_vars["parallelism"],
                'ParallelismPerKPU': 1,
                'AutoScalingEnabled': False
            }
        }
    
    def _build_flink_config_update(self) -> Dict[str, Any]:
        """Build FlinkApplicationConfigurationUpdate for update operation"""
        return {
            'CheckpointConfigurationUpdate': {
                'ConfigurationTypeUpdate': 'DEFAULT'
            },
            'MonitoringConfigurationUpdate': {
                'ConfigurationTypeUpdate': 'CUSTOM',
                'LogLevelUpdate': 'INFO',
                'MetricsLevelUpdate': 'APPLICATION'
            },
            'ParallelismConfigurationUpdate': {
                'ConfigurationTypeUpdate': 'CUSTOM',
                'ParallelismUpdate': self.env_vars["parallelism"],
                'ParallelismPerKPUUpdate': 1,
                'AutoScalingEnabledUpdate': False
            }
        }
    
    def _build_env_properties(self) -> Dict[str, Any]:
        """Build EnvironmentProperties for create/update operations"""
        return {
            'PropertyGroups': [
                {
                    'PropertyGroupId': 'KinesisSource',
                    'PropertyMap': {
                        'stream.arn': self.env_vars["kinesis_arn"],
                        'aws.region': self.env_vars["region"]
                    }
                },
                {
                    'PropertyGroupId': 'S3Sink',
                    'PropertyMap': {
                        'bucket': self.env_vars["output_bucket"],
                        'table': self.env_vars["output_table"]
                    }
                }
            ]
        }
