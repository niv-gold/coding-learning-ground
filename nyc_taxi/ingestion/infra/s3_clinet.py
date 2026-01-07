"""AWS S3 client helper.

Provides a thin wrapper that creates a Boto3 S3 client using
configuration from `S3Config`.

Main use (simple):
    - Instantiate with S3Config
    - Call `create_s3_client()` to get a ready-to-use `boto3` client
"""

import boto3
from nyc_taxi.ingestion.config.settings import S3Config


class S3Client:
    """Helper to construct a configured boto3 S3 client.

    Args:
        config (S3Config): Configuration containing AWS credentials and region.

    Methods:
        create_s3_client() -> boto3.client: Return a configured S3 client.
    """

    def __init__(self, config: S3Config):
        self.config = config

    def create_s3_client(self):
        """Create and return a boto3 S3 client.

        Returns:
            boto3.client: S3 client configured with credentials and region.
        """
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=self.config.aws_access_key_id,
            aws_secret_access_key=self.config.aws_secret_access_key,
            region_name=self.config.region_name,
        )
        return s3_client