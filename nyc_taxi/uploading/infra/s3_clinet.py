# upload/s3_client.py
import boto3
from nyc_taxi.uploading.config.settings import S3Config

class S3Client:
    def __init__(self, config: S3Config):
        self.config = config

    def create_s3_clinet(self):
        s3_client = boto3.client(
            "s3",
            aws_access_key_id = self.config.aws_access_key_id,
            aws_secret_access_key = self.config.aws_secret_access_key,
            region_name = self.config.region_name)
        return s3_client