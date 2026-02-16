from __future__ import annotations
import sys
from pathlib import Path
if __name__ == "__main__":
    PROJECT_ROOT = Path(__file__).resolve().parents[3]
    sys.path.insert(0, str(PROJECT_ROOT))
    print("Project root added to sys.path[0]:", sys.path[0])

from nyc_taxi.ingestion.config.settings import S3Config
from nyc_taxi.ingestion.core.ports import Uploader, FileIdentity
from nyc_taxi.ingestion.infra.s3_clinet import S3Client
from datetime import datetime
from nyc_taxi.ingestion.config.settings import MyLocalData
from dotenv import load_dotenv


class S3Uploader(Uploader):
    """Upload files to Amazon S3.

    This class wraps upload operations and S3 key construction so the
    pipeline can store files under a logical prefix (e.g. `raw/`).

    Args:
        config (S3Config): Configuration with AWS credentials and bucket name.
        bucket (str): Optional explicit bucket name. If empty, uses value from config.
        base_prefix (str): Optional logical S3 prefix (no leading/trailing slash).
    """

    def __init__(self, config: S3Config, bucket: str = "", base_prefix: str = ""):
        # Keep provided config for potential future use
        self.config = config
        # Normalise base prefix (remove stray slashes)
        self.base_prefix = base_prefix.strip("/")
        # Create a boto3 S3 client via the helper
        self.s3_client = S3Client(self.config).create_s3_client()
        # Determine which S3 bucket to use
        if bucket:
            self.bucket = bucket.strip('/')
        else:
            self.bucket = self.config.bucket_name

    def build_s3_key(self, file: FileIdentity) -> str:
        """Build the full S3 object key for `file`.

        Args:
            file (FileIdentity): File metadata object.

        Returns:
            str: Object key to use in S3 (prefix + file_type_subdir + file name).

        Main use:
            - Ensures files are written under `base_prefix` when provided.
            - Organizes files into subdirectories by file type (csv, parquet, json, etc.).
            
        Example:
            - base_prefix='raw', file='data.csv' -> 's3://bucket/raw/csv/data.csv'
            - base_prefix='raw', file='data.parquet' -> 's3://bucket/raw/parquet/data.parquet'
        """
        # Extract file extension (without the dot) and convert to lowercase
        file_suffix = Path(file.name).suffix.lstrip('.').lower()
        if file_suffix not in {'csv', 'parquet'}:
            raise ValueError(f"Unsupported file extension: .{file_suffix}. Supported: csv, parquet.")            
        
        if self.base_prefix:
            if file_suffix:
                return f'{self.base_prefix}/{file_suffix}/{file.name}'
            else:
                # If no suffix, place in base_prefix directly
                return f'{self.base_prefix}/{file.name}'
        return file.name

    def upload(self, file: FileIdentity) -> None:
        """Upload a local file to S3.

        Args:
            file (FileIdentity): File metadata with `path` pointing to local file.

        Returns:
            None

        Side effects:
            - Uploads the file to S3 using `upload_file`
            - Prints the S3 URI on success
        """
        s3_key = self.build_s3_key(file)
        # Use the boto3 client to perform the upload
        self.s3_client.upload_file(
            Filename=str(file.path),
            Bucket=self.bucket,
            Key=s3_key,
        )
        print(f"s3://{self.bucket}/{s3_key}")




##-------------------------------------------------------------------------
## QA - unit test
##-------------------------------------------------------------------------
if __name__=='__main__':
    print('~'*150)
    print('--> Start')
    
    my_local_path = MyLocalData()
    load_dotenv()
    s3_config = S3Config.from_env()
    file_1 = FileIdentity(Path(f'{my_local_path.loacl_path}/taxi_zone_lookup.csv'),123456, datetime.now())    
    s3uploader_ins = S3Uploader(config=s3_config, base_prefix='raw')
    s3uploader_ins.upload(file_1)
    
    print('--> End')