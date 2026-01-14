# Configuration module for NYC Taxi ingestion system
# Handles AWS S3 and Snowflake connection settings

from dataclasses import dataclass, fields
from nyc_taxi.utils import required_value, to_connector_kwarg_utils
from pathlib import Path
import os
from typing import Optional, Dict, Any, List, Iterable
from dotenv import load_dotenv  # only for test usage!!!
from great_expectations.datasource.fluent import PandasS3Datasource


@dataclass
class MyLocalData:
    """Paths for local data storage and archive locations."""
    loacl_path:Path = Path(Path(__file__).resolve().parent.parent.as_posix() + '/app/data_files')
    archive_path:Path = Path(Path(__file__).resolve().parent.parent.as_posix() + '/app/data_files/archive')

@dataclass(frozen=True)
class S3Config:
    """
    Configuration dataclass for AWS S3 file uploads.
    
    This dataclass encapsulates all parameters needed to connect
    to AWS S3 and upload files from local storage.
    
    Attributes:
        aws_access_key_id (str): AWS access key for authentication
        aws_secret_access_key (str): AWS secret key for authentication
        region_name (str): AWS region where the S3 bucket is located
                          (e.g., 'us-east-1', 'eu-west-1')
        bucket_name (str): Name of the S3 bucket to upload files to
        local_file_extension (str): File extension of local files to upload
        s3_base_prefix (str, optional): Prefix/folder structure in S3 bucket
                                       (e.g., 'raw/', 'staging/')
                                       Defaults to '' (bucket root)
    """
    # Read values from environment (defaults to empty string when missing)
    aws_access_key_id: str
    aws_secret_access_key: str
    region_name: str
    bucket_name: str
    local_file_extension: str
    s3_base_prefix_name: str = ""

    @classmethod
    def from_env(cls)-> "S3Config":
        """
        Load S3 config from .env file, inject credentials on runtime.
        Fail fast if any required variable is missing.
        """
        return cls(
            aws_access_key_id = required_value("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key = required_value("AWS_SECRET_ACCESS_KEY"),
            region_name = required_value("AWS_REGION"),
            bucket_name = required_value("S3_BUCKET_NAME"),
            local_file_extension = required_value("LOCAL_FILE_EXTENSION"),
            s3_base_prefix_name = required_value("S3_BASE_PREFIX_NAME"),
        )
    
    def to_connector_kwarg(self)-> dict:
        """Convert dataclass fields to a dictionary of connector kwargs."""
        return to_connector_kwarg_utils(self)
    
    def __repr__(self) -> str:
        """String representation with redacted sensitive credentials."""
        return (
            "S3Config("
            f"aws_access_key_id={self.aws_access_key_id!r},  aws_secret_access_key='***REDACTED***', "
            f"region_name={self.region_name!r}, bucket_name={self.bucket_name!r}, local_file_extension={self.local_file_extension!r}, "
        )

@dataclass(frozen=True)
class SnowflakeConfig:
    """Configuration dataclass for Snowflake database connection."""
    account: str
    user: str
    password: str
    warehouse: str
    database: str
    schema: str
    log_table: str
    log_schema: str
    role: str | None = None

    @classmethod
    def from_env(cls)-> "SnowflakeConfig":
        """
        Load Snowflake config from environment variables.
        Fail fast if something important is missing.
        """
        return cls(
            account = required_value("SNOWFLAKE_ACCOUNT"),
            user = required_value("SNOWFLAKE_USER"),
            password = required_value("SNOWFLAKE_PASSWORD"),
            warehouse = required_value("SNOWFLAKE_WAREHOUSE"),
            database = required_value("SNOWFLAKE_DATABASE"),
            schema = required_value("SNOWFLAKE_SCHEMA"),
            role = required_value("SNOWFLAKE_ROLE"),
            log_table = required_value("SNOWFLAKE_LOG_TABLE"),
            log_schema = required_value("SNOWFLAKE_LOG_SCHEMA")
        )

    def to_connector_kwarg(self)-> dict:
        kwarg =  to_connector_kwarg_utils(self)
        if self.role:
            kwarg["role"] = self.role

        return kwarg

    def __repr__(self) -> str:
        return (
            "SnowflakeConfig("
            f"account={self.account!r}, user={self.user!r}, password='***REDACTED***', "
            f"warehouse={self.warehouse!r}, database={self.database!r}, schema={self.schema!r}, "
            f"role={self.role!r}, log_table={self.log_table!r})"
        )

@dataclass(frozen=True)
class GreatExpectationsConfig:
    ge_root_dir: str
    ge_data_context_id: str

    @staticmethod
    def get_default_ge_root_dir() -> str:
        """Get default Great Expectations root directory path."""
        return os.path.join(Path(__file__).resolve().parents[2].as_posix(), "great_expectations")

    @classmethod
    def from_env(cls) -> "GreatExpectationsConfig":
        """Configuration for Great Expectations data validation."""
        return cls(
            ge_root_dir = cls.get_default_ge_root_dir(),
            ge_data_context_id = required_value("GX_SUITE_NAME")
        )

@dataclass(frozen=True, slots=True)
class GXS3AssetSpec():
    data_source_name: str
    s3conf: S3Config
    data_source: PandasS3Datasource    
    asset_name: str
    batching_regex: str  # e.g. r"(?P<file_name>.*)\.csv"
    asset_type: str

    @property
    def bucket(self) -> str:
        return self.s3conf.bucket_name
    
    @property
    def region(self) -> str:
        return self.s3conf.region_name
    
    @property
    def s3_prefix(self) -> str:
        return self.s3conf.s3_base_prefix_name

    def to_kwarg(self) -> Dict[str, Any]:
        return {
            "bucket": self.bucket,
            "region": self.region,  
            "s3_prefix": self.s3_prefix,
            "asset_name": self.asset_name,            
            "batching_regex": self.batching_regex,
            "asset_type": self.asset_type
        }

@dataclass(frozen=True, slots=True)
class GXValidationSpec:
    suite_name: str
    validation_definition_name: str
    batch_definition_name: str
    batch_options: Dict[str, Any]  # e.g. {"file_name": "sample"}

@dataclass(frozen=True, slots=True)
class GXCheckpointSpec:
    checkpoint_name: str
    data_docs_site_name: str = "local_site"
    build_data_docs: bool = True

    # Email is optional (recommended to keep off for first iteration).
    email_on_failure: bool = False
    smtp_address: Optional[str] = None
    smtp_port: Optional[str] = None
    sender_login: Optional[str] = None
    sender_password: Optional[str] = None
    receiver_emails: Optional[str] = None
    use_ssl: bool = True

if __name__=="__main__":
    load_dotenv()

    for k,v in SnowflakeConfig.from_env().to_connector_kwarg().items():
        print(f'{k}: {v}')