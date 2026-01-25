# Main entry point for the NYC Taxi S3 ingestion pipeline
# Orchestrates file discovery, S3 upload, and Snowflake logging
import sys
from pathlib import Path
if __name__ == "__main__":
    PROJECT_ROOT = Path(__file__).resolve().parents[3]
    sys.path.insert(0, str(PROJECT_ROOT))
    print("Project root added to sys.path[0]:", sys.path[0])

# Core pipeline orchestration
from nyc_taxi.ingestion.core.pipeline import IngestionPipeline

# Infrastructure components
from nyc_taxi.ingestion.infra.local_finder import LocalFileFinder
from nyc_taxi.ingestion.infra.local_archiver import ArchiveLocalFiles
from nyc_taxi.ingestion.infra.s3_uploader import S3Uploader
from nyc_taxi.ingestion.infra.snowflake_system_event_log import SnowflakeLoadLogRepository

# Configuration and environment setup
from nyc_taxi.ingestion.config.settings import S3Config, SnowflakeConfig
from dotenv import load_dotenv
load_dotenv()

def main():
    """Execute the complete NYC Taxi data ingestion pipeline.
    
    The pipeline:
    1. Loads environment configuration from .env file
    2. Discovers local files to upload
    3. Uploads files to S3
    4. Logs events to Snowflake
    5. Archives local files after successful upload
    """
    
    # Load environment variables from .env file

    
    # Initialize configurations from environment variables
    s3_config = S3Config.from_env()
    snowflake_config = SnowflakeConfig.from_env().to_connector_kwarg()

    # Instantiate pipeline components
    # Local file discovery service
    filefinder = LocalFileFinder()
    
    # S3 upload handler with 'raw' folder prefix
    uploader = S3Uploader(config=s3_config, base_prefix='raw')
    
    # Snowflake event logger for tracking upload status
    loadLogReposetory = SnowflakeLoadLogRepository(conn_params=snowflake_config)
    
    # Local file archiver for post-upload cleanup
    archive = ArchiveLocalFiles()
    
    # Execute the complete ingestion pipeline with all components
    IngestionPipeline(filefinder, uploader, loadLogReposetory, archive).run()

if __name__=='__main__':
    # Run the ingestion pipeline when script is executed directly
    main()