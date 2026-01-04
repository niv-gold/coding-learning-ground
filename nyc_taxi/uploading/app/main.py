from nyc_taxi.uploading.core.pipeline import IngestionPipeline
from nyc_taxi.uploading.infra.local_finder import LocalFileFinder
from nyc_taxi.uploading.infra.local_archiver import ArchiveLocalFiles
from nyc_taxi.uploading.infra.s3_uploader import S3Uploader
from nyc_taxi.uploading.config.settings import S3Config, SnowflakeConfig
from nyc_taxi.uploading.infra.snowflake_system_event_log import SnowflakeLoadLogRepository
from dotenv import load_dotenv

def main():

    # set configurations:
    load_dotenv()
    s3_config = S3Config.from_env()
    snowflake_config = SnowflakeConfig.from_env().to_connector_kwarg()

    # classes instantiation:
    filefinder = LocalFileFinder()
    uploader = S3Uploader(config=s3_config, base_prefix='raw')
    loadLogReposetory = SnowflakeLoadLogRepository(conn_params=snowflake_config)
    archive = ArchiveLocalFiles()
    
    IngestionPipeline(filefinder, uploader, loadLogReposetory, archive).run()

if __name__=='__main__':
    main()