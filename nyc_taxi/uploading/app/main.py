from nyc_taxi.uploading.core.pipeline import IngestionPipeline
from nyc_taxi.uploading.infra.local_finder import LocalFileFinder
from nyc_taxi.uploading.infra.local_archiver import ArchiveLocalFiles
from nyc_taxi.uploading.config.settings import MyLocalData
from nyc_taxi.uploading.infra.s3_uploader import S3Uploader
from nyc_taxi.uploading.config.settings import S3Config
from dotenv import load_dotenv
import os

def main():

    # set configurations:
    path = MyLocalData.loacl_path
    load_dotenv()
    config_s3 = S3Config(
        aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID", None),
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY", None),
        region_name = os.getenv("AWS_REGION", None),
        bucket_name = os.getenv("S3_BUCKET_NAME", None),
        local_file_extension = os.getenv("LOCAL_FILE_EXTENSION", None),
    ) 

    # classes instantiation:
    filefinder = LocalFileFinder(path)
    uploader = S3Uploader(config=config_s3, base_prefix='raw')
    # loadLogReposetory = 
    archive = ArchiveLocalFiles()
    
    # IngestionPipeline(filefinder, Uploader, loadLogReposetory, archive)

if __name__=='__main__':
    main()