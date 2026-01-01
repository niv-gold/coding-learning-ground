from __future__ import annotations
from nyc_taxi.uploading.config.settings import S3Config
from nyc_taxi.uploading.core.ports import Uploader, FileIdentity
from nyc_taxi.uploading.infra.s3_clinet import S3Client
from pathlib import Path
from datetime import datetime
from nyc_taxi.uploading.config.settings import MyLocalData
from dotenv import load_dotenv
import os

class S3Uploader(Uploader):

    def __init__(self, config: S3Config, bucket: str = "", base_prefix: str = ""):
        """
        bucket: S3 bucket name (NO slash)
        base_prefix: logical root folder in S3, e.g. 'raw'
        """
        self.config = config        
        self.base_prefix = base_prefix.strip("/")
        self.s3_clinet = S3Client(self.config).create_s3_clinet()
        if bucket:
            self.bucket = bucket.strip('/')
        else: 
            self.bucket = self.config.bucket_name

    def build_s3_key(self, file: FileIdentity)-> str:
        ''' Build the full S3 key for the file.
            Example result:
            raw/taxi_zone_lookup.csv
        '''
        if self.base_prefix:
            return f'{self.base_prefix}/{file.name}'
        return file.name

    def upload(self, file: FileIdentity)-> None:
        """
        Upload local file to S3 using an explicit bucket + s3_key.
        """
        s3_key = self.build_s3_key(file)
        self.s3_clinet.upload_file(
            Filename=str(file.path),
            Bucket=self.bucket,
            Key=s3_key
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