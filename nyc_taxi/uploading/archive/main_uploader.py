from nyc_taxi.uploading.archive.s3_client import S3Client
from nyc_taxi.uploading.archive.config import S3Config
from nyc_taxi.uploading.archive.local_file_manager import LocalFilesManager

def run():

    lfm_inst = LocalFilesManager(s3_config)
    lfm_inst.run()

    s3_config = S3Config()
    s3_inst = S3Client(s3_config)
    s3_inst.extract_and_load_file(
        local_file_path='/home/niv/home/GitHubeRepos/my_codes/nyc_taxi/upload/data files/taxi_zone_lookup.csv',
        s3_key='taxi_zone_lookup123.csv'
    )

if __name__ == "__main__":
    run()