from __future__ import annotations
from nyc_taxi.uploading.core.models import FileIdentity
from nyc_taxi.uploading.core.ports import Uploader, Archiver, LoadLogRepository, FileFinder
from datetime import datetime
from pathlib import Path


now = datetime(2025, 12, 23, 10, 41, 23, 422997)

class FakeFileFinder(FileFinder):
    def list_files(self)-> list[FileIdentity]:        
        f_list = [FileIdentity(Path('file_1.csv'),10,now),
                  FileIdentity(Path('file_2.csv'),23,now),
                  FileIdentity(Path('file_3.csv'),35,now),
                  FileIdentity(Path('file_4.csv'),49,now),
                  FileIdentity(Path('file_5.csv'),50,now)
        ]
        return f_list

class FakeUploader(Uploader):
    def upload(self, file: FileIdentity)-> str:
        print (f'Upload_Success --> S3:://nyc_taxi/raw/{file.name}')
        return f'S3:://nyc_taxi/raw/{file.name}'

class FakeArchiver(Archiver):
    def archive(self, file: FileIdentity)-> None:
        print(f'Archive_Success --> file {file.name} Archived')

class FakeLoadLogRepository(LoadLogRepository):
    def already_loaded(self, file_keys: list[str]) -> set[str]:
        exists_keys = ['file_1.csv|10|1766504483','file_2.csv|23|1766504483','file_3.csv|35|1766504483']
        used_keys = [key for key in file_keys if key in exists_keys]
        return used_keys
    
    def log_success(self, file: FileIdentity, destination: str)-> None:
        print(f'Log_Success --> File name: {file.name}, logged at: {destination}')

    def log_failure(self, file: FileIdentity, error: str)-> None:
        print(f'file name: {file.name}, FAIL logging' )
        print(f'error message: {error}')