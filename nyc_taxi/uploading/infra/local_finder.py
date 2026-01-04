from __future__ import  annotations
from pathlib import Path
from nyc_taxi.uploading.config.settings import MyLocalData
from nyc_taxi.uploading.core.ports import FileIdentity, FileFinder
from datetime import datetime

class LocalFileFinder(FileFinder):
    def __init__(self, source_dir: Path='', extention: tuple[str]=['.csv','.parquet']):
        self.src_dir = source_dir
        self.extention = extention
        if not self.src_dir: 
            self.src_dir = MyLocalData.loacl_path

    def list_files(self)-> list[FileIdentity]:
        files: list[FileIdentity]=[]
        for ext in self.extention:
            for f in self.src_dir.glob(f'*{ext}'):
                f_st = f.stat()
                files.append(FileIdentity(
                                path=f, 
                                size_bytes=f_st.st_size, 
                                modified_time=datetime.fromtimestamp(f_st.st_mtime)
                                )
                            )
        return files
    
if __name__=='__main__':
    
    print('~'*150, '\n', '--> Start')
    path = Path('/home/niv/home/GitHubeRepos/my_codes/nyc_taxi/uploading/app/data_files')
    finder = LocalFileFinder()
    files = finder.list_files()
    for file in finder.list_files():
        print(f'file - {file.name}')
    print('--> End')
