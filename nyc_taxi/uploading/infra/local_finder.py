from __future__ import  annotations
from pathlib import Path
from nyc_taxi.uploading.core.ports import FileIdentity, FileFinder
from datetime import datetime

class LocalFileFinder(FileFinder):
    def __init__(self, source_dir: Path, extention: tuple[str]=['.csv','.parquet']):
        self.src_dir = source_dir
        self.extention = extention

    def list_files(self)-> list[FileIdentity]:
        files: list[FileIdentity]=[]
        for ext in self.extention:
            for f in self.src_dir.rglob(f'*{ext}'):
                f_st = f.stat()
                files.append(FileIdentity(
                                path=f, 
                                size_byte=f_st.st_size, 
                                modified_time=datetime.fromtimestamp(f_st.st_mtime)
                                )
                            )
        return files
    
if __name__=='__main__':
    
    print('~'*150, '\n', '--> Start')
    path = Path('/home/niv/home/GitHubeRepos/my_codes/nyc_taxi/uploading/app/data_files')
    finder = LocalFileFinder(path)
    files = finder.list_files()
    for file in finder.list_files():
        print(f'file - {file.name}')
    print('--> End')
