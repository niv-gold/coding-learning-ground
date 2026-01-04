from nyc_taxi.uploading.core.ports import Archiver, FileIdentity
from pathlib import Path
from nyc_taxi.uploading.config.settings import MyLocalData
import shutil
from datetime import datetime

class ArchiveLocalFiles(Archiver):

    def __init__(self):        
        self.archive_path: Path = MyLocalData.archive_path
        self.archive_path.mkdir(parents=True, exist_ok=True)

    def archive(self, file: FileIdentity)-> None:
        try:
            src_path = file.path
            trg_path: Path = self.archive_path / file.name

            if trg_path.exists():
                raise FileExistsError()                

            shutil.move(src_path, trg_path)
        except FileNotFoundError:
            print('--> Archive FAILED!!!', '\n', '--> Source file is missing')
            return None
        except PermissionError:
            print('--> Archive FAILED!!!', '\n', '--> Permission issue while archiveing!!!')
            return None
        except FileExistsError:
            print('--> Archive FAILED!!!', '\n', '--> File name already exists in archive!!!')
            return None
        except Exception as e:
            print('--> Archive FAILED!!!', '\n', f'-->  Error message: {e}')
            return None
        print(f'--> {file.name} - SUCCESSULY archived')


if __name__=='__main__':
    file = FileIdentity(Path('/home/niv/home/GitHubeRepos/my_codes/nyc_taxi/uploading/app/data_files/taxi_zone_lookup.csv'),12345,datetime.now())
    ArchiveLocalFiles().archive(file)
