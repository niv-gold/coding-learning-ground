from __future__ import annotations
from datetime import datetime
from nyc_taxi.uploading.core.ports import FileFinder, Uploader, LoadLogRepository, Archiver
from nyc_taxi.uploading.core.ports_fakes import FakeArchiver, FakeFileFinder, FakeLoadLogRepository, FakeUploader
from nyc_taxi.uploading.infra.local_finder import LocalFileFinder

class FakeIngestionPipeline:
    def __init__(self, filefinder: FileFinder, uploder: Uploader, loadlogrep: LoadLogRepository, archive: Archiver):
        self.filefinder = filefinder
        self.uploder = uploder
        self.loadlogrep = loadlogrep
        self.archive = archive

    def run(self)-> None:
        
        print(f'--> Start session: {datetime.now()}\n')
        ingest_files = self.filefinder.list_files()
        ingest_files_keys = [file.stable_key for file in ingest_files]

        if not ingest_files:
            print( 'No Files were found!!!')
            return

        print('Ingested files:')
        for file in ingest_files:
            print(file.stable_key)
        print('\n')

        already_loaded_key = self.loadlogrep.already_loaded(ingest_files_keys)
        print('Already loade files:')
        for key in already_loaded_key:
            print(key)
        print('\n')

        new_files = [file for file in ingest_files if file.stable_key not in already_loaded_key]
        print('New files key:')
        for file in new_files:
            print(file.stable_key)
        print('\n')

        # try:
        #     print('Upload files:')
        #     for file in new_files:
        #         trg = self.uploder.upload(file)
        #         self.loadlogrep.log_success(file, trg)
        #         self.archive.archive(file)
        #     print('\n')
        # except Exception as e:
        #     print(f'\n !!! Error: loading file into storage !!!')
        #     self.loadlogrep.log_failure(file=file, error='divided by zero\n')
            

        print(f'--> End session: {datetime.now()}')              


if __name__ == '__main__':
    fake_filefinder = FakeFileFinder()
    fake_uploder = FakeUploader()
    fake_loadlogrep = FakeLoadLogRepository()
    fake_archive = FakeArchiver()

    inst = FakeIngestionPipeline(fake_filefinder, fake_uploder, fake_loadlogrep, fake_archive)
    inst.run()