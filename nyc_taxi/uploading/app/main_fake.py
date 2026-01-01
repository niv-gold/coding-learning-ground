from nyc_taxi.uploading.core.ports_fakes import FakeArchiver, FakeFileFinder, FakeUploader, FakeLoadLogRepository
from nyc_taxi.uploading.core.pipeline_fakes import FakeIngestionPipeline

def main()-> None:
    fileFinder = FakeFileFinder()
    uploader = FakeUploader()
    loadLogRepository = FakeLoadLogRepository()
    archive = FakeArchiver()    
    Ingest_inst = FakeIngestionPipeline(fileFinder, uploader, loadLogRepository, archive)
    Ingest_inst.run()
    
if __name__ == '__main__':
    main()