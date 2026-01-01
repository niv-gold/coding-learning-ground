from __future__ import annotations
from nyc_taxi.uploading.core.ports import FileFinder, Uploader, LoadLogRepository, Archiver

class IngestionPipeline:
    def __init__(self, finder: FileFinder, uploader: Uploader, log_repo: LoadLogRepository, archiver: Archiver):
        self.finder = finder
        self.uploader = uploader
        self.log_repo = log_repo
        self.archiver = archiver

    def run(self) -> None:
        files = self.finder.list_files()
        if not files:
            print("No files found.")
            return

        keys = [f.stable_key for f in files]
        loaded_keys = self.log_repo.already_loaded(keys)

        new_files = [f for f in files if f.stable_key not in loaded_keys]
        print(f"Discovered: {len(files)}, New: {len(new_files)}")

        # for f in new_files:
        #     try:
        #         dest = self.uploader.upload(f)
        #         self.log_repo.log_success(f, dest)
        #         self.archiver.archive(f)
        #         print(f"SUCCESS: {f.name} -> {dest}")
        #     except Exception as e:
        #         self.log_repo.log_failure(f, str(e))
        #         print(f"FAIL: {f.name} -> {e}")
