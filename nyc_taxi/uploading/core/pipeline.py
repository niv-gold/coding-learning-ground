from __future__ import annotations
from nyc_taxi.uploading.core.ports import FileFinder, Uploader, Archiver, LoadLogRepository
import uuid

class IngestionPipeline:
    def __init__(self, finder: FileFinder, uploader: Uploader, loadLogReposetory: LoadLogRepository, archiver: Archiver, component: str = "LOCAL_TO_S3"):
        self.finder = finder
        self.uploader = uploader
        self.log_repo = loadLogReposetory
        self.archiver = archiver
        self.component = component
        self.entity_id_mode: str = "stable_key",  # "stable_key" or "file_name"

    def run(self) -> None:
        run_id = str(uuid.uuid4())
        self.log_repo.log_run_started(
            event_id=str(uuid.uuid4()),
            run_id=run_id,
            component=self.component,
            message="Pipeline run started",
            metadata={"component": self.component, "entity_id_mode": self.entity_id_mode},
        )

        files = self.finder.list_files()
        if not files:
            self.log_repo.log_run_finished(
                event_id=str(uuid.uuid4()),
                run_id=run_id,
                component=self.component,
                status="SUCCESS",
                message="No files found. Run finished.",
                metadata={"files_found": 0},
            )
            print("No files found.")
            return

        entity_ids = [f.stable_key for f in files]
        loaded_ids = self.log_repo.already_loaded(entity_ids)

        new_files = [f for f in files if f.stable_key not in loaded_ids]
        skipped_count = len(files) - len(new_files)
        print(f"Discovered: {len(files)}, New: {len(new_files)}, Skipped: {skipped_count}")

        success_count = 0
        failure_count = 0

        for f in new_files:
            eid = f.stable_key
            event_id = str(uuid.uuid4())
            try:
                destination = self.uploader.upload(f)

                self.log_repo.log_ingest_success(
                    event_id=event_id,
                    run_id=run_id,
                    component=self.component,
                    entity_id=eid,
                    message=f"Uploaded {f.name}",
                    metadata={
                        "file_name": f.name,
                        "entity_id": eid,
                        "size_bytes": f.size_bytes,
                        "modified_time": f.modified_time.isoformat(),
                        "destination": destination,
                    },
                )

                self.archiver.archive(f)
                success_count += 1
                print(f"SUCCESS: {f.name} -> {destination}")

            except Exception as e:
                failure_count += 1
                self.log_repo.log_ingest_failure(
                    event_id=event_id,
                    run_id=run_id,
                    component=self.component,
                    entity_id=eid,
                    message=f"Failed to ingest {f.name}",
                    error_code=None,
                    error_details=str(e),
                    metadata={
                        "file_name": f.name,
                        "entity_id": eid,
                        "size_bytes": f.size_bytes,
                        "modified_time": f.modified_time.isoformat(),
                    },
                )
                print(f"FAIL: {f.name} -> {e}")

        run_status = "SUCCESS" if failure_count == 0 else "FAILURE"
        self.log_repo.log_run_finished(
            event_id=str(uuid.uuid4()),
            run_id=run_id,
            component=self.component,
            status=run_status,
            message="Pipeline run finished",
            metadata={
                "files_found": len(files),
                "files_new": len(new_files),
                "files_skipped": skipped_count,
                "files_success": success_count,
                "files_failure": failure_count,
            },
        )
