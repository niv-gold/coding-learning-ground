# Ingestion pipeline orchestration
# Coordinates file discovery, upload, logging, and archival operations

from __future__ import annotations
from nyc_taxi.ingestion.core.ports import FileFinder, Uploader, Archiver, LoadLogRepository
import uuid

class IngestionPipeline:
    """Orchestrates the complete data ingestion workflow.
    
    This pipeline coordinates:
    1. File discovery from local source
    2. Deduplication against previously loaded files
    3. Upload to S3 storage
    4. Archival of processed files locally
    5. Event logging to Snowflake for audit trail
    
    Args:
        finder (FileFinder): Service to discover local files
        uploader (Uploader): Service to upload files to S3
        loadLogReposetory (LoadLogRepository): Service for event logging
        archiver (Archiver): Service to archive processed files
        component (str): Identifier for this pipeline component (default: "LOCAL_TO_S3")
    """
    def __init__(self, finder: FileFinder, uploader: Uploader, loadLogReposetory: LoadLogRepository, archiver: Archiver, component: str = "LOCAL_TO_S3"):
        # Initialize pipeline components
        self.finder = finder
        self.uploader = uploader
        self.log_repo = loadLogReposetory
        self.archiver = archiver
        self.component = component
        # Entity ID strategy: use stable_key (file+size+timestamp) for deduplication
        self.entity_id_mode: str = "stable_key",  # "stable_key" or "file_name"

    def run(self) -> None:
        """Execute the complete ingestion pipeline.
        
        Process flow:
        1. Initialize pipeline run and log start event
        2. Discover all files from local source
        3. Check which files have already been loaded
        4. Filter to only new files (deduplication)
        5. Process each new file: upload, log, archive
        6. Finalize with summary metrics and completion event
        """
        # Generate unique run ID for tracking this execution
        run_id = str(uuid.uuid4())
        # Log pipeline initialization
        self.log_repo.log_run_started(
            event_id=str(uuid.uuid4()),
            run_id=run_id,
            component=self.component,
            message="Pipeline run started",
            metadata={"component": self.component, "entity_id_mode": self.entity_id_mode},
        )

        # Phase 1: Discover all files in local source directory
        files = self.finder.list_files()
        # Handle case where no files are found
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

        # Phase 2: Check deduplication - identify which files are already loaded
        entity_ids = [f.stable_key for f in files]
        loaded_ids = self.log_repo.already_loaded(entity_ids)

        # Phase 3: Filter to only new files (not previously loaded)
        new_files = [f for f in files if f.stable_key not in loaded_ids]
        skipped_count = len(files) - len(new_files)
        print(f"Discovered: {len(files)}, New: {len(new_files)}, Skipped: {skipped_count}")

        # Initialize counters for tracking success/failure during processing
        success_count = 0
        failure_count = 0

        # Phase 4: Process each new file
        for f in new_files:
            eid = f.stable_key
            event_id = str(uuid.uuid4())
            try:
                # Upload file to S3
                destination = self.uploader.upload(f)

                # Log successful upload with metadata
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

                # Archive the file locally after successful upload
                self.archiver.archive(f)
                success_count += 1
                print(f"SUCCESS: {f.name} -> {destination}")

            # Handle any errors during processing
            except Exception as e:
                # Increment failure counter and log detailed error information
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

        # Phase 5: Finalize pipeline run with summary metrics
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
