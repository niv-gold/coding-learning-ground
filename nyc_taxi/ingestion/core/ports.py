# Port definitions (abstract interfaces) for the ingestion pipeline
# These abstract base classes define contracts for pluggable implementations
# This follows the ports & adapters (hexagonal architecture) pattern

from __future__ import annotations
from abc import ABC, abstractmethod
from .models import FileIdentity

class FileFinder(ABC):
    """Interface for discovering files from a source location.
    
    Implementations may discover files from local filesystem, cloud storage,
    or other sources. Files are represented as FileIdentity objects with
    metadata needed for deduplication.
    """
    @abstractmethod
    def list_files(self)-> list[FileIdentity]:
        """Discover and return all files from the source.
        
        Returns:
            list[FileIdentity]: List of discovered files with metadata
        """
        pass

class Uploader(ABC):
    """Interface for ingestion files to a destination storage.
    
    Implementations handle transferring file content to cloud storage
    such as S3, with support for folder prefixes and metadata.
    """
    @abstractmethod
    def upload(self, file: FileIdentity) -> str:
        """Upload a file to the destination storage.
        
        Args:
            file (FileIdentity): File metadata and location to upload
            
        Returns:
            str: Destination reference (e.g., s3://bucket/key)
            
        Raises:
            Exception: If upload fails
        """
        pass
class Archiver(ABC):
    """Interface for archiving processed files locally.
    
    Implementations handle moving or copying successfully processed files
    to an archive location for retention and organizational purposes.
    """
    @abstractmethod
    def archive(self, file: FileIdentity) -> None:
        """Archive a file after successful processing.
        
        Args:
            file (FileIdentity): File metadata to archive
            
        Raises:
            Exception: If archival fails
        """
        raise NotImplementedError

class LoadLogRepository(ABC):
    """Interface for event logging and audit trail management.
    
    Implementations persist pipeline events to a data warehouse (e.g., Snowflake)
    for monitoring, debugging, and compliance purposes. This provides a complete
    audit trail of all files processed and any errors encountered.
    """
    
    @abstractmethod
    def already_loaded(self, entity_ids: list[str]) -> set[str]:
        """Check which files have already been successfully loaded.
        
        Used for deduplication to avoid reprocessing files.
        
        Args:
            entity_ids (list[str]): List of entity identifiers (stable keys) to check
            
        Returns:
            set[str]: Subset of entity_ids that have been successfully loaded before
        """
        raise NotImplementedError

    @abstractmethod
    def log_success(self, *, event_id: str, run_id: str, component: str, entity_type: str, entity_id: str, message: str, metadata: dict) -> None:
        """Log a successful operation with an explicit event id.

        Args:
            event_id (str): Unique identifier for this event (caller-generated).
            run_id (str): Unique identifier for this pipeline run.
            component (str): Pipeline component that performed the operation.
            entity_type (str): Type of entity (e.g., 'FILE' or 'RUN').
            entity_id (str): Unique identifier for the entity.
            message (str): Human-readable success message.
            metadata (dict): Additional context and details.
        """
        raise NotImplementedError

    @abstractmethod
    def log_failure(self, *, event_id: str, run_id: str, component: str, entity_type: str, entity_id: str, message: str, error_details: str, metadata: dict) -> None:
        """Log a failed operation with an explicit event id.

        Args:
            event_id (str): Unique identifier for this event (caller-generated).
            run_id (str): Unique identifier for this pipeline run.
            component (str): Pipeline component that failed.
            entity_type (str): Type of entity (e.g., 'FILE' or 'RUN').
            entity_id (str): Unique identifier for the entity.
            message (str): Human-readable failure message.
            error_details (str): Detailed error information/stack trace.
            metadata (dict): Additional context and details.
        """
        raise NotImplementedError
    
    @abstractmethod
    def log_run_started(self, *, event_id: str, run_id: str, component: str, message: str, metadata: dict) -> None:
        """Log the start of a pipeline run.

        Args:
            event_id (str): Unique identifier for this event (caller-generated).
            run_id (str): Unique identifier for this pipeline run.
            component (str): Pipeline component identifier.
            message (str): Descriptive message.
            metadata (dict): Additional context and details.
        """
        raise NotImplementedError

    @abstractmethod
    def log_run_finished(self, *, event_id: str, run_id: str, component: str, status: str, message: str, metadata: dict) -> None:
        """Log the completion of a pipeline run.

        Args:
            event_id (str): Unique identifier for this event (caller-generated).
            run_id (str): Unique identifier for this pipeline run.
            component (str): Pipeline component identifier.
            status (str): Final run status ('SUCCESS'|'FAILURE').
            message (str): Descriptive message.
            metadata (dict): Summary metrics and additional context.
        """
        raise NotImplementedError

    @abstractmethod
    def log_ingest_success(self, *, event_id: str, run_id: str, component: str, entity_id: str, message: str, metadata: dict) -> None:
        """Log successful ingestion of a file.

        Args:
            event_id (str): Unique event id (caller-generated).
            run_id (str): Unique identifier for this pipeline run.
            component (str): Pipeline component identifier.
            entity_id (str): Unique identifier for the ingested entity.
            message (str): Descriptive message.
            metadata (dict): File metadata and destination information.
        """
        raise NotImplementedError

    @abstractmethod
    def log_ingest_failure(self, *, event_id: str, run_id: str, component: str, entity_id: str, message: str, error_details: str, metadata: dict) -> None:
        """Log failed ingestion of a file.

        Args:
            event_id (str): Unique event id (caller-generated).
            run_id (str): Unique identifier for this pipeline run.
            component (str): Pipeline component identifier.
            entity_id (str): Unique identifier for the ingested entity.
            message (str): Descriptive message.
            error_details (str): Detailed error information/stack trace.
            metadata (dict): File metadata and context.
        """
        raise NotImplementedError
