from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Iterable
from .models import FileIdentity

class FileFinder(ABC):
    @abstractmethod
    def list_files(self)-> list[FileIdentity]:
        pass

class Uploader(ABC):
    @abstractmethod
    def upload(self, file: FileIdentity) -> str:
        """Return destination reference (e.g., s3://bucket/key)."""
        pass

class LoadLogRepository(ABC):
    @abstractmethod
    def already_loaded(self, entity_ids: list[str]) -> set[str]:
        """Return subset of entity_ids already loaded successfully."""
        raise NotImplementedError

    @abstractmethod
    def log_success(self, run_id: str, entity_type: str, entity_id: str, component: str, message: str, metadata: dict) -> None:
        raise NotImplementedError

    @abstractmethod
    def log_failure(self, run_id: str, entity_type: str, entity_id: str, component: str, message: str, error_details: str, metadata: dict) -> None:
        raise NotImplementedError