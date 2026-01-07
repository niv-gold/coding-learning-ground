# Data models for NYC Taxi pipeline
# Purpose: Provide a consistent way to represent file metadata and identity
# This allows for tracking file state, deduplication, and stable file identification

from __future__ import annotations
from datetime import datetime
from dataclasses import dataclass 
from pathlib import Path

@dataclass(frozen=True)
class FileIdentity:
    """Immutable representation of a file's identity and metadata.
    
    This class encapsulates essential file information for tracking,
    deduplication, and consistency checks across the ingestion pipeline.
    
    Attributes:
        path (Path): Full file path
        size_bytes (int): File size in bytes
        modified_time (datetime): Last modification timestamp
    """
    path: Path
    size_bytes: int
    modified_time: datetime 
    
    @property
    def name(self)-> str:
        """Extract the file name (without directory path) from the full path."""
        return self.path.name
    
    @property
    def stable_key(self) -> str:
        """Generate a practical stable identifier for deduplication checks.
        
        Combines file name, size, and modification timestamp to create
        a unique key. This allows the system to detect if a file has
        already been processed or loaded.
        
        Note: For stronger uniqueness guarantees in production, consider
        adding file checksum (MD5/SHA256) in the future.
        
        Returns:
            str: A pipe-separated key in format "name|size_bytes|timestamp"
        """
        return f"{self.name}|{self.size_bytes}|{int(self.modified_time.timestamp())}"

if __name__ == '__main__':

    f  = FileIdentity(Path('nyc_taxi/ingestion/app/data_files/taxi_zone_lookup.csv'),12345,datetime.now())
    if f.path.exists():
        # Test FileIdentity creation and validation    
        # Check if the test file exists and display its stable key
        print(f'file Exist - {f.stable_key}')
    else:
        print('file Not exists')
    

    

