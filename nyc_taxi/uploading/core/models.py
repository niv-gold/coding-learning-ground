# Meaning: Your system needs a consistent way to represent a file (name, metadata, stable key)

from __future__ import annotations
from datetime import datetime
from dataclasses import dataclass 
from pathlib import Path

@dataclass(frozen=True)
class FileIdentity:
    path: Path
    size_bytes: int
    modified_time: datetime 
    
    @property
    def name(self)-> str:
        return self.path.name
    
    @property
    def stable_key(self) -> str:
        """
        A practical stable identifier for 'already loaded?' checks.
        (For stronger guarantees later: add checksum.)
        """
        return f"{self.name}|{self.size_bytes}|{int(self.modified_time.timestamp())}"

if __name__ == '__main__':

    f  = FileIdentity(Path('nyc_taxi/uploading/app/data_files/taxi_zone_lookup.csv'),12345,datetime.now())
    if f.path.exists():
        print(f'file Exist - {f.stable_key}')
    else:
        print('file Not exists')
    

    

