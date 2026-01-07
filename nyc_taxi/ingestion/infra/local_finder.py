"""Local file discovery utilities.

Provides `LocalFileFinder`, a small helper to discover files on the
local filesystem matching one or more extensions. The finder returns
`FileIdentity` objects used by the ingestion pipeline for deduplication
and metadata propagation.

This module also includes a tiny utility `sumpos` and custom exception
used for demonstration and simple unit tests.
"""

from __future__ import annotations
from pathlib import Path
from nyc_taxi.ingestion.config.settings import MyLocalData
from nyc_taxi.ingestion.core.ports import FileIdentity, FileFinder
from datetime import datetime
from typing import Iterable, Tuple


class LocalFileFinder(FileFinder):
    """Discover local files with specific extensions.

    Simple helper that searches a directory for files whose names end
    with one of the provided extensions and returns a list of
    `FileIdentity` entries containing path, size and modification time.

    Args:
        source_dir (Path | str | None): Directory to search. If not
            provided, falls back to `MyLocalData.loacl_path` (configured
            project data directory).
        extension (tuple[str, ...]): File extensions to include
            (e.g. ('.csv', '.parquet')). Default is ('.csv', '.parquet').

    Main use (simple):
        - Find all CSV and Parquet files in the configured local folder
        - Return minimal metadata required by the ingestion pipeline
    """

    def __init__(self, source_dir: Path | str | None = None, extension: Tuple[str, ...] = ('.csv', '.parquet')):
        # Accept either a Path or a string; convert to Path when provided
        self.src_dir: Path | None = Path(source_dir) if source_dir else None
        # Keep extensions as a tuple for predictable iteration
        self.extension: Tuple[str, ...] = tuple(extension)

        # If no source provided, use configured local data path
        if not self.src_dir:
            # Note: settings currently expose `loacl_path` (typo kept for compatibility)
            self.src_dir = MyLocalData.loacl_path

    def list_files(self) -> list[FileIdentity]:
        """List discovered files as `FileIdentity` objects.

        Returns:
            list[FileIdentity]: Discovered files with `path`, `size_bytes`,
            and `modified_time` filled in.

        Main use (simple):
            - Called by the pipeline to enumerate candidate files to upload
            - The pipeline will use the returned `stable_key` values for
              deduplication checks
        """
        files: list[FileIdentity] = []

        # Iterate over the requested extensions and glob files
        for ext in self.extension:
            # Use glob on the configured directory
            for f in self.src_dir.glob(f'*{ext}'):
                f_st = f.stat()
                files.append(
                    FileIdentity(
                        path=f,
                        size_bytes=f_st.st_size,
                        modified_time=datetime.fromtimestamp(f_st.st_mtime),
                    )
                )

        return files


if __name__ == '__main__':
    # Quick manual check when running this module directly
    print('~' * 80, '\n', '--> Start')
    finder = LocalFileFinder()
    for file in finder.list_files():
        print(f'file - {file.name}')
    print('--> End')
