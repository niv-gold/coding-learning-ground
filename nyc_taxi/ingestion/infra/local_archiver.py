"""Local archiver implementation.

Provides an `Archiver` implementation that moves processed files into
an archive directory under `MyLocalData.archive_path`.

Behavior:
- Ensures archive directory exists on initialization
- Moves the source file to the archive folder using `shutil.move`
- Handles common errors and prints concise diagnostic messages
"""

from nyc_taxi.ingestion.core.ports import Archiver, FileIdentity
from pathlib import Path
from nyc_taxi.ingestion.config.settings import MyLocalData
import shutil
from datetime import datetime


class ArchiveLocalFiles(Archiver):
    """Archive files locally by moving them into the configured archive folder.

    This class is intended to be used after a file has been successfully
    uploaded. It preserves the original file name in the archive directory.
    """

    def __init__(self):
        # Resolve archive path from configuration and ensure it exists
        self.archive_path: Path = MyLocalData.archive_path
        self.archive_path.mkdir(parents=True, exist_ok=True)

    def archive(self, file: FileIdentity) -> None:
        """Move `file` into the archive directory.

        Args:
            file (FileIdentity): Metadata for the file to archive. `file.path`
                must point to the existing source file on disk.

        Returns:
            None

        Notes:
            - If the target file name already exists in the archive, the
              operation is aborted to avoid overwriting.
            - Errors are reported via printed messages to keep this
              implementation lightweight for CLI usage.
        """
        try:
            src_path = file.path
            trg_path: Path = self.archive_path / file.name

            # Prevent accidental overwrite in the archive
            if trg_path.exists():
                raise FileExistsError()                

            # Move the file into the archive (preserves file metadata)
            shutil.move(src_path, trg_path)

        except FileNotFoundError:
            # Source is missing; caller should ensure file exists before archiving
            print('--> Archive FAILED!!!', '\n', '--> Source file is missing')
            return None

        except PermissionError:
            # Likely insufficient filesystem permissions
            print('--> Archive FAILED!!!', '\n', '--> Permission issue while archiving!!!')
            return None

        except FileExistsError:
            # Do not overwrite existing archived files
            print('--> Archive FAILED!!!', '\n', '--> File name already exists in archive!!!')
            return None

        except Exception as e:
            # Catch-all for unexpected errors; surface details for debugging
            print('--> Archive FAILED!!!', '\n', f'-->  Error message: {e}')
            return None

        # Success acknowledgement
        print(f'--> {file.name} - SUCCESSFULLY archived')


if __name__ == '__main__':
    # Simple local test harness
    file = FileIdentity(
        Path('/home/niv/home/GitHubeRepos/my_codes/nyc_taxi/ingestion/app/data_files/taxi_zone_lookup.csv'),
        12345,
        datetime.now(),
    )
    ArchiveLocalFiles().archive(file)
