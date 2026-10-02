"""Stage file outputs so a run that fails before commit leaves no complete-looking files.

Files are written under their final names into a fresh ``<output>/<entity>.partial-XXXXXXXX/`` directory, so
staging never collides with existing files or another run's staging directory. ``commit()`` then moves them
into ``<output>/<entity>/`` one file at a time. That is not atomic as a set: a failure part-way through, or two
runs committing to the same directory, can leave a mix of files. Keeping the final names (rather than adding a
suffix) keeps the original name stored in gzip headers correct.
"""

from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable


class StagedFiles:
    """One entity's output files, staged until the run completes."""

    def __init__(
        self, output_dir: Path, entity_name: str, filenames: Iterable[str]
    ) -> None:
        self.filenames = list(filenames)
        self.final_dir = output_dir / entity_name
        output_dir.mkdir(exist_ok=True, parents=True)
        # Visible (not dot-prefixed) so leftovers from a killed process, which can
        # be many GB, are easy to spot and delete.
        self.staging_dir = Path(
            tempfile.mkdtemp(dir=output_dir, prefix=f"{entity_name}.partial-")
        )

    def commit(self) -> int:
        """Move every staged file into the final directory. Returns their total size in bytes."""
        self.final_dir.mkdir(exist_ok=True, parents=True)
        total_bytes = 0
        for name in self.filenames:
            final_path = self.final_dir / name
            os.replace(self.staging_dir / name, final_path)
            total_bytes += final_path.stat().st_size
        shutil.rmtree(self.staging_dir, ignore_errors=True)
        return total_bytes

    def discard(self) -> None:
        """Delete the staging directory; existing final files stay untouched."""
        # Errors are ignored so cleanup never replaces the error that aborted the run.
        shutil.rmtree(self.staging_dir, ignore_errors=True)

    def path(self, name: str) -> Path:
        return self.staging_dir / name
