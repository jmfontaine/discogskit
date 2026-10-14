"""Stage file outputs so a run that fails before commit leaves no complete-looking files.

Files are written under their final names into a fresh ``<output>/<entity>.partial-XXXXXXXX/`` directory, so
staging never collides with existing files or another run's staging directory. ``commit()`` then moves them
into ``<output>/<entity>/`` one file at a time. That is not atomic as a set: a failure part-way through, or two
runs committing to the same directory, can leave a mix of files. Keeping the final names (rather than adding a
suffix) keeps the original name stored in gzip headers correct.

Moving the files can fail after hours of parsing, e.g. when ``<output>/<entity>/`` isn't writable or is on another
filesystem. So the constructor makes the same move with a probe file first, and a failed ``commit()`` keeps the
staging directory, which then holds the finished files, and names it in its error.
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
        self._kept = False
        if self.final_dir.exists():
            self._probe()

    def _probe(self) -> None:
        """Fail now, not after the run, if ``commit()`` can't move files into the existing final directory."""
        # Named after the staging directory, which is unique in output_dir, so it can't clash with a real file.
        probe = self.staging_dir / f"{self.staging_dir.name}.probe"
        target = self.final_dir / probe.name
        try:
            probe.touch()
            # The exact call commit() makes, so a cross-filesystem or permission failure shows up here too.
            os.replace(probe, target)
            target.unlink()
        except OSError as exc:
            self.discard()
            # strerror, not the exception: its paths are the probe's, which mean nothing to the user.
            raise OSError(
                f"Can't write output files into {self.final_dir}: {exc.strerror or exc}"
            ) from exc

    def commit(self) -> int:
        """Move every staged file into the final directory. Returns their total size in bytes.

        If a move fails, the staging directory is kept, ``discard()`` no longer deletes it, and the error names it.
        """
        total_bytes = 0
        moved = 0
        try:
            self.final_dir.mkdir(exist_ok=True, parents=True)
            for name in self.filenames:
                final_path = self.final_dir / name
                os.replace(self.staging_dir / name, final_path)
                moved += 1
                total_bytes += final_path.stat().st_size
        except OSError as exc:
            self._kept = True
            raise OSError(
                f"Moving the output into {self.final_dir} failed after {moved} of {len(self.filenames)} files: "
                f"{exc}. The completed files not moved yet are in {self.staging_dir}."
            ) from exc
        shutil.rmtree(self.staging_dir, ignore_errors=True)
        return total_bytes

    def discard(self) -> None:
        """Delete the staging directory, unless a failed ``commit()`` left finished files in it; existing final
        files stay untouched.
        """
        if self._kept:
            return
        # Errors are ignored so cleanup never replaces the error that aborted the run.
        shutil.rmtree(self.staging_dir, ignore_errors=True)

    def path(self, name: str) -> Path:
        return self.staging_dir / name
