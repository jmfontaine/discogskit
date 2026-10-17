"""Stage file outputs so a run that fails before commit leaves no complete-looking files.

Files are written under their final names into a fresh ``<output>/<entity>.partial-XXXXXXXX/`` directory, so
staging never collides with existing files or another run's staging directory. ``commit()`` then moves them
into ``<output>/<entity>/`` one file at a time. That is not atomic as a set: a failure part-way through, or two
runs committing to the same directory, can leave a mix of files. Keeping the final names (rather than adding a
suffix) keeps the original name stored in gzip headers correct.

Moving the files can fail after hours of parsing, e.g. when ``<output>/<entity>/`` isn't writable or is on another
filesystem. So the constructor makes the same move with a probe file first, and a failed ``commit()`` keeps the
staging directory, which then holds the finished files, and names it in its error. ``check_output_dir()`` makes the
same probes before the run, when the staging directory doesn't exist yet, so an unusable output fails before the
input is decompressed.

A run that fails removes the directories it created (``<output>``, its missing parents, ``<output>/<entity>/``) if
they're empty, so no empty directory looks like output. Directories that existed before the run stay.
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
        # Absolute, so cleanup acts on these directories even if the working directory changes during the run.
        output_dir = output_dir.absolute()
        self.final_dir = output_dir / entity_name
        # The directories this run created, deepest first: discard() removes the ones still empty.
        self._created_dirs: list[Path] = []
        try:
            self.staging_dir = _make_staging_dir(
                output_dir, entity_name, self._created_dirs
            )
        except BaseException as exc:
            # No writer holds this object yet to discard() it.
            _remove_if_empty(self._created_dirs)
            if isinstance(exc, OSError):
                # strerror, not the exception: its path may be a parent's or the staging directory's.
                raise OSError(
                    f"Can't write output files into {output_dir}: {exc.strerror or exc}"
                ) from exc
            raise
        self._kept = False
        if self.final_dir.exists():
            try:
                _probe(self.final_dir, self.staging_dir, self.final_dir)
            except OSError:
                self.discard()
                raise

    def commit(self) -> int:
        """Move every staged file into the final directory. Returns their total size in bytes.

        If a move fails, the staging directory is kept, ``discard()`` no longer deletes it, and the error names it.
        A final directory this call created is removed again if no file reached it, also when ``discard()`` follows
        another failure, such as Ctrl+C.
        """
        total_bytes = 0
        moved = 0
        created_final = False
        try:
            try:
                self.final_dir.mkdir()
            except FileExistsError:
                pass
            else:
                created_final = True
                self._created_dirs.insert(0, self.final_dir)
            for name in self.filenames:
                final_path = self.final_dir / name
                os.replace(self.staging_dir / name, final_path)
                moved += 1
                total_bytes += final_path.stat().st_size
        except OSError as exc:
            self._kept = True
            if created_final:
                _remove_if_empty([self.final_dir])
            raise OSError(
                f"Moving the output into {self.final_dir} failed after {moved} of {len(self.filenames)} files: "
                f"{exc}. The completed files not moved yet are in {self.staging_dir}."
            ) from exc
        shutil.rmtree(self.staging_dir, ignore_errors=True)
        return total_bytes

    def discard(self) -> None:
        """Delete the staging directory, unless a failed ``commit()`` left finished files in it, then the directories
        this run created, if they're empty; existing final files stay untouched.
        """
        if self._kept:
            return
        # Errors are ignored so cleanup never replaces the error that aborted the run.
        shutil.rmtree(self.staging_dir, ignore_errors=True)
        _remove_if_empty(self._created_dirs)

    def path(self, name: str) -> Path:
        return self.staging_dir / name


def check_output_dir(output_dir: Path, entity_name: str) -> None:
    """Raise ``OSError`` now, before any work, if a run can't write ``entity_name``'s files into ``output_dir``.

    Probes the nearest existing directory of ``output_dir``, where ``StagedFiles`` creates the missing ones and its
    staging directory, and, if ``<output>/<entity>/`` exists, the move ``commit()`` makes into it.
    """
    # Absolute, so the walk up ends at the filesystem root rather than at a relative path's first part.
    output_dir = output_dir.absolute()
    # A path that exists but isn't a directory is kept: creating the probe in it fails, as the run would.
    nearest = next(d for d in (output_dir, *output_dir.parents) if d.exists())
    _probe(output_dir, nearest)
    final_dir = output_dir / entity_name
    if final_dir.exists():
        _probe(final_dir, output_dir, final_dir)


def _probe(shown: Path, directory: Path, final_dir: Path | None = None) -> None:
    """Create a file in ``directory``, move it into ``final_dir`` if given, then delete it.

    Raises ``OSError`` "Can't write output files into ``shown``" if any step fails.
    """
    try:
        fd, name = tempfile.mkstemp(dir=directory, prefix=".discogskit-probe-")
        os.close(fd)
        probe = Path(name)
        try:
            if final_dir is not None:
                target = final_dir / probe.name
                # The exact call commit() makes, so a cross-filesystem or permission failure shows up here too.
                os.replace(probe, target)
                probe = target
        finally:
            probe.unlink(missing_ok=True)
    except OSError as exc:
        # strerror, not the exception: its paths are the probe's, which mean nothing to the user.
        raise OSError(
            f"Can't write output files into {shown}: {exc.strerror or exc}"
        ) from exc


# Times to create the staging directory when another run's cleanup removes a directory it needs first.
_STAGING_ATTEMPTS = 10


def _make_staging_dir(output_dir: Path, entity_name: str, created: list[Path]) -> Path:
    """Create ``output_dir`` if needed, then a fresh staging directory in it, which it returns.

    Adds the directories it created for it (``output_dir`` and its missing parents) to ``created``, deepest first,
    even if it then fails. Only directories this call's own ``mkdir()`` created count: a path such as
    ``missing/../existing`` names a directory that existed before.
    """
    attempts = 0
    while True:
        attempts += 1
        try:
            for d in reversed((output_dir, *output_dir.parents)):
                if d.is_dir():
                    continue
                try:
                    d.mkdir()
                except FileExistsError:
                    if d.is_dir():  # Another run created it just now.
                        continue
                    raise
                if d not in created:
                    created.insert(0, d)
            # Visible (not dot-prefixed) so leftovers from a killed process, which can
            # be many GB, are easy to spot and delete.
            return Path(
                tempfile.mkdtemp(dir=output_dir, prefix=f"{entity_name}.partial-")
            )
        except FileNotFoundError:
            # Another run that failed removed a directory, empty then, after this one saw or created it: try again.
            if attempts == _STAGING_ATTEMPTS:
                raise


def _remove_if_empty(dirs: Iterable[Path]) -> None:
    """Remove each directory of ``dirs``, deepest first, until one isn't empty or can't be removed."""
    for d in dirs:
        try:
            d.rmdir()
        except FileNotFoundError:
            continue  # Already removed, by another run that failed.
        except OSError:
            return
