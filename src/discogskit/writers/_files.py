"""Base of the file writers: the output bookkeeping that Parquet and JSONL output share.

A subclass sets ``_output_exts`` and passes the extension of the files it writes to ``__init__``, implements
``_close_handles()``, ``setup()`` and ``write_chunk()``, and stages its files in ``setup()`` through ``_stage()``. Files
are staged and only moved to their final names by ``finalize()``.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar, Protocol

from discogskit._console import status
from discogskit.writers import OutputExistsError
from discogskit.writers._staging import StagedFiles, check_output_dir

if TYPE_CHECKING:
    import os

    from discogskit.entities import EntityDef


class _Closeable(Protocol):
    def close(self) -> None: ...


class _FileWriter(ABC):
    """Writer that produces one file per table, staged until ``finalize()``."""

    # Every extension a table file of this format has, whatever its codec: any file in an entity's directory ending in
    # one is existing output, and ``finalize()`` with overwrite deletes the entity's table files in the others.
    _output_exts: ClassVar[tuple[str, ...]]

    def __init__(
        self, output_dir: str | os.PathLike[str], ext: str, *, overwrite: bool
    ) -> None:
        self._output_dir = Path(output_dir)
        # The extension of the files this writer writes.
        self._ext = ext
        self._overwrite = overwrite
        self._staged: StagedFiles | None = None

    def cancel(self) -> None:
        """Do nothing: a local file write can't stall the way a database server can."""

    def check(self, entity: EntityDef) -> None:
        """Raise ``OutputExistsError`` if ``entity``'s directory holds files of this format and overwrite is off, or
        ``OSError`` if the output directory can't take the files.
        """
        # First, so an entity directory that can't be listed either fails with the probe's error.
        check_output_dir(self._output_dir, entity.name)
        entity_dir = self._output_dir / entity.name

        if not self._overwrite and entity_dir.exists():
            existing = [
                f.name
                for f in entity_dir.iterdir()
                if f.name.endswith(self._output_exts)
            ]
            if existing:
                raise OutputExistsError(
                    f"Output files already exist in {entity_dir} "
                    f"(e.g. {existing[0]}). Use --overwrite to replace them."
                )

    def close(self) -> None:
        """Release file handles; output not committed by ``finalize()`` is deleted, unless a failed commit kept it."""
        # The files are discarded below, so a failing flush or footer write doesn't matter and must not replace the
        # error that aborted the run.
        self._close_handles(ignore_errors=True)
        if self._staged is not None:
            self._staged.discard()
            self._staged = None

    def finalize(self, entity: EntityDef) -> None:
        assert self._staged is not None, "finalize() called before setup()"
        n_files = len(self._staged.filenames)
        self._close_handles(ignore_errors=False)
        total_bytes = self._staged.commit()
        entity_dir = self._staged.final_dir
        self._staged = None
        if self._overwrite:
            self._remove_other_exts(entity_dir, entity)
        total_mib = total_bytes / 2**20
        status("Output", f"{n_files} files, {total_mib:,.1f} MiB total")

    @staticmethod
    def _close(handle: _Closeable, *, ignore_errors: bool) -> None:
        """Close ``handle``, swallowing any error if ``ignore_errors``."""
        if ignore_errors:
            with suppress(Exception):
                handle.close()
        else:
            handle.close()

    @abstractmethod
    def _close_handles(self, *, ignore_errors: bool) -> None:
        """Close the per-table handles, in the order their formats need, and forget them. With ``ignore_errors``, close
        every one whatever fails; otherwise the first failure propagates."""

    def _remove_other_exts(self, entity_dir: Path, entity: EntityDef) -> None:
        """Delete ``entity``'s table files in ``entity_dir`` whose extension isn't ``_ext``.

        Only names made of a table name and one of ``_output_exts``: every other file stays.
        """
        for other in self._output_exts:
            if other == self._ext:
                continue
            for table_name in entity.table_order:
                path = entity_dir / f"{table_name}{other}"
                try:
                    path.unlink(missing_ok=True)
                except OSError as exc:
                    raise OSError(
                        f"The new output is in {entity_dir}, but removing {path.name}, earlier output in another "
                        f"compression, failed: {exc.strerror or exc}. Delete it before reading the directory."
                    ) from exc

    def _stage(self, entity: EntityDef) -> StagedFiles:
        """Check again that no output exists, then stage a file per table: output can appear after the caller's own
        ``check()``."""
        self.check(entity)
        self._staged = StagedFiles(
            self._output_dir,
            entity.name,
            [f"{table_name}{self._ext}" for table_name in entity.table_order],
        )
        return self._staged
