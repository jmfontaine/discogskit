"""JSONL writer: Arrow RecordBatches serialized as one JSON object per line.

Arrow list columns become JSON arrays via ``to_pylist()``, so no manual
conversion is needed. Gzip compression defaults to level 6 rather than
the library's level-9 default: output a few percent larger for much
less compression CPU (measured in #32). bz2 stays at its own default —
its ``compresslevel`` only changes the BWT block size, not compression
speed, and level 6 produced no measured speedup and slightly larger
output. ``compression_level`` overrides either default, within the
range ``_LEVEL_RANGES`` gives per codec.
Batching rows into fewer, larger write() calls was tried and measured:
the speedup didn't reproduce beyond noise, since json.dumps() dominates,
so write_chunk() still makes two write() calls per row (the JSON text,
then the newline). Files are staged and only moved to their final names
by ``finalize()``.
"""

from __future__ import annotations

import bz2
import gzip
import json
import time
from contextlib import suppress
from pathlib import Path
from typing import IO, TYPE_CHECKING

from discogskit._console import status
from discogskit.writers._compression import check_compression, check_compression_level
from discogskit.writers._staging import StagedFiles, check_output_dir

if TYPE_CHECKING:
    import os

    import pyarrow as pa

    from discogskit.entities import EntityDef


_COMPRESSED_EXT = {"bzip2": ".jsonl.bz2", "gzip": ".jsonl.gz"}

# (min, max) compression level Python's own bz2/gzip modules accept.
_LEVEL_RANGES = {"bzip2": (1, 9), "gzip": (0, 9)}


class JSONLWriter:
    """Writer implementation that produces one .jsonl file per table."""

    def __init__(
        self,
        output_dir: str | os.PathLike[str],
        *,
        compression: str | None = None,
        compression_level: int | None = None,
        overwrite: bool = False,
    ) -> None:
        """``compression`` None means uncompressed. Raises ``ValueError`` for a codec or level JSONL output can't use."""
        compression = "none" if compression is None else compression
        check_compression("jsonl", compression, [*_COMPRESSED_EXT, "none"])
        check_compression_level(
            compression, compression_level, _LEVEL_RANGES.get(compression)
        )
        self._output_dir = Path(output_dir)
        self._compression = compression
        self._compression_level = compression_level
        self._overwrite = overwrite
        self._files: dict[str, IO] = {}
        self._staged: StagedFiles | None = None

    def cancel(self) -> None:
        """Do nothing: a local file write can't stall the way a database server can."""

    def check(self, entity: EntityDef) -> None:
        """Raise ``OutputExistsError`` if ``entity``'s directory holds JSONL files and overwrite is off, or
        ``OSError`` if the output directory can't take the files.
        """
        # First, so an entity directory that can't be listed either fails with the probe's error.
        check_output_dir(self._output_dir, entity.name)
        entity_dir = self._output_dir / entity.name
        ext = _COMPRESSED_EXT.get(self._compression, ".jsonl")

        if not self._overwrite and entity_dir.exists():
            existing = [f.name for f in entity_dir.iterdir() if f.name.endswith(ext)]
            if existing:
                from discogskit.writers import OutputExistsError

                raise OutputExistsError(
                    f"Output files already exist in {entity_dir} "
                    f"(e.g. {existing[0]}). Use --overwrite to replace them."
                )

    def setup(self, entity: EntityDef) -> None:
        # Checked again here: output can appear after the caller's own check().
        self.check(entity)
        ext = _COMPRESSED_EXT.get(self._compression, ".jsonl")

        self._staged = StagedFiles(
            self._output_dir,
            entity.name,
            [f"{table_name}{ext}" for table_name in entity.table_order],
        )

        t0 = time.perf_counter()
        for table_name in entity.table_order:
            path = self._staged.path(f"{table_name}{ext}")
            # File lifetime is managed by finalize()/close(), not a with-block.
            if self._compression == "gzip":
                level = (
                    6 if self._compression_level is None else self._compression_level
                )
                self._files[table_name] = gzip.open(  # noqa: SIM115
                    path, "wt", compresslevel=level, encoding="utf-8"
                )
            elif self._compression == "bzip2":
                if self._compression_level is None:
                    self._files[table_name] = bz2.open(  # noqa: SIM115
                        path, "wt", encoding="utf-8"
                    )
                else:
                    self._files[table_name] = bz2.open(  # noqa: SIM115
                        path,
                        "wt",
                        compresslevel=self._compression_level,
                        encoding="utf-8",
                    )
            else:
                self._files[table_name] = open(path, "w", encoding="utf-8")  # noqa: SIM115
        codec = f", {self._compression}" if self._compression != "none" else ""
        status(
            "Create",
            f"{len(self._files)} jsonl files{codec}",
            f"[{time.perf_counter() - t0:.2f}s]",
        )

    def write_chunk(self, tables: dict[str, pa.RecordBatch]) -> dict[str, float]:
        timings: dict[str, float] = {}
        for table_name, batch in tables.items():
            t0 = time.perf_counter()
            if batch.num_rows:
                f = self._files[table_name]
                for row in batch.to_pylist():
                    f.write(json.dumps(row, ensure_ascii=False))
                    f.write("\n")
            timings[table_name] = time.perf_counter() - t0
        return timings

    def finalize(self, entity: EntityDef) -> None:
        assert self._staged is not None, "finalize() called before setup()"
        n_files = len(self._files)
        for f in self._files.values():
            f.close()
        self._files.clear()
        total_bytes = self._staged.commit()
        self._staged = None
        total_mib = total_bytes / 2**20
        status("Output", f"{n_files} files, {total_mib:,.1f} MiB total")

    def close(self) -> None:
        """Release file handles; output not committed by ``finalize()`` is deleted, unless a failed commit kept it."""
        for f in self._files.values():
            # The file is discarded below, so a failing flush doesn't matter and
            # must not replace the error that aborted the run.
            with suppress(Exception):
                f.close()
        self._files.clear()
        if self._staged is not None:
            self._staged.discard()
            self._staged = None
