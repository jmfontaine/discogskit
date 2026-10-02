"""JSONL writer: Arrow RecordBatches serialized as one JSON object per line.

Arrow list columns become JSON arrays via ``to_pylist()``, so no manual
conversion is needed. Optional gzip compression produces ``.jsonl.gz`` files.
Files are staged and only moved to their final names by ``finalize()``.
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
from discogskit.writers._ipc import deserialize_batches
from discogskit.writers._staging import StagedFiles

if TYPE_CHECKING:
    from discogskit.entities import EntityDef


_COMPRESSED_EXT = {"bzip2": ".jsonl.bz2", "gzip": ".jsonl.gz"}


class JSONLWriter:
    """Writer implementation that produces one .jsonl file per table."""

    def __init__(
        self, output_dir: str, *, compression: str = "none", overwrite: bool = False
    ) -> None:
        self._output_dir = Path(output_dir)
        self._compression = compression
        self._overwrite = overwrite
        self._files: dict[str, IO] = {}
        self._staged: StagedFiles | None = None

    def setup(self, entity: EntityDef) -> None:
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
                self._files[table_name] = gzip.open(path, "wt", encoding="utf-8")  # noqa: SIM115
            elif self._compression == "bzip2":
                self._files[table_name] = bz2.open(path, "wt", encoding="utf-8")  # noqa: SIM115
            else:
                self._files[table_name] = open(path, "w", encoding="utf-8")  # noqa: SIM115
        codec = f", {self._compression}" if self._compression != "none" else ""
        status(
            "Create",
            f"{len(self._files)} jsonl files{codec}",
            f"[{time.perf_counter() - t0:.2f}s]",
        )

    def write_chunk(
        self,
        ipc_dict: dict[str, bytes],
        entity: EntityDef,
        table_timings: dict[str, float] | None = None,
    ) -> int:
        count = 0
        root_table = entity.table_order[0]

        for table_name in entity.table_order:
            t0 = time.perf_counter() if table_timings is not None else 0
            batches = deserialize_batches(ipc_dict[table_name])
            if not batches or batches[0].num_rows == 0:
                if table_timings is not None:
                    table_timings[table_name] = table_timings.get(table_name, 0.0) + (
                        time.perf_counter() - t0
                    )
                continue
            if table_name == root_table:
                count = sum(b.num_rows for b in batches)
            f = self._files[table_name]
            for batch in batches:
                for row in batch.to_pylist():
                    f.write(json.dumps(row, ensure_ascii=False))
                    f.write("\n")
            if table_timings is not None:
                table_timings[table_name] = table_timings.get(table_name, 0.0) + (
                    time.perf_counter() - t0
                )
        return count

    def finalize(self, entity: EntityDef) -> None:
        assert self._staged is not None, "finalize() called before setup()"
        n_files = len(self._files)
        for f in self._files.values():
            f.close()
        self._files.clear()
        total_bytes = self._staged.commit()
        self._staged = None
        total_mb = total_bytes / (1024 * 1024)
        status("Output", f"{n_files} files, {total_mb:,.1f} MB total")

    def close(self) -> None:
        """Release file handles; output not committed by ``finalize()`` is deleted."""
        for f in self._files.values():
            # The file is discarded below, so a failing flush doesn't matter and
            # must not replace the error that aborted the run.
            with suppress(Exception):
                f.close()
        self._files.clear()
        if self._staged is not None:
            self._staged.discard()
            self._staged = None
