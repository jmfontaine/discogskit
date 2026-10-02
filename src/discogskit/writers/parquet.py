"""Parquet writer: Arrow RecordBatches written directly to Parquet files.

Arrow list columns (``pa.list_(pa.utf8())``) are natively supported by Parquet, so no type conversion is needed.
Each chunk becomes a row group. Files are staged and only moved to their final names by ``finalize()``.
"""

from __future__ import annotations

import time
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow.parquet as pq

from discogskit._console import status
from discogskit.writers._ipc import deserialize_batches
from discogskit.writers._staging import StagedFiles

if TYPE_CHECKING:
    import pyarrow as pa

    from discogskit.entities import EntityDef


class ParquetWriter:
    """Writer implementation that produces one .parquet file per table."""

    def __init__(
        self, output_dir: str, *, compression: str = "zstd", overwrite: bool = False
    ) -> None:
        self._output_dir = Path(output_dir)
        self._compression = compression
        self._overwrite = overwrite
        self._staged: StagedFiles | None = None
        self._writers: dict[str, pq.ParquetWriter] = {}

    def setup(self, entity: EntityDef) -> None:
        entity_dir = self._output_dir / entity.name

        if not self._overwrite and entity_dir.exists():
            existing = [f.name for f in entity_dir.iterdir() if f.suffix == ".parquet"]
            if existing:
                from discogskit.writers import OutputExistsError

                raise OutputExistsError(
                    f"Output files already exist in {entity_dir} "
                    f"(e.g. {existing[0]}). Use --overwrite to replace them."
                )

        self._staged = StagedFiles(
            self._output_dir,
            entity.name,
            [f"{table_name}.parquet" for table_name in entity.table_order],
        )

        t0 = time.perf_counter()
        for table_name in entity.table_order:
            path = self._staged.path(f"{table_name}.parquet")
            schema: pa.Schema = entity.schemas[table_name]
            self._writers[table_name] = pq.ParquetWriter(
                str(path), schema, compression=self._compression
            )
        status(
            "Create",
            f"{len(self._writers)} parquet files, {self._compression}",
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
            writer = self._writers[table_name]
            for batch in batches:
                writer.write_batch(batch)
            if table_timings is not None:
                table_timings[table_name] = table_timings.get(table_name, 0.0) + (
                    time.perf_counter() - t0
                )
        return count

    def finalize(self, entity: EntityDef) -> None:
        assert self._staged is not None, "finalize() called before setup()"
        n_files = len(self._writers)
        for writer in self._writers.values():
            writer.close()
        self._writers.clear()
        total_bytes = self._staged.commit()
        self._staged = None
        total_mb = total_bytes / (1024 * 1024)
        status("Output", f"{n_files} files, {total_mb:,.1f} MB total")

    def close(self) -> None:
        """Release file handles; output not committed by ``finalize()`` is deleted."""
        for writer in self._writers.values():
            # The file is discarded below, so a failing footer write doesn't matter
            # and must not replace the error that aborted the run.
            with suppress(Exception):
                writer.close()
        self._writers.clear()
        if self._staged is not None:
            self._staged.discard()
            self._staged = None
