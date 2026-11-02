"""Parquet writer: Arrow RecordBatches written directly to Parquet files.

Arrow list columns (``pa.list_(pa.utf8())``) are natively supported by Parquet, so no type conversion is needed.
Each chunk becomes a row group. Files are staged and only moved to their final names by ``finalize()``.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq

from discogskit._console import status
from discogskit.writers._compression import check_compression, check_compression_level
from discogskit.writers._files import _FileWriter

if TYPE_CHECKING:
    import os

    from discogskit.entities import EntityDef


_COMPRESSIONS = ("brotli", "gzip", "none", "snappy", "zstd")


def _level_range(compression: str) -> tuple[int, int] | None:
    """(min, max) compression level pyarrow's Codec accepts for this codec, or None if it has none.

    Asks pyarrow directly rather than hard-coding ranges, since they're codec- (and pyarrow-version-)
    specific: pyarrow's gzip range differs from Python's gzip module, and zstd's is nothing like 0-9.
    """
    if compression == "none" or not pa.Codec.supports_compression_level(compression):
        return None
    return (
        pa.Codec.minimum_compression_level(compression),
        pa.Codec.maximum_compression_level(compression),
    )


class ParquetWriter(_FileWriter):
    """Writer implementation that produces one .parquet file per table."""

    _output_exts = (".parquet",)

    def __init__(
        self,
        output_dir: str | os.PathLike[str],
        *,
        compression: str | None = None,
        compression_level: int | None = None,
        overwrite: bool = False,
    ) -> None:
        """``compression`` None means zstd. Raises ``ValueError`` for a codec or level Parquet output can't use."""
        compression = "zstd" if compression is None else compression
        check_compression("parquet", compression, _COMPRESSIONS)
        check_compression_level(
            compression, compression_level, _level_range(compression)
        )
        super().__init__(output_dir, ".parquet", overwrite=overwrite)
        self._compression = compression
        self._compression_level = compression_level
        self._writers: dict[str, pq.ParquetWriter] = {}

    def setup(self, entity: EntityDef) -> None:
        staged = self._stage(entity)

        t0 = time.perf_counter()
        for table_name in entity.table_order:
            path = staged.path(f"{table_name}{self._ext}")
            schema: pa.Schema = entity.schemas[table_name]
            self._writers[table_name] = pq.ParquetWriter(
                str(path),
                schema,
                compression=self._compression,
                compression_level=self._compression_level,
            )
        status(
            "Create",
            f"{len(self._writers)} parquet files, {self._compression}",
            f"[{time.perf_counter() - t0:.2f}s]",
        )

    def write_chunk(self, tables: dict[str, pa.RecordBatch]) -> dict[str, float]:
        timings: dict[str, float] = {}
        for table_name, batch in tables.items():
            t0 = time.perf_counter()
            if batch.num_rows:
                self._writers[table_name].write_batch(batch)
            timings[table_name] = time.perf_counter() - t0
        return timings

    def _close_handles(self, *, ignore_errors: bool) -> None:
        for writer in self._writers.values():
            self._close(writer, ignore_errors=ignore_errors)
        self._writers.clear()
