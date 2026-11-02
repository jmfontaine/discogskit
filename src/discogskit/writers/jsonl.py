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

The bytes are fixed so files are portable and reproducible: compact
separators, non-ASCII as UTF-8 except U+2028, U+2029 and U+0085, which
are escaped (``str.splitlines()`` and some editors and log tools split
lines on them), ``\\n`` line endings on every platform, and gzip headers
with no creation time or file name. Escaping changes no decoded value.
Running ``str.translate()`` on every line tripled json.dumps()' time on
200,000 labels rows, so write_chunk() runs it only on lines holding one
of the three, which three ``in`` checks find for about 5% more.

Existing output means any file in the entity's directory with a JSONL
extension of any codec, since a reader globbing ``*.jsonl*`` would read
old files of another codec along with the new ones. For the same reason,
``finalize()`` with overwrite deletes the entity's table files of the
other codecs, and nothing else.
"""

from __future__ import annotations

import bz2
import gzip
import io
import json
import time
from typing import IO, TYPE_CHECKING

from discogskit._console import status
from discogskit.writers._compression import check_compression, check_compression_level
from discogskit.writers._files import _FileWriter

if TYPE_CHECKING:
    import os

    import pyarrow as pa

    from discogskit.entities import EntityDef


_COMPRESSED_EXT = {"bzip2": ".jsonl.bz2", "gzip": ".jsonl.gz"}

# Line breaks to str.splitlines() that JSON allows raw in strings, mapped to their JSON escapes.
_LINE_BREAK_ESCAPES = str.maketrans(
    {"\u2028": "\\u2028", "\u2029": "\\u2029", "\x85": "\\u0085"}
)

# Every extension a JSONL table file has, whatever its codec.
_EXTENSIONS = (".jsonl", *_COMPRESSED_EXT.values())

# (min, max) compression level Python's own bz2/gzip modules accept.
_LEVEL_RANGES = {"bzip2": (1, 9), "gzip": (0, 9)}

# For a level without a codec: "none does not support a compression level" would name a codec the caller never passed.
_DEFAULT_CODEC_LEVEL_ERROR = "JSONL output is uncompressed by default; --compression-level needs --compression gzip or bzip2."


class JSONLWriter(_FileWriter):
    """Writer implementation that produces one .jsonl file per table."""

    _output_exts = _EXTENSIONS

    def __init__(
        self,
        output_dir: str | os.PathLike[str],
        *,
        compression: str | None = None,
        compression_level: int | None = None,
        overwrite: bool = False,
    ) -> None:
        """``compression`` None means uncompressed. Raises ``ValueError`` for a codec or level JSONL output can't use."""
        codec = "none" if compression is None else compression
        check_compression("jsonl", codec, [*_COMPRESSED_EXT, "none"])
        check_compression_level(
            codec,
            compression_level,
            _LEVEL_RANGES.get(codec),
            no_level_error=_DEFAULT_CODEC_LEVEL_ERROR if compression is None else None,
        )
        super().__init__(
            output_dir, _COMPRESSED_EXT.get(codec, ".jsonl"), overwrite=overwrite
        )
        self._compression = codec
        self._compression_level = compression_level
        self._files: dict[str, IO] = {}
        # The files under the gzip streams in _files, which don't close them.
        self._raw_files: list[IO[bytes]] = []

    def setup(self, entity: EntityDef) -> None:
        staged = self._stage(entity)

        t0 = time.perf_counter()
        for table_name in entity.table_order:
            path = staged.path(f"{table_name}{self._ext}")
            # File lifetime is managed by finalize()/close(), not a with-block. newline="\n": no "\r\n" on Windows.
            if self._compression == "gzip":
                level = (
                    6 if self._compression_level is None else self._compression_level
                )
                raw = open(path, "wb")  # noqa: SIM115
                self._raw_files.append(raw)
                # An empty file name and mtime 0 keep the header free of both, so identical input gives identical
                # bytes; gzip.open() records them.
                self._files[table_name] = io.TextIOWrapper(
                    gzip.GzipFile(
                        compresslevel=level,
                        filename="",
                        fileobj=raw,
                        mode="wb",
                        mtime=0,
                    ),
                    encoding="utf-8",
                    newline="\n",
                )
            elif self._compression == "bzip2":
                if self._compression_level is None:
                    self._files[table_name] = bz2.open(  # noqa: SIM115
                        path, "wt", encoding="utf-8", newline="\n"
                    )
                else:
                    self._files[table_name] = bz2.open(  # noqa: SIM115
                        path,
                        "wt",
                        compresslevel=self._compression_level,
                        encoding="utf-8",
                        newline="\n",
                    )
            else:
                self._files[table_name] = open(  # noqa: SIM115
                    path, "w", encoding="utf-8", newline="\n"
                )
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
                    line = json.dumps(row, ensure_ascii=False, separators=(",", ":"))
                    if "\u2028" in line or "\u2029" in line or "\x85" in line:
                        line = line.translate(_LINE_BREAK_ESCAPES)
                    f.write(line)
                    f.write("\n")
            timings[table_name] = time.perf_counter() - t0
        return timings

    def _close_handles(self, *, ignore_errors: bool) -> None:
        for f in self._files.values():
            self._close(f, ignore_errors=ignore_errors)
        self._files.clear()
        # After the gzip streams above, whose close() writes their trailers here but leaves these open.
        for raw in self._raw_files:
            self._close(raw, ignore_errors=ignore_errors)
        self._raw_files.clear()
