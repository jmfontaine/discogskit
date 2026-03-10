"""Gzip decompression via rapidgzip.

Decompresses to disk (not streamed) because mmap-based chunk splitting in
``entities/_split.py`` requires random access to the full XML file.  rapidgzip
parallelizes decompression across cores by exploiting the block structure of
the deflate format, which is significantly faster than single-threaded
gzip/igzip for large files (~58 GB uncompressed for the full releases dump).
"""

from __future__ import annotations

import time
from pathlib import Path

import rapidgzip

from discogskit._console import status


class DecompressError(Exception):
    """Raised when decompression fails (corrupt or unrecognized file)."""

    def __init__(self, path: Path) -> None:
        super().__init__(
            f"failed to decompress {path.name} (corrupt or not a gzip file)"
        )


def ensure_xml(gz_path: Path, xml_path: Path, workers: int) -> None:
    """Decompress .gz to .xml using rapidgzip (parallel). Skips if xml exists."""
    if xml_path.exists():
        status("Decompress", f"cached {xml_path.name}")
        return
    t0 = time.perf_counter()
    try:
        with rapidgzip.open(str(gz_path), parallelization=workers) as fin:
            with open(xml_path, "wb") as fout:
                while True:
                    chunk = fin.read(4 * 1024 * 1024)
                    if not chunk:
                        break
                    fout.write(chunk)
    except KeyboardInterrupt:  # pragma: no cover
        # Remove partial XML so it won't be treated as cached on next run
        xml_path.unlink(missing_ok=True)
        raise
    except (OSError, ValueError):
        xml_path.unlink(missing_ok=True)
        raise DecompressError(gz_path) from None
    elapsed = time.perf_counter() - t0
    size_mb = xml_path.stat().st_size / 1024 / 1024
    status("Decompress", f"{size_mb:,.1f} MB", f"[{elapsed:.2f}s]")
