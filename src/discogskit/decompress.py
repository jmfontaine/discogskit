"""Gzip decompression via rapidgzip.

Decompresses to disk (not streamed) because mmap-based chunk splitting in
``entities/_split.py`` requires random access to the full XML file.  rapidgzip
parallelizes decompression across cores by exploiting the block structure of
the deflate format, which is significantly faster than single-threaded
gzip/igzip for large files (~58 GB uncompressed for the full releases dump).

An existing ``.xml`` is reused as a cache, so it must only ever hold complete
output: decompression writes ``<name>.xml.partial`` and renames it into place
once every byte is on disk.

Runs on the same dump (say a ``convert`` and a ``load``) coordinate through a
``filelock.ReadWriteLock`` on ``<name>.xml.lock``:

- Write lock to decompress or delete the XML.  Only the writer touches
  ``.partial``, so one found while holding the lock is a crashed run's leftover.
- Read lock while a run reads the XML.  A finishing run deletes it only if it
  gets the write lock without waiting; otherwise the last reader decides.

The lock file stays next to the dump: deleting it would let a run still
waiting on the old file and a new run on a fresh one both hold "the" lock.
"""

from __future__ import annotations

import os
import time
from collections.abc import Callable
from pathlib import Path

import rapidgzip
from filelock import ReadWriteLock, Timeout

from discogskit._console import status

# A blocking acquire waits inside SQLite, where Ctrl+C isn't handled until it
# returns, so wait in slices this long.
_LOCK_WAIT_SLICE = 0.5


class DecompressError(Exception):
    """Raised when decompression fails (corrupt or unrecognized file)."""

    def __init__(self, path: Path) -> None:
        super().__init__(
            f"failed to decompress {path.name} (corrupt or not a gzip file)"
        )


class XmlLease:
    """A run's read lock on a complete decompressed XML, from ``ensure_xml``."""

    def __init__(self, lock: ReadWriteLock, xml_path: Path) -> None:
        self._lock: ReadWriteLock | None = lock
        self._xml_path = xml_path

    def release(self, *, remove_xml: bool) -> None:
        """Stop using the XML.  Calls after the first do nothing.

        With ``remove_xml``, deletes the XML unless another run still reads it,
        in which case that run decides.
        """
        lock = self._lock
        if lock is None:
            return
        self._lock = None
        try:
            # Read locks can't be upgraded, so drop ours first.  Of two runs
            # finishing together, the second to drop its read lock gets in.
            lock.release()
            if not remove_xml:
                return
            try:
                lock.acquire_write(blocking=False)
            except Timeout:
                status("Cleanup", f"kept {self._xml_path.name}, in use by another run")
                return
            self._xml_path.unlink(missing_ok=True)
        finally:
            lock.close()


def _acquire(acquire: Callable[..., object], xml_name: str) -> None:
    """Call a ``ReadWriteLock`` acquire method, waiting for other runs if needed."""
    try:
        acquire(blocking=False)
        return
    except Timeout:
        status("Decompress", f"waiting for another run using {xml_name}")
    while True:
        try:
            acquire(timeout=_LOCK_WAIT_SLICE)
            return
        except Timeout:
            continue


def ensure_xml(gz_path: Path, xml_path: Path, workers: int) -> XmlLease:
    """Decompress .gz to .xml with rapidgzip (parallel) unless the .xml exists.

    Returns once the .xml is complete, holding a read lock until ``release()``.
    """
    lock = ReadWriteLock(
        xml_path.with_name(xml_path.name + ".lock"), is_singleton=False
    )
    partial_path = xml_path.with_name(xml_path.name + ".partial")
    decompressed = False
    try:
        while True:
            _acquire(lock.acquire_read, xml_path.name)
            # Nobody holds the write lock, so nobody is writing this.
            partial_path.unlink(missing_ok=True)
            if xml_path.exists():
                if not decompressed:
                    status("Decompress", f"cached {xml_path.name}")
                return XmlLease(lock, xml_path)
            lock.release()

            _acquire(lock.acquire_write, xml_path.name)
            try:
                # Another run may have decompressed it while we waited.
                if not xml_path.exists():
                    _decompress(gz_path, partial_path, xml_path, workers)
                    decompressed = True
            finally:
                # Loop to take the read lock and check again: another run can
                # delete the XML between this release and that acquire.
                lock.release()
    except BaseException:
        lock.close()
        raise


def _decompress(
    gz_path: Path, partial_path: Path, xml_path: Path, workers: int
) -> None:
    """Decompress into ``partial_path``, then rename it to ``xml_path``."""
    # Left behind by a run that died mid-decompression; never complete.
    partial_path.unlink(missing_ok=True)
    t0 = time.perf_counter()
    try:
        with (
            rapidgzip.open(str(gz_path), parallelization=workers) as fin,
            open(partial_path, "wb") as fout,
        ):
            while True:
                chunk = fin.read(4 * 1024 * 1024)
                if not chunk:
                    break
                fout.write(chunk)
            fout.flush()
            # Without this, a power loss can persist the rename below before
            # the data, leaving a complete-looking but corrupt .xml.
            os.fsync(fout.fileno())
    except KeyboardInterrupt:
        partial_path.unlink(missing_ok=True)
        raise
    except (OSError, ValueError):
        partial_path.unlink(missing_ok=True)
        raise DecompressError(gz_path) from None
    try:
        os.replace(partial_path, xml_path)
    except OSError:
        # Not a decompression failure: surface the real filesystem error.
        partial_path.unlink(missing_ok=True)
        raise
    elapsed = time.perf_counter() - t0
    size_mb = xml_path.stat().st_size / 1024 / 1024
    status("Decompress", f"{size_mb:,.1f} MB", f"[{elapsed:.2f}s]")
