"""Gzip decompression via rapidgzip.

Decompresses to disk (not streamed) because mmap-based chunk splitting in
``entities/_split.py`` requires random access to the full XML file.  rapidgzip
parallelizes decompression across cores by exploiting the block structure of
the deflate format, which is significantly faster than single-threaded
gzip/igzip for large files (~58 GB uncompressed for the full releases dump).

An existing ``.xml`` is reused as a cache, so it must only ever hold complete
output: decompression writes ``<name>.xml.partial`` and renames it into place
once every byte is on disk.

Only an XML discogskit wrote is reused or deleted.  Before that rename,
decompression writes a ``<name>.xml.discogskit`` marker recording the source
``.gz`` (resolved path, size, mtime).  An XML whose marker matches the ``.gz``
is reused; one whose marker doesn't (an older dump of the same name) or that
is empty is decompressed again; one with no marker is someone else's file, so
the run fails without touching it.  Size and mtime are a freshness check, not
proof the content matches: a ``.gz`` replaced by one of the same size and
mtime isn't noticed.  The marker goes in before the XML and out after it, so
a crash in between leaves a marker without an XML (decompressed again), never
an XML without its marker.

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

import bisect
import enum
import json
import os
import struct
import time
import zlib
from collections.abc import Callable
from pathlib import Path

import rapidgzip
from filelock import ReadWriteLock, Timeout

from discogskit._console import status

# A blocking acquire waits inside SQLite, where Ctrl+C isn't handled until it
# returns, so wait in slices this long.
_LOCK_WAIT_SLICE = 0.5


class DecompressError(Exception):
    """Raised when the .gz can't be decompressed (truncated, corrupt, not gzip)."""

    def __init__(self, path: Path) -> None:
        super().__init__(
            f"failed to decompress {path.name} (truncated, corrupt, or not a gzip file)"
        )


class ForeignXmlError(Exception):
    """Raised when the dump's ``.xml`` exists but discogskit didn't write it."""

    def __init__(self, xml_path: Path) -> None:
        super().__init__(
            f"{xml_path.name} exists but wasn't written by discogskit; "
            "move or delete it"
        )


class XmlLease:
    """A run's read lock on a complete decompressed XML, from ``ensure_xml``."""

    def __init__(self, lock: ReadWriteLock, marker_path: Path, xml_path: Path) -> None:
        self._lock: ReadWriteLock | None = lock
        self._marker_path = marker_path
        self._xml_path = xml_path

    def release(self, *, remove_xml: bool) -> None:
        """Stop using the XML.  Calls after the first do nothing.

        With ``remove_xml``, deletes the XML unless another run still reads it,
        in which case that run decides, or discogskit didn't write it.
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
            if not self._marker_path.exists():
                status(
                    "Cleanup",
                    f"kept {self._xml_path.name}, not written by discogskit",
                )
                return
            # XML first, its removal on disk before the marker's: a crash in
            # between must not leave the XML without its marker.
            self._xml_path.unlink(missing_ok=True)
            _sync_dir(self._xml_path.parent)
            self._marker_path.unlink(missing_ok=True)
        finally:
            lock.close()


class _Cache(enum.Enum):
    """What ``ensure_xml`` finds at the XML's path."""

    ABSENT = enum.auto()
    EMPTY = enum.auto()  # discogskit's, for this .gz, but 0 bytes
    FOREIGN = enum.auto()  # no marker: not discogskit's
    FRESH = enum.auto()  # discogskit's, for this .gz
    STALE = enum.auto()  # discogskit's, for another .gz (or an unreadable marker)


def _source_identity(gz_path: Path) -> bytes:
    """The marker content for ``gz_path``: its resolved path, mtime and size."""
    stat = gz_path.stat()
    identity = {
        "gz_path": str(gz_path.resolve()),
        "mtime_ns": stat.st_mtime_ns,
        "size": stat.st_size,
    }
    return json.dumps(identity, sort_keys=True).encode()


def _sync_dir(directory: Path) -> None:
    """Put the directory's entries on disk.

    ``fsync`` on a file covers only its data; this makes earlier creates,
    renames and deletes in ``directory`` survive a power loss before later ones.
    """
    if os.name == "nt":  # Windows can't open a directory to fsync it.
        return
    fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _cache_state(gz_path: Path, marker_path: Path, xml_path: Path) -> _Cache:
    """Classify the XML at ``xml_path``; call with the read or write lock held."""
    try:
        xml_size = xml_path.stat().st_size
    except FileNotFoundError:
        return _Cache.ABSENT
    try:
        recorded = marker_path.read_bytes()
    except FileNotFoundError:
        return _Cache.FOREIGN
    if recorded != _source_identity(gz_path):
        return _Cache.STALE
    return _Cache.FRESH if xml_size else _Cache.EMPTY


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
    """Decompress .gz to .xml with rapidgzip (parallel) unless discogskit's own
    .xml for this .gz is already there.

    Returns once the .xml is complete, holding a read lock until ``release()``.
    Raises ``ForeignXmlError``, without touching the file, if the .xml exists
    but discogskit didn't write it.
    """
    lock = ReadWriteLock(
        xml_path.with_name(xml_path.name + ".lock"), is_singleton=False
    )
    marker_path = xml_path.with_name(xml_path.name + ".discogskit")
    partial_path = xml_path.with_name(xml_path.name + ".partial")
    decompressed = False

    def usable(state: _Cache) -> bool:
        if state is _Cache.FOREIGN:
            raise ForeignXmlError(xml_path)
        # A .gz holding an empty XML decompresses to an empty XML every time:
        # once is enough, and the splitter reports it.
        return state is _Cache.FRESH or (decompressed and state is _Cache.EMPTY)

    try:
        while True:
            _acquire(lock.acquire_read, xml_path.name)
            # Nobody holds the write lock, so nobody is writing this.
            partial_path.unlink(missing_ok=True)
            if usable(_cache_state(gz_path, marker_path, xml_path)):
                if not decompressed:
                    status("Decompress", f"cached {xml_path.name}")
                return XmlLease(lock, marker_path, xml_path)
            lock.release()

            _acquire(lock.acquire_write, xml_path.name)
            try:
                # Another run may have decompressed it while we waited.
                if not usable(_cache_state(gz_path, marker_path, xml_path)):
                    _decompress(gz_path, marker_path, partial_path, xml_path, workers)
                    decompressed = True
            finally:
                # Loop to take the read lock and check again: another run can
                # delete the XML between this release and that acquire.
                lock.release()
    except BaseException:
        lock.close()
        raise


def _decompress(
    gz_path: Path, marker_path: Path, partial_path: Path, xml_path: Path, workers: int
) -> None:
    """Decompress into ``partial_path``, then rename it to ``xml_path``.

    Any XML already there is discogskit's (stale or empty), so it's replaced.
    """
    # Remove a stale XML before its marker, and both before writing anything:
    # the new marker must never vouch for the old XML, even after a power loss.
    # Synced even if the XML is already gone: a run that crashed right after
    # unlinking it may have left that removal off the disk.
    xml_path.unlink(missing_ok=True)
    _sync_dir(xml_path.parent)
    marker_path.unlink(missing_ok=True)
    # Left behind by a run that died mid-decompression; never complete.
    partial_path.unlink(missing_ok=True)
    # Taken before reading, so a .gz that changes meanwhile counts as stale.
    source = _source_identity(gz_path)
    t0 = time.perf_counter()
    try:
        gz_bits = gz_path.stat().st_size * 8
        with (
            rapidgzip.open(str(gz_path), parallelization=workers) as fin,
            open(partial_path, "wb") as fout,
        ):
            checkpoints = [(0, 0)]
            while True:
                chunk = fin.read(4 * 1024 * 1024)
                if not chunk:
                    break
                fout.write(chunk)
                offset, crc = checkpoints[-1]
                checkpoints.append((offset + len(chunk), zlib.crc32(chunk, crc)))
            # rapidgzip's ISA-L decoder, built into its Linux wheels but not its
            # macOS ones, takes a deflate stream that runs out of input for its
            # end: reads stop early with no error.  A complete .gz is decoded to
            # its last byte (trailing bytes are already an error), so stopping
            # anywhere else is truncation.
            consumed_bits = fin.tell_compressed()
            if consumed_bits != gz_bits:
                raise ValueError(
                    f"decompression ended at byte {consumed_bits // 8:,}"
                    f" of {gz_bits // 8:,}"
                )
            fout.flush()
            # It can also stop at the end of the file right after a later gzip
            # member's header, without decoding that member.  A complete .gz
            # ends with its last member's footer, which has to match the end of
            # the output.
            _check_last_member(gz_path, partial_path, checkpoints)
            # Without this, a power loss can persist the rename below before
            # the data, leaving a complete-looking but corrupt .xml.
            os.fsync(fout.fileno())
    except (KeyboardInterrupt, OSError):
        # OSError is the filesystem (disk full, permissions), not the gzip:
        # surface it unchanged so the errno and path reach the user.
        partial_path.unlink(missing_ok=True)
        raise
    except (RuntimeError, ValueError) as exc:
        # rapidgzip raises ValueError for corrupt or non-gzip input and a bare
        # RuntimeError('std::exception') for truncated input; the check above
        # raises ValueError for truncation rapidgzip doesn't report.
        partial_path.unlink(missing_ok=True)
        raise DecompressError(gz_path) from exc
    try:
        with open(marker_path, "wb") as marker:
            marker.write(source)
            marker.flush()
            os.fsync(marker.fileno())
        # The marker must be on disk before the rename, which would otherwise
        # leave an XML without its marker after a power loss.
        _sync_dir(marker_path.parent)
        os.replace(partial_path, xml_path)
    except OSError:
        # Not a decompression failure: surface the real filesystem error.
        # The XML wasn't published, so neither file may stay.  (Not on
        # Ctrl+C: the rename may have finished, and its XML needs the marker.)
        partial_path.unlink(missing_ok=True)
        marker_path.unlink(missing_ok=True)
        raise
    elapsed = time.perf_counter() - t0
    size_mb = xml_path.stat().st_size / 1024 / 1024
    status("Decompress", f"{size_mb:,.1f} MB", f"[{elapsed:.2f}s]")


# The CRC-32 polynomial, bit-reflected as gzip and zlib use it.
_CRC32_POLY = 0xEDB88320


def _crc32_multiply(a: int, b: int) -> int:
    """Multiply two polynomials modulo the CRC-32 polynomial (zlib's multmodp).

    ``a`` must not be 0.
    """
    m = 1 << 31
    p = 0
    while True:
        if a & m:
            p ^= b
            if a & (m - 1) == 0:
                return p
        m >>= 1
        b = (b >> 1) ^ _CRC32_POLY if b & 1 else b >> 1


# x^(2^k) modulo the CRC-32 polynomial for k = 0..31 (zlib's x2n_table).
_CRC32_X2N = [1 << 30]
for _ in range(31):
    _CRC32_X2N.append(_crc32_multiply(_CRC32_X2N[-1], _CRC32_X2N[-1]))


def _crc32_shift(crc: int, length: int) -> int:
    """``crc`` times x^(8 * length): what ``crc`` contributes to the CRC-32 of
    its data followed by ``length`` more bytes (zlib's crc32_combine).
    """
    x2n = 1 << 31  # x^0
    k = 3  # x^(8 * length) = x^(2^3 * length)
    while length:
        if length & 1:
            x2n = _crc32_multiply(_CRC32_X2N[k & 31], x2n)
        length >>= 1
        k += 1
    return _crc32_multiply(x2n, crc)


def _check_last_member(
    gz_path: Path, partial_path: Path, checkpoints: list[tuple[int, int]]
) -> None:
    """Raise ``ValueError`` unless the .gz ends with the footer of a gzip member
    whose output ends exactly where the decompressed output does.

    ``checkpoints`` holds ``(offset, CRC-32 of the output before offset)``,
    ascending, from ``(0, 0)`` to the end of the output.  The footer holds the
    last member's CRC-32 and its size modulo 2^32, so every size it allows is
    tried; each needs only the CRC of the output between the checkpoint before
    the member's start and that start, read back from ``partial_path``.
    """
    with open(gz_path, "rb") as gz:
        # rapidgzip also decodes raw deflate, which can be shorter than this.
        if os.fstat(gz.fileno()).st_size < 8:
            raise ValueError("too short to end with a gzip footer")
        gz.seek(-8, os.SEEK_END)
        footer_crc, footer_size = struct.unpack("<II", gz.read(8))
    total, total_crc = checkpoints[-1]
    for size in range(footer_size, total + 1, 1 << 32):
        start = total - size
        i = bisect.bisect_right(checkpoints, start, key=lambda c: c[0]) - 1
        offset, prefix_crc = checkpoints[i]
        if offset < start:
            with open(partial_path, "rb") as out:
                out.seek(offset)
                prefix_crc = zlib.crc32(out.read(start - offset), prefix_crc)
        # CRC(prefix + member) = shift(CRC(prefix), len(member)) ^ CRC(member)
        if total_crc ^ _crc32_shift(prefix_crc, size) == footer_crc:
            return
    raise ValueError("the output doesn't end with a complete gzip member")
