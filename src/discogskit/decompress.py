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

import bisect
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
        os.replace(partial_path, xml_path)
    except OSError:
        # Not a decompression failure: surface the real filesystem error.
        partial_path.unlink(missing_ok=True)
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


def _crc32_x2n_table() -> tuple[int, ...]:
    """x^(2^k) modulo the CRC-32 polynomial for k = 0..31 (zlib's x2n_table)."""
    table = [1 << 30]  # x^1
    while len(table) < 32:
        table.append(_crc32_multiply(table[-1], table[-1]))
    return tuple(table)


_CRC32_X2N = _crc32_x2n_table()


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
        if total_crc ^ _crc32_shift(prefix_crc, size) != footer_crc:
            continue
        # An empty member's footer is eight zero bytes, so it matches whenever
        # the file ends in zeros, as a header cut after a zero-filled extra
        # field does: only accept it if an empty member is really there.
        if size > 0 or _ends_with_empty_member(gz_path):
            return
    raise ValueError("the output doesn't end with a complete gzip member")


# How far back from the end of the .gz to look for an empty last member.  Real
# ones are small: a header (its extra field is at most 64 KiB, plus a file name
# and a comment) and an empty deflate stream of a few bytes.  A valid file whose
# empty last member is longer, which takes a huge name or comment or thousands
# of empty deflate blocks, is rejected rather than risk publishing a truncated
# one.  The bound also caps the cost: each candidate is parsed to the end.
_EMPTY_MEMBER_SEARCH_BYTES = 256 * 1024


def _ends_with_empty_member(gz_path: Path) -> bool:
    """Whether the .gz ends with a complete gzip member that decompresses to
    nothing, starting within its last ``_EMPTY_MEMBER_SEARCH_BYTES``.
    """
    with open(gz_path, "rb") as gz:
        gz.seek(max(0, os.fstat(gz.fileno()).st_size - _EMPTY_MEMBER_SEARCH_BYTES))
        tail = gz.read()
    view = memoryview(tail)
    start = tail.rfind(b"\x1f\x8b\x08")
    while start >= 0:
        member = zlib.decompressobj(wbits=31)  # gzip framing
        try:
            # One byte of output is enough to rule a candidate out.
            output = member.decompress(view[start:], 1)
        except zlib.error:
            output = None
        if (
            output == b""
            and member.eof
            and not member.unused_data
            and not member.unconsumed_tail
        ):
            return True
        start = tail.rfind(b"\x1f\x8b\x08", 0, start)
    return False
