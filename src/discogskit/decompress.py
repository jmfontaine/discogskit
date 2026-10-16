"""Gzip decompression via rapidgzip.

Decompresses to disk (not streamed) because mmap-based chunk splitting in
``entities/_split.py`` requires random access to the full XML file.  rapidgzip
parallelizes decompression across cores by exploiting the block structure of
the deflate format, which is significantly faster than single-threaded
gzip/igzip for large files (~58 GB uncompressed for the full releases dump).

An existing ``.xml`` is reused as a cache, so it must only ever hold complete
output: decompression writes ``<name>.xml.partial`` and renames it into place
once every byte is on disk.

rapidgzip runs in a child process, which writes ``.partial`` and reports
success or the exception that stopped it.  On some truncated input rapidgzip
aborts the process or hangs, in native code: the parent raises
``DecompressError`` when the child dies or exits non-zero, even after
reporting success, or goes ``_STALL_TIMEOUT`` without reading a chunk, as it
does for rapidgzip's exceptions.  The parent kills the child on every way
out, Ctrl+C included.
If the parent dies, the child stops before it starts writing or after its
next chunk, so not while rapidgzip hangs.
The child's stderr, where rapidgzip's native code prints, goes to a temporary
file: the parent adds its text to the ``DecompressError`` message, and prints
it on the current console in every other case.

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
import multiprocessing
import os
import signal
import struct
import tempfile
import threading
import time
import zlib
from collections.abc import Callable
from contextlib import suppress
from multiprocessing.connection import Connection
from multiprocessing.process import BaseProcess
from pathlib import Path
from typing import BinaryIO

import rapidgzip
from filelock import ReadWriteLock, Timeout
from rich.text import Text

from discogskit._console import print_text, status

# A blocking acquire waits inside SQLite, where Ctrl+C isn't handled until it
# returns, so wait in slices this long.
_LOCK_WAIT_SLICE = 0.5

# How often the parent checks on the decompression child.
_POLL_INTERVAL = 0.5

# How long the decompression child may go without reading a chunk before it
# counts as hung, as rapidgzip does on some truncated input.  Progress, not
# total time: a healthy run reads a 4 MiB chunk many times a second, while a
# full dump takes minutes.
_STALL_TIMEOUT = 60.0

# How long the decompression child may take to start running.  Starting it
# imports discogskit and, with spawn, the calling script again: one that calls
# discogskit outside an ``if __name__ == "__main__":`` guard waits there
# forever for the lock this run holds.
_START_TIMEOUT = 60.0

# How much of what the decompression child printed the parent reads, from the end: what it prints after a run that
# didn't fail with a DecompressError, and what it folds into a DecompressError's message, cut to
# _CHILD_OUTPUT_CHARS characters.  A flood of output must not cost memory or time.
_CHILD_OUTPUT_BYTES = 16 * 1024
_CHILD_OUTPUT_CHARS = 2000


class DecompressError(Exception):
    """Raised when the .gz can't be decompressed (truncated, corrupt, not gzip).

    ``detail``, when set, is what the decompressor printed before failing.
    """

    def __init__(self, path: Path, detail: str = "") -> None:
        message = (
            f"failed to decompress {path.name} (truncated, corrupt, or not a gzip file)"
        )
        super().__init__(f"{message}: {detail}" if detail else message)


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
        _run_decompress_child(gz_path, partial_path, workers)
        # Without this, a power loss can persist the rename below before the
        # data, leaving a complete-looking but corrupt .xml.  It covers the
        # child's writes: they're this file's, whichever process syncs it.
        # Read-only, as a umask may have left the file: POSIX fsync doesn't
        # need write access, though Windows' does.
        with open(partial_path, "r+b" if os.name == "nt" else "rb") as written:
            os.fsync(written.fileno())
    except BaseException:
        # Including OSError, the filesystem (disk full, permissions) rather
        # than the gzip, which surfaces unchanged so the errno and path reach
        # the user.  The child is gone by now, so nothing writes this again.
        partial_path.unlink(missing_ok=True)
        raise
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
    size_mib = xml_path.stat().st_size / 2**20
    status("Decompress", f"{size_mib:,.1f} MiB", f"[{elapsed:.2f}s]")


def _run_decompress_child(gz_path: Path, partial_path: Path, workers: int) -> None:
    """Run ``_decompress_child`` in a new process, its stderr captured; raise what it reports.

    A ``DecompressError`` carries what the child printed; otherwise it's printed on the current console.
    """
    fd, stderr_path = tempfile.mkstemp(prefix="discogskit-decompress-", suffix=".log")
    try:
        with open(fd, "rb") as log:
            # mkstemp's mode goes through the umask; chmod's doesn't, and the child opens the file again to write it.
            os.chmod(stderr_path, 0o600)
            try:
                _supervise_decompress_child(gz_path, partial_path, workers, stderr_path)
            except DecompressError as exc:
                output, size = _child_output(log)
                # On one line, and only its end: the message is a line of output.
                detail = " ".join(output.split())
                if size > _CHILD_OUTPUT_BYTES or len(detail) > _CHILD_OUTPUT_CHARS:
                    # Even if the end is blank: what came before it is cut.
                    detail = "…" + detail[-_CHILD_OUTPUT_CHARS:]
                if not detail:
                    raise
                raise DecompressError(gz_path, detail) from exc.__cause__
            except BaseException:
                _pass_on_child_output(log)
                raise
            _pass_on_child_output(log)
    finally:
        # The child removes it once it has it open, unless it never got that far or the system can't remove an
        # open file (Windows).
        with suppress(OSError):
            os.remove(stderr_path)


def _child_output(log: BinaryIO) -> tuple[str, int]:
    """The end of what the decompression child printed, at most ``_CHILD_OUTPUT_BYTES`` of it, and the size in
    bytes of all of it; ``("", 0)`` if ``log`` can't be read.
    """
    try:
        size = os.fstat(log.fileno()).st_size
        log.seek(max(0, size - _CHILD_OUTPUT_BYTES))
        return log.read(_CHILD_OUTPUT_BYTES).decode(errors="replace"), size
    except OSError:
        return "", 0


def _pass_on_child_output(log: BinaryIO) -> None:
    """Print the end of what the decompression child printed, if anything, literally on the current console."""
    output, size = _child_output(log)
    if size > _CHILD_OUTPUT_BYTES:
        # Even if the end is blank: what came before it is cut.
        print_text(
            Text(
                f"The decompressor printed {size:,} bytes; "
                f"only the last {_CHILD_OUTPUT_BYTES // 1024} KiB follow."
            )
        )
    output = output.rstrip()
    if output:
        print_text(Text(output))


def _supervise_decompress_child(
    gz_path: Path, partial_path: Path, workers: int, stderr_path: str
) -> None:
    """Run ``_decompress_child`` in a new process, its stderr going to ``stderr_path``; raise what it reports.

    On some truncated input, rapidgzip aborts the process or hangs, in native
    code Python can't catch or interrupt.  In a child, an abort is the child
    dying without a report, and a hang is the child going ``_STALL_TIMEOUT``
    without reading a chunk: both raise ``DecompressError``, the child having
    been killed, as on every other way out of here.
    """
    # Spawn, like the parse workers: forking a process that runs threads (the
    # writer, a library caller's) can copy a lock another thread holds.
    ctx = multiprocessing.get_context("spawn")
    receiver, sender = ctx.Pipe(duplex=False)
    child = ctx.Process(
        args=(gz_path, partial_path, sender, workers, stderr_path),
        name="discogskit-decompress",
        target=_decompress_child,
    )
    try:
        _start_holding_ctrl_c(child)
        # With the child holding the only sending end, its death is EOF here.
        sender.close()
        running = False
        heard_at = time.monotonic()
        while True:
            if not receiver.poll(_POLL_INTERVAL):
                silent = time.monotonic() - heard_at
                if not running and silent >= _START_TIMEOUT:
                    raise RuntimeError(
                        f"the decompression process didn't start within"
                        f" {_START_TIMEOUT:g}s; a script that runs discogskit"
                        ' must do so under `if __name__ == "__main__":`'
                    )
                if running and silent >= _STALL_TIMEOUT:
                    raise DecompressError(gz_path) from TimeoutError(
                        f"decompression made no progress for {_STALL_TIMEOUT:g}s"
                    )
                continue
            try:
                message = receiver.recv()
            except EOFError:
                child.join()
                raise DecompressError(gz_path) from ChildProcessError(
                    f"the decompression process {_describe_exit(child.exitcode)}"
                )
            if isinstance(message, int):  # progress
                running = True
                heard_at = time.monotonic()
                continue
            error = message
            break
        # It has closed rapidgzip and the output, so it's only exiting.
        child.join(_STALL_TIMEOUT)
        # Success counts only if the child then exits cleanly: a non-zero
        # exit or a signal death fails the run wherever it happens.
        if error is None and child.exitcode != 0:
            raise DecompressError(gz_path) from ChildProcessError(
                f"the decompression process {_describe_exit(child.exitcode)}"
                " after reporting success"
            )
    finally:
        receiver.close()
        sender.close()
        if child.pid is not None:  # started
            child.kill()
            child.join()
    if isinstance(error, (RuntimeError, ValueError)):
        # rapidgzip raises ValueError for corrupt or non-gzip input and a bare
        # RuntimeError('std::exception') for truncated input; the child's own
        # checks raise ValueError for truncation rapidgzip doesn't report.
        raise DecompressError(gz_path) from error
    if error is not None:
        raise error


def _describe_exit(exitcode: int | None) -> str:
    """How a process that ended with ``exitcode`` ended, for an error message."""
    if exitcode is None:
        return f"didn't exit within {_STALL_TIMEOUT:g}s"
    if exitcode < 0:
        try:
            return f"was killed by {signal.Signals(-exitcode).name}"
        except ValueError:  # a signal Python has no name for
            return f"was killed by signal {-exitcode}"
    return f"exited with status {exitcode}"


def _start_holding_ctrl_c(child: BaseProcess) -> None:
    """Start ``child``, holding back Ctrl+C until ``child`` records its pid.

    ``start()`` launches the process before it records it, so an interrupt in
    between would leave a running child that ``child.kill()`` can't reach.
    """
    if threading.current_thread() is not threading.main_thread():
        child.start()  # Python delivers Ctrl+C to the main thread only.
        return
    pending = False

    def hold(signum: int, frame: object) -> None:
        nonlocal pending
        pending = True

    previous = signal.signal(signal.SIGINT, hold)
    try:
        child.start()
    finally:
        signal.signal(signal.SIGINT, previous)
        if pending:  # Delivered again, to whatever handles it normally.
            signal.raise_signal(signal.SIGINT)


def _decompress_child(
    gz_path: Path,
    partial_path: Path,
    result: Connection,
    workers: int,
    stderr_path: str,
) -> None:
    """Decompress ``gz_path`` into ``partial_path``, in the process
    ``_supervise_decompress_child`` starts.

    Sends on ``result``, for the parent's watchdog, the number of bytes
    written so far (an ``int``) once running and after each chunk; then
    ``None`` once ``partial_path`` holds the complete output, or the exception
    that stopped it.  Stops before writing anything or after a chunk, sending
    nothing more, if the parent has died.  Its stderr goes to ``stderr_path``.
    """
    # rapidgzip's native code prints to file descriptor 2, which Python's
    # sys.stderr also writes to: the parent reads it from the file.
    log = os.open(stderr_path, os.O_WRONLY | os.O_APPEND)
    os.dup2(log, 2)
    os.close(log)
    # The parent reads it through the handle it keeps open: removed now, the
    # file isn't left behind by a parent killed before its own cleanup.
    with suppress(OSError):
        os.remove(stderr_path)
    # The parent handles Ctrl+C, the terminal's for both processes, and kills
    # this one; here it would only print a traceback.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    # A parent that died (killed, say) can't kill this process, and after it
    # the lock and the .partial path belong to other runs.  This is the
    # process that started this one even if it died before this line ran, by
    # when os.getppid() would name whichever process adopted this one.
    parent = multiprocessing.parent_process()

    def orphaned() -> bool:
        return parent is not None and not parent.is_alive()

    if orphaned():
        return
    result.send(0)
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
                if orphaned():
                    return
                result.send(offset + len(chunk))
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
    except Exception as exc:  # noqa: BLE001 - the parent sorts them out
        result.send(exc)
    else:
        result.send(None)


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
