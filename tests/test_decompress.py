"""Tests for decompress module."""

from __future__ import annotations

import errno
import functools
import gzip
import io
import multiprocessing
import os
import random
import signal
import threading
import time
from multiprocessing.context import SpawnProcess

import pytest
from filelock import ReadWriteLock, Timeout

from discogskit import decompress
from discogskit.decompress import DecompressError, ForeignXmlError, ensure_xml

_XML = b"<?xml version='1.0'?>\n<root>hello</root>"

# A complete gzip member header whose extra field is eight zero bytes: cut
# here, the file ends like an empty member's all-zero footer.
_HEADER_ZERO_EXTRA = bytes.fromhex("1f8b0804000000000000") + b"\x08\x00" + bytes(8)

# A gzip member header cut inside its file name ("test.xml").
_HEADER_NAME_CUT = bytes.fromhex("1f8b0808000000000003") + b"test.x"

# Where to cut _labels_gz() so rapidgzip 0.16.0's Linux wheels (ISA-L) abort
# the interpreter: "The bit buffer should not contain more data than have been
# read from the file!".  Its macOS wheels raise an exception instead.
_ABORTING_CUTS = (88_082, 198_185, 616_578)


@functools.cache
def _labels_gz():
    """A deterministic labels-like dump, about 880 KB compressed."""
    rng = random.Random(0)
    words = [
        "acid",
        "ambient",
        "bass",
        "blues",
        "dub",
        "electro",
        "folk",
        "funk",
        "house",
        "jazz",
        "punk",
        "soul",
        "techno",
    ]
    records = [b"<labels>"]
    for i in range(50_000):
        name = " ".join(rng.choices(words, k=rng.randint(1, 6)))
        records.append(
            f"<label><id>{i}</id><name>{name}</name>"
            f"<profile>{rng.random()}</profile></label>".encode()
        )
    records.append(b"</labels>")
    return gzip.compress(b"\n".join(records), mtime=0)


def _write_gz(path, content=_XML):
    with gzip.open(path, "wb") as f:
        f.write(content)
    return path


def _leave_cached_xml(gz_path, xml_path):
    """Decompress like a ``--keep-xml`` run, leaving discogskit's own XML behind."""
    ensure_xml(gz_path, xml_path, workers=1).release(remove_xml=False)


def _record_decompressions(monkeypatch):
    """Spy on ``_decompress``: returns the list of .gz paths it decompresses."""
    decompressed = []
    real_decompress = decompress._decompress

    def recording_decompress(gz_path, *args):
        decompressed.append(gz_path)
        real_decompress(gz_path, *args)

    monkeypatch.setattr(decompress, "_decompress", recording_decompress)
    return decompressed


def _run_patched_child(patches, *args):
    """The decompression child, with ``patches`` applied first: values for
    dotted names under ``discogskit.decompress``, such as ``rapidgzip.open``.
    """
    with pytest.MonkeyPatch.context() as mp:
        for name, value in patches.items():
            mp.setattr(f"discogskit.decompress.{name}", value, raising=False)
        decompress._decompress_child(*args)


def _patch_child(monkeypatch, patches):
    """Apply ``patches`` in the decompression child, which ``monkeypatch``
    can't reach: it's another process.  Values must pickle.
    """
    monkeypatch.setattr(
        decompress,
        "_decompress_child",
        functools.partial(_run_patched_child, patches),
    )


class _CompleteGzipReader(io.BytesIO):
    """Stands in for ``rapidgzip.open`` on a complete .gz at ``path``: yields
    ``_XML`` and, like rapidgzip, reports every compressed bit as consumed.
    """

    def __init__(self, path, parallelization):
        super().__init__(_XML)
        self._path = path

    def tell_compressed(self):
        return os.path.getsize(self._path) * 8


class _AbortingReader(_CompleteGzipReader):
    """Aborts the process, as rapidgzip does on some truncated input."""

    def read(self, size=-1):
        os.abort()


class _GatedReader(_CompleteGzipReader):
    """The first read signals ``started``, then blocks until ``go``."""

    def __init__(self, path, parallelization, *, go, started):
        super().__init__(path, parallelization)
        self._go = go
        self._read_once = False
        self._started = started

    def read(self, size=-1):
        if not self._read_once:
            self._read_once = True
            self._started.set()
            self._go.wait()
        return super().read(size)


class _InterruptingReader(_CompleteGzipReader):
    """Reports its pid, sends the parent a Ctrl+C, then never returns."""

    def __init__(self, path, parallelization, *, pids):
        super().__init__(path, parallelization)
        self._pids = pids

    def read(self, size=-1):
        self._pids.put(os.getpid())
        # put() only queues it for a feeder thread: wait until it's sent.
        self._pids.close()
        self._pids.join_thread()
        os.kill(os.getppid(), signal.SIGINT)
        time.sleep(3600)


class _SlowReader(_CompleteGzipReader):
    """Pauses before each four-byte read: slow overall, never stalled."""

    def read(self, size=-1):
        time.sleep(0.2)
        return super().read(4)


def _crashing_child(*args):
    """The decompression child, exiting with status 1 after its report."""
    decompress._decompress_child(*args)
    os._exit(1)


def _never_starting_child(*args):
    """Stands in for ``_decompress_child``: never reports it's running."""
    time.sleep(3600)


class _StalledReader(_CompleteGzipReader):
    """Never returns, as rapidgzip does on some truncated input."""

    def read(self, size=-1):
        time.sleep(3600)


class _XmlCheckingReader(_CompleteGzipReader):
    """Reads a byte at a time, failing if the .xml exists during a read."""

    def read(self, size=-1):
        if os.path.exists(self._path.removesuffix(".gz")):
            raise AssertionError("the .xml exists during decompression")
        return super().read(1)


class _FullDisk(io.BufferedWriter):
    """Stands in for ``open``: every write fails with ENOSPC."""

    def __init__(self, path, mode):
        super().__init__(io.FileIO(path, mode))
        self._path = os.fspath(path)

    def write(self, data):
        raise OSError(errno.ENOSPC, "No space left on device", self._path)


def _child_outliving_parent(running, finished, *args):
    """The decompression child, run only once its parent has died."""
    running.set()
    parent = multiprocessing.parent_process()
    assert parent is not None
    while parent.is_alive():
        time.sleep(0.05)
    try:
        decompress._decompress_child(*args)
    finally:
        finished.set()


def _ensure_xml_then_die(gz_path, xml_path, running, finished):
    """Run ``ensure_xml`` in its own process, which dies abruptly once the
    decompression child is running.
    """

    def die_once_running():
        running.wait()
        os._exit(0)

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            decompress,
            "_decompress_child",
            functools.partial(_child_outliving_parent, running, finished),
        )
        threading.Thread(target=die_once_running, daemon=True).start()
        ensure_xml(gz_path, xml_path, workers=1)


def _ensure_xml_in_child(xml_path, blocked, go, results, started):
    """Run ``ensure_xml`` in its own process; report the XML it returned with.

    ``started`` is set once this run is decompressing, and ``blocked`` once it
    waits for another run's lock. Decompression finishes only after ``go``.
    """

    def reporting_wait(acquire):
        def wrapper(self, timeout=None, *, blocking=None):
            try:
                return acquire(self, blocking=False)
            except Timeout:
                blocked.set()
            return acquire(self, timeout, blocking=blocking)

        return wrapper

    with pytest.MonkeyPatch.context() as mp:
        for name in ("acquire_read", "acquire_write"):
            mp.setattr(
                ReadWriteLock, name, reporting_wait(getattr(ReadWriteLock, name))
            )
        _patch_child(
            mp,
            {"rapidgzip.open": functools.partial(_GatedReader, go=go, started=started)},
        )
        try:
            lease = ensure_xml(xml_path.with_name("test.xml.gz"), xml_path, workers=1)
            results.put(xml_path.read_bytes())
            lease.release(remove_xml=False)
        except BaseException as exc:  # noqa: BLE001
            results.put(repr(exc))


class TestPartialFile:
    """``.xml`` is the cache, so it must never hold incomplete output."""

    def test_corrupt_gz_leaves_no_xml(self, tmp_path):
        gz_path = tmp_path / "test.xml.gz"
        gz_path.write_bytes(b"not a real gz")
        xml_path = tmp_path / "test.xml"

        with pytest.raises(DecompressError):
            ensure_xml(gz_path, xml_path, workers=1)

        assert not xml_path.exists()
        assert not (tmp_path / "test.xml.partial").exists()

    def test_disk_full_keeps_os_error_and_removes_partial(self, monkeypatch, tmp_path):
        """ENOSPC isn't reported as a corrupt gzip, and leaves no .partial."""
        gz_path = _write_gz(tmp_path / "test.xml.gz")
        xml_path = tmp_path / "test.xml"
        partial_path = tmp_path / "test.xml.partial"
        # Shadows the builtin inside the module only.
        _patch_child(monkeypatch, {"open": _FullDisk})

        with pytest.raises(OSError) as excinfo:
            ensure_xml(gz_path, xml_path, workers=1)

        assert excinfo.value.errno == errno.ENOSPC
        assert excinfo.value.filename == str(partial_path)
        assert not isinstance(excinfo.value, DecompressError)
        assert not xml_path.exists()
        assert not partial_path.exists()

    def test_keyboard_interrupt_stops_child_and_removes_partial(
        self, monkeypatch, tmp_path
    ):
        """Ctrl+C mid-decompression leaves no child process and no .partial."""
        gz_path = _write_gz(tmp_path / "test.xml.gz")
        xml_path = tmp_path / "test.xml"
        partial_path = tmp_path / "test.xml.partial"
        pids = multiprocessing.get_context("spawn").Queue()
        reader = functools.partial(_InterruptingReader, pids=pids)
        _patch_child(monkeypatch, {"rapidgzip.open": reader})

        with pytest.raises(KeyboardInterrupt):
            ensure_xml(gz_path, xml_path, workers=1)

        with pytest.raises(ProcessLookupError):  # killed and reaped
            os.kill(pids.get(timeout=30), 0)
        assert not xml_path.exists()
        assert not partial_path.exists()

    def test_keyboard_interrupt_while_child_starts_stops_it(
        self, monkeypatch, tmp_path
    ):
        """Ctrl+C after ``start()`` launches the child but before it records it."""
        gz_path = _write_gz(tmp_path / "test.xml.gz")
        xml_path = tmp_path / "test.xml"
        _patch_child(monkeypatch, {"rapidgzip.open": _StalledReader})
        launched = []
        real_popen = SpawnProcess._Popen

        def popen_then_interrupt(process_obj):
            popen = real_popen(process_obj)
            launched.append(popen.pid)
            signal.raise_signal(signal.SIGINT)
            return popen

        monkeypatch.setattr(SpawnProcess, "_Popen", staticmethod(popen_then_interrupt))

        with pytest.raises(KeyboardInterrupt):
            ensure_xml(gz_path, xml_path, workers=1)

        with pytest.raises(ProcessLookupError):  # killed and reaped
            os.kill(launched[0], 0)
        assert not (tmp_path / "test.xml.partial").exists()

    def test_orphaned_child_writes_nothing(self, tmp_path):
        """A child whose parent died before it ran leaves the .partial path
        alone: another run may own it by then.
        """
        ctx = multiprocessing.get_context("spawn")
        gz_path = _write_gz(tmp_path / "test.xml.gz")
        running, finished = ctx.Event(), ctx.Event()
        parent = ctx.Process(
            args=(gz_path, tmp_path / "test.xml", running, finished),
            target=_ensure_xml_then_die,
        )
        parent.start()
        parent.join(30)
        if parent.is_alive():  # a hung run must not hang the suite
            parent.kill()
            parent.join()

        assert parent.exitcode == 0  # os._exit(0) once the child ran
        assert finished.wait(30)
        assert not (tmp_path / "test.xml.partial").exists()

    @pytest.mark.skipif(
        not hasattr(os, "geteuid") or os.geteuid() == 0,
        reason="root writes read-only files",
    )
    def test_read_only_umask_decompresses(self, tmp_path):
        """A umask that leaves new files read-only doesn't stop the run: only
        the child's creating handle writes the output.
        """
        gz_path = _write_gz(tmp_path / "test.xml.gz")
        xml_path = tmp_path / "test.xml"
        (tmp_path / "test.xml.lock").touch()  # the lock needs a writable file
        umask = os.umask(0o222)
        try:
            lease = ensure_xml(gz_path, xml_path, workers=1)
        finally:
            os.umask(umask)
        lease.release(remove_xml=False)

        assert xml_path.read_bytes() == _XML

    def test_rename_failure_removes_partial_and_keeps_error(
        self, monkeypatch, tmp_path
    ):
        """A failed rename isn't reported as a corrupt gzip, and leaves no .partial."""
        gz_path = _write_gz(tmp_path / "test.xml.gz")
        xml_path = tmp_path / "test.xml"
        error = PermissionError(13, "Permission denied", str(xml_path))

        def failing_replace(src, dst):
            raise error

        monkeypatch.setattr(decompress.os, "replace", failing_replace)

        with pytest.raises(PermissionError) as excinfo:
            ensure_xml(gz_path, xml_path, workers=1)

        assert excinfo.value is error
        assert not xml_path.exists()
        assert not (tmp_path / "test.xml.partial").exists()

    def test_stale_partial_removed_when_xml_cached(self, tmp_path):
        gz_path = _write_gz(tmp_path / "test.xml.gz")
        xml_path = tmp_path / "test.xml"
        _leave_cached_xml(gz_path, xml_path)
        partial_path = tmp_path / "test.xml.partial"
        partial_path.write_bytes(b"<?xml version='1.0'?>\n<ro")

        ensure_xml(gz_path, xml_path, workers=1).release(remove_xml=False)

        assert xml_path.read_bytes() == _XML
        assert not partial_path.exists()

    def test_stale_partial_replaced(self, tmp_path):
        """A crashed run's .partial is neither reused nor appended to."""
        gz_path = _write_gz(tmp_path / "test.xml.gz")
        xml_path = tmp_path / "test.xml"
        partial_path = tmp_path / "test.xml.partial"
        partial_path.write_bytes(b"<?xml version='1.0'?>\n<ro" * 1000)

        ensure_xml(gz_path, xml_path, workers=1).release(remove_xml=False)

        assert xml_path.read_bytes() == _XML
        assert not partial_path.exists()

    def test_truncated_gz_raises_decompress_error(self, tmp_path):
        gz_path = tmp_path / "test.xml.gz"
        gz_path.write_bytes(gzip.compress(_XML * 1000)[:-20])
        xml_path = tmp_path / "test.xml"

        with pytest.raises(
            DecompressError, match=r"truncated, corrupt, or not a gzip file"
        ) as excinfo:
            ensure_xml(gz_path, xml_path, workers=1)

        assert excinfo.value.__cause__ is not None
        assert not xml_path.exists()
        assert not (tmp_path / "test.xml.partial").exists()

    @pytest.mark.parametrize(
        "cut",
        [
            lambda gz: gz[:10],
            lambda gz: gz + gz[:-20],
            lambda gz: gz + gz[:10],
            lambda gz: gz + _HEADER_ZERO_EXTRA,
        ],
        ids=[
            "header_only",
            "second_member_cut",
            "second_member_header_only",
            "second_member_header_ending_in_zeros",
        ],
    )
    def test_truncation_rapidgzip_misses_raises_decompress_error(self, tmp_path, cut):
        """rapidgzip reads these as a clean end of data (with ISA-L: its Linux
        wheels), so only discogskit's own checks catch them.
        """
        gz_path = tmp_path / "test.xml.gz"
        gz_path.write_bytes(cut(gzip.compress(_XML * 1000)))
        xml_path = tmp_path / "test.xml"

        with pytest.raises(
            DecompressError, match=r"truncated, corrupt, or not a gzip file"
        ):
            ensure_xml(gz_path, xml_path, workers=1)

        assert not xml_path.exists()
        assert not (tmp_path / "test.xml.partial").exists()

    @pytest.mark.parametrize(
        "tail",
        [gzip.compress(_XML)[:10], _HEADER_ZERO_EXTRA],
        ids=["header", "header_ending_in_zeros"],
    )
    def test_unfinished_last_member_raises_decompress_error(
        self, monkeypatch, tmp_path, tail
    ):
        """Like rapidgzip with ISA-L on a .gz cut right after a later member's
        header: the first member's output, every byte reported as consumed.
        """
        gz_path = tmp_path / "test.xml.gz"
        gz_path.write_bytes(gzip.compress(_XML) + tail)
        xml_path = tmp_path / "test.xml"
        _patch_child(monkeypatch, {"rapidgzip.open": _CompleteGzipReader})

        with pytest.raises(
            DecompressError, match=r"truncated, corrupt, or not a gzip file"
        ):
            ensure_xml(gz_path, xml_path, workers=1)

        assert not xml_path.exists()
        assert not (tmp_path / "test.xml.partial").exists()

    def test_xml_appears_only_after_decompression(self, monkeypatch, tmp_path):
        gz_path = _write_gz(tmp_path / "test.xml.gz")
        xml_path = tmp_path / "test.xml"
        _patch_child(monkeypatch, {"rapidgzip.open": _XmlCheckingReader})

        ensure_xml(gz_path, xml_path, workers=1).release(remove_xml=False)

        assert xml_path.read_bytes() == _XML


class TestNativeFailures:
    """rapidgzip runs in a child process, so its aborts and hangs on bad input
    end in ``DecompressError`` like its exceptions do.
    """

    def test_abort_raises_decompress_error(self, monkeypatch, tmp_path):
        gz_path = _write_gz(tmp_path / "test.xml.gz")
        xml_path = tmp_path / "test.xml"
        _patch_child(monkeypatch, {"rapidgzip.open": _AbortingReader})

        with pytest.raises(
            DecompressError, match=r"truncated, corrupt, or not a gzip file"
        ) as excinfo:
            ensure_xml(gz_path, xml_path, workers=1)

        assert "SIGABRT" in str(excinfo.value.__cause__)
        assert not xml_path.exists()
        assert not (tmp_path / "test.xml.partial").exists()

    def test_child_that_never_starts_raises_runtime_error(self, monkeypatch, tmp_path):
        """Like a script without a ``__main__`` guard, run again in the child."""
        gz_path = _write_gz(tmp_path / "test.xml.gz")
        xml_path = tmp_path / "test.xml"
        monkeypatch.setattr(decompress, "_START_TIMEOUT", 1)
        monkeypatch.setattr(decompress, "_decompress_child", _never_starting_child)

        with pytest.raises(RuntimeError, match=r"didn't start within 1s"):
            ensure_xml(gz_path, xml_path, workers=1)

        assert not multiprocessing.active_children()
        assert not xml_path.exists()
        assert not (tmp_path / "test.xml.partial").exists()

    def test_crash_after_success_raises_decompress_error(self, monkeypatch, tmp_path):
        """A child that reports success, then exits non-zero, fails the run."""
        gz_path = _write_gz(tmp_path / "test.xml.gz")
        xml_path = tmp_path / "test.xml"
        monkeypatch.setattr(decompress, "_decompress_child", _crashing_child)

        with pytest.raises(
            DecompressError, match=r"truncated, corrupt, or not a gzip file"
        ) as excinfo:
            ensure_xml(gz_path, xml_path, workers=1)

        assert "exited with status 1 after reporting success" in str(
            excinfo.value.__cause__
        )
        assert not xml_path.exists()
        assert not (tmp_path / "test.xml.partial").exists()

    @pytest.mark.parametrize("cut", _ABORTING_CUTS)
    def test_rapidgzip_abort_raises_decompress_error(self, tmp_path, cut):
        gz_path = tmp_path / "test.xml.gz"
        gz_path.write_bytes(_labels_gz()[:cut])
        xml_path = tmp_path / "test.xml"

        with pytest.raises(
            DecompressError, match=r"truncated, corrupt, or not a gzip file"
        ):
            ensure_xml(gz_path, xml_path, workers=1)

        assert not xml_path.exists()
        assert not (tmp_path / "test.xml.partial").exists()

    def test_rapidgzip_hang_raises_decompress_error(self, monkeypatch, tmp_path):
        """rapidgzip's Linux wheels hang on a later member cut in its file name."""
        gz_path = tmp_path / "test.xml.gz"
        gz_path.write_bytes(gzip.compress(_XML * 1000) + _HEADER_NAME_CUT)
        xml_path = tmp_path / "test.xml"
        monkeypatch.setattr(decompress, "_STALL_TIMEOUT", 1)

        with pytest.raises(
            DecompressError, match=r"truncated, corrupt, or not a gzip file"
        ):
            ensure_xml(gz_path, xml_path, workers=1)

        assert not multiprocessing.active_children()
        assert not xml_path.exists()
        assert not (tmp_path / "test.xml.partial").exists()

    def test_slow_decompression_isnt_stalled(self, monkeypatch, tmp_path):
        """The watchdog limits the time between reads, not the total."""
        gz_path = _write_gz(tmp_path / "test.xml.gz")
        xml_path = tmp_path / "test.xml"
        monkeypatch.setattr(decompress, "_STALL_TIMEOUT", 1)
        _patch_child(monkeypatch, {"rapidgzip.open": _SlowReader})

        ensure_xml(gz_path, xml_path, workers=1).release(remove_xml=False)

        assert xml_path.read_bytes() == _XML

    def test_stall_raises_decompress_error(self, monkeypatch, tmp_path):
        gz_path = _write_gz(tmp_path / "test.xml.gz")
        xml_path = tmp_path / "test.xml"
        monkeypatch.setattr(decompress, "_STALL_TIMEOUT", 1)
        _patch_child(monkeypatch, {"rapidgzip.open": _StalledReader})
        start = time.monotonic()

        with pytest.raises(
            DecompressError, match=r"truncated, corrupt, or not a gzip file"
        ) as excinfo:
            ensure_xml(gz_path, xml_path, workers=1)

        assert time.monotonic() - start < 30
        assert isinstance(excinfo.value.__cause__, TimeoutError)
        assert not multiprocessing.active_children()
        assert not xml_path.exists()
        assert not (tmp_path / "test.xml.partial").exists()


class TestCachedXml:
    """Only an XML discogskit decompressed from this very .gz is reused or deleted."""

    @pytest.mark.parametrize("change", ["content", "mtime"])
    def test_cache_from_other_gz_decompressed_again(
        self, monkeypatch, tmp_path, change
    ):
        """A ``--keep-xml`` cache from an older dump of the same name isn't reused."""
        gz_path = _write_gz(tmp_path / "test.xml.gz")
        xml_path = tmp_path / "test.xml"
        _leave_cached_xml(gz_path, xml_path)
        newer = _XML.replace(b"hello", b"newer dump")
        if change == "content":
            _write_gz(gz_path, newer)
        else:
            stat = gz_path.stat()
            os.utime(gz_path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 10**9))
        decompressed = _record_decompressions(monkeypatch)

        ensure_xml(gz_path, xml_path, workers=1).release(remove_xml=False)

        assert decompressed == [gz_path]
        assert xml_path.read_bytes() == (newer if change == "content" else _XML)

    def test_empty_cached_xml_recovered(self, tmp_path):
        gz_path = _write_gz(tmp_path / "test.xml.gz")
        xml_path = tmp_path / "test.xml"
        _leave_cached_xml(gz_path, xml_path)
        xml_path.write_bytes(b"")

        ensure_xml(gz_path, xml_path, workers=1).release(remove_xml=False)

        assert xml_path.read_bytes() == _XML

    def test_foreign_xml_rejected_and_untouched(self, monkeypatch, tmp_path):
        """An XML a user made (``gunzip -k``, trimmed copy) is never converted or deleted."""
        gz_path = _write_gz(tmp_path / "test.xml.gz")
        xml_path = tmp_path / "test.xml"
        xml_path.write_bytes(b"the user's own file")
        decompressed = _record_decompressions(monkeypatch)

        for _ in range(2):  # the failed attempt doesn't adopt the file either
            with pytest.raises(
                ForeignXmlError,
                match=r"^test\.xml exists but wasn't written by discogskit; "
                r"move or delete it$",
            ):
                ensure_xml(gz_path, xml_path, workers=1)

        assert decompressed == []
        assert xml_path.read_bytes() == b"the user's own file"

    def test_gz_of_empty_xml_decompressed_once(self, monkeypatch, tmp_path):
        """An empty result straight from the .gz is returned, not decompressed forever."""
        gz_path = _write_gz(tmp_path / "test.xml.gz", b"")
        xml_path = tmp_path / "test.xml"
        decompressed = _record_decompressions(monkeypatch)

        ensure_xml(gz_path, xml_path, workers=1).release(remove_xml=False)

        assert decompressed == [gz_path]
        assert xml_path.read_bytes() == b""

    def test_interrupt_after_rename_keeps_marker(self, monkeypatch, tmp_path):
        """Ctrl+C landing just after the XML is published leaves it reusable."""
        gz_path = _write_gz(tmp_path / "test.xml.gz")
        xml_path = tmp_path / "test.xml"
        real_replace = os.replace

        def replace_then_interrupt(src, dst):
            real_replace(src, dst)
            raise KeyboardInterrupt

        monkeypatch.setattr(decompress.os, "replace", replace_then_interrupt)
        with pytest.raises(KeyboardInterrupt):
            ensure_xml(gz_path, xml_path, workers=1)
        monkeypatch.undo()
        decompressed = _record_decompressions(monkeypatch)

        ensure_xml(gz_path, xml_path, workers=1).release(remove_xml=False)

        assert decompressed == []
        assert xml_path.read_bytes() == _XML

    def test_own_leftover_reused(self, monkeypatch, tmp_path):
        gz_path = _write_gz(tmp_path / "test.xml.gz")
        xml_path = tmp_path / "test.xml"
        _leave_cached_xml(gz_path, xml_path)
        decompressed = _record_decompressions(monkeypatch)

        ensure_xml(gz_path, xml_path, workers=1).release(remove_xml=False)

        assert decompressed == []
        assert xml_path.read_bytes() == _XML


class TestConcurrentRuns:
    def test_run_starting_mid_decompression_gets_complete_xml(self, tmp_path):
        """Separate processes: CLI runs share nothing in memory, so neither may a test."""
        ctx = multiprocessing.get_context("spawn")
        _write_gz(tmp_path / "test.xml.gz")
        xml_path = tmp_path / "test.xml"
        results = ctx.Queue()
        first_blocked, first_go, first_started = ctx.Event(), ctx.Event(), ctx.Event()
        second_blocked, second_go, second_started = (
            ctx.Event(),
            ctx.Event(),
            ctx.Event(),
        )
        first = ctx.Process(
            args=(xml_path, first_blocked, first_go, results, first_started),
            target=_ensure_xml_in_child,
        )
        second = ctx.Process(
            args=(xml_path, second_blocked, second_go, results, second_started),
            target=_ensure_xml_in_child,
        )
        first.start()
        try:
            assert first_started.wait(30)
            assert (tmp_path / "test.xml.partial").exists()
            second.start()
            # Unfixed, the second run decompresses on its own; fixed, it waits.
            deadline = time.monotonic() + 30
            while not (second_blocked.is_set() or second_started.is_set()):
                assert time.monotonic() < deadline, "second run neither waited nor ran"
                time.sleep(0.01)
            assert second_blocked.is_set()
            assert not second_started.is_set()
            first_go.set()
            returned_first = results.get(timeout=30)
            second_go.set()
            returned_second = results.get(timeout=30)
        finally:
            first_go.set()
            second_go.set()
            for process in (first, second):
                if process.pid is None:  # never started
                    continue
                process.join(30)
                if process.is_alive():  # a hung run must not hang the suite
                    process.kill()
                    process.join()

        assert (first.exitcode, second.exitcode) == (0, 0)
        assert returned_first == _XML
        assert returned_second == _XML
        assert xml_path.read_bytes() == _XML

    def test_last_run_out_removes_xml(self, tmp_path):
        """A finishing run doesn't delete the XML while another run still reads it."""
        gz_path = _write_gz(tmp_path / "test.xml.gz")
        xml_path = tmp_path / "test.xml"
        first = ensure_xml(gz_path, xml_path, workers=1)
        second = ensure_xml(gz_path, xml_path, workers=1)

        second.release(remove_xml=True)
        assert xml_path.read_bytes() == _XML

        first.release(remove_xml=True)
        assert sorted(p.name for p in tmp_path.iterdir()) == [
            "test.xml.gz",
            "test.xml.lock",
        ]


class TestDecompress:
    def test_decompress_gz_file(self, tmp_path):
        """ensure_xml decompresses a .gz file to .xml."""
        xml_content = b"<?xml version='1.0'?>\n<root>hello</root>"
        gz_path = tmp_path / "test.xml.gz"
        with gzip.open(gz_path, "wb") as f:
            f.write(xml_content)

        xml_path = tmp_path / "test.xml"
        ensure_xml(gz_path, xml_path, workers=1).release(remove_xml=False)

        assert xml_path.exists()
        assert xml_path.read_bytes() == xml_content

    @pytest.mark.parametrize(
        "last", [_XML, b""], ids=["non_empty_last_member", "empty_last_member"]
    )
    def test_multi_member_gz_decompressed(self, tmp_path, last):
        """The footer check finds the last member's start inside the output,
        and accepts a real empty last member (an all-zero footer).
        """
        gz_path = tmp_path / "test.xml.gz"
        gz_path.write_bytes(gzip.compress(_XML * 1000) + gzip.compress(last))
        xml_path = tmp_path / "test.xml"

        ensure_xml(gz_path, xml_path, workers=1).release(remove_xml=False)

        assert xml_path.read_bytes() == _XML * 1000 + last
