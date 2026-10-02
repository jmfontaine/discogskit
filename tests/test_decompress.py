"""Tests for decompress module."""

from __future__ import annotations

import gzip
import io
import multiprocessing
import time

import pytest
from filelock import ReadWriteLock, Timeout

from discogskit import decompress
from discogskit.decompress import DecompressError, ensure_xml

_XML = b"<?xml version='1.0'?>\n<root>hello</root>"


def _write_gz(path):
    with gzip.open(path, "wb") as f:
        f.write(_XML)
    return path


class _GatedReader(io.BytesIO):
    """Stands in for ``rapidgzip.open``: the first read signals, then blocks."""

    def __init__(self, go, started):
        super().__init__(_XML)
        self._go = go
        self._read_once = False
        self._started = started

    def read(self, size=-1):
        if not self._read_once:
            self._read_once = True
            self._started.set()
            self._go.wait()
        return super().read(size)


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
        mp.setattr(
            decompress.rapidgzip,
            "open",
            lambda path, parallelization: _GatedReader(go, started),
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
        xml_path.write_bytes(_XML)
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

    def test_xml_appears_only_after_decompression(self, monkeypatch, tmp_path):
        xml_path = tmp_path / "test.xml"
        xml_seen_during_reads: list[bool] = []

        class RecordingReader(io.BytesIO):
            def read(self, size=-1):
                xml_seen_during_reads.append(xml_path.exists())
                return super().read(1)  # one byte per read: several reads

        monkeypatch.setattr(
            decompress.rapidgzip,
            "open",
            lambda path, parallelization: RecordingReader(_XML),
        )

        ensure_xml(tmp_path / "test.xml.gz", xml_path, workers=1).release(
            remove_xml=False
        )

        assert len(xml_seen_during_reads) > 1
        assert not any(xml_seen_during_reads)
        assert xml_path.read_bytes() == _XML


class TestConcurrentRuns:
    def test_run_starting_mid_decompression_gets_complete_xml(self, tmp_path):
        """Separate processes: CLI runs share nothing in memory, so neither may a test."""
        ctx = multiprocessing.get_context("spawn")
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
        assert not xml_path.exists()


@pytest.mark.integration
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

    def test_cached_xml_skipped(self, tmp_path):
        """ensure_xml skips decompression if .xml already exists."""
        gz_path = tmp_path / "test.xml.gz"
        gz_path.write_bytes(b"not a real gz")

        xml_path = tmp_path / "test.xml"
        xml_path.write_bytes(b"already here")

        ensure_xml(gz_path, xml_path, workers=1).release(remove_xml=False)

        assert xml_path.read_bytes() == b"already here"
