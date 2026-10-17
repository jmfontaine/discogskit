"""Tests for pipeline utility functions."""

from __future__ import annotations

import dataclasses
import errno
import gzip
import json
import multiprocessing
import os
import signal
import sys
import threading
import time
import tracemalloc
from concurrent.futures.process import BrokenProcessPool
from multiprocessing.connection import Connection
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pytest
from filelock import ReadWriteLock
from rich.live import _RefreshThread

from discogskit._console import console
from discogskit.entities._def import ChunkArgs
from discogskit.entities._worker import ChunkResult, extract_chunk_to_ipc
from discogskit.pipeline import (
    _ChunkHeader,
    _DuplicateIdTracker,
    _ElapsedEstTotalColumn,
    _fmt_time,
    _kind,
    _parse_worker,
)
from tests.conftest import ARTISTS_XML, LABELS_XML

# ------------------------------------------------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------------------------------------------------

_ARTISTS_GZ_XML = (
    b"<?xml version='1.0' encoding='UTF-8'?>\n"
    b"<artists>\n"
    b"<artist>\n"
    b"  <id>1</id>\n"
    b"  <name>Test</name>\n"
    b"  <data_quality>Correct</data_quality>\n"
    b"</artist>\n"
    b"<artist>\n"
    b"  <id>2</id>\n"
    b"  <name>Other</name>\n"
    b"  <data_quality>Correct</data_quality>\n"
    b"</artist>\n"
    b"</artists>"
)


@pytest.fixture()
def artists_gz(tmp_path):
    """Create a small artists .xml.gz file."""
    gz_path = tmp_path / "artists.xml.gz"
    with gzip.open(gz_path, "wb") as f:
        f.write(_ARTISTS_GZ_XML)
    return gz_path


@pytest.fixture()
def duplicate_artists_gz(tmp_path):
    """Two <artist> elements with the same id, in the same chunk."""
    xml = (
        b"<?xml version='1.0' encoding='UTF-8'?>\n"
        b"<artists>\n"
        b"<artist>\n  <id>1</id>\n  <name>Test</name>\n"
        b"  <data_quality>Correct</data_quality>\n</artist>\n"
        b"<artist>\n  <id>1</id>\n  <name>Duplicate</name>\n"
        b"  <data_quality>Correct</data_quality>\n</artist>\n"
        b"</artists>"
    )
    gz_path = tmp_path / "artists.xml.gz"
    with gzip.open(gz_path, "wb") as f:
        f.write(xml)
    return gz_path


# Enough artists for several 1 MB chunks, so more chunks exist than can be in
# flight at once (parse_workers + write_queue).
_MANY_ARTISTS = 60_000


def _many_artists_gz(gz_path, bad_id_at=None, bad_close_at=None, n=_MANY_ARTISTS):
    """Write a multi-chunk artists .xml.gz of ``n`` artists; ``bad_id_at`` gets a
    non-numeric id, ``bad_close_at`` a mismatched ``</nam>`` closing tag."""
    body = b"".join(
        b"<artist>\n  <id>%s</id>\n  <name>Artist %d</%s>\n"
        b"  <data_quality>Correct</data_quality>\n</artist>\n"
        % (
            b"x" if i == bad_id_at else b"%d" % i,
            i,
            b"nam" if i == bad_close_at else b"name",
        )
        for i in range(1, n + 1)
    )
    # 1 MB chunks: need more than _multi_chunk_config's 2 workers + write_queue 2.
    assert len(body) > 4 * 1024 * 1024
    with gzip.open(gz_path, "wb", compresslevel=1) as f:
        f.write(b"<?xml version='1.0' encoding='UTF-8'?>\n<artists>\n")
        f.write(body)
        f.write(b"</artists>")
    return gz_path


def _multi_chunk_config(gz_path):
    from discogskit import pipeline

    return pipeline.PipelineConfig(
        chunk_mb=1,
        entity="artists",
        gz_path=gz_path,
        keep_xml=True,
        parse_workers=2,
        profile=False,
        progress=False,
        strict=False,
        write_queue=2,
    )


def _run_or_fail_on_hang(config, writer, timeout=30.0):
    """Run the pipeline on a helper thread so a hang fails the test, not the suite."""
    from discogskit import pipeline

    errors: list[BaseException] = []

    def target():
        try:
            pipeline.run(config, writer)
        # Hand any error to the test thread, which re-raises it.
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    thread = threading.Thread(daemon=True, target=target)
    thread.start()
    thread.join(timeout)
    if thread.is_alive():
        pytest.fail(f"pipeline.run did not return within {timeout}s")
    if errors:
        raise errors[0]


def _single_chunk_config(gz_path):
    from discogskit import pipeline

    return pipeline.PipelineConfig(
        chunk_mb=1,
        entity="artists",
        gz_path=gz_path,
        keep_xml=True,
        parse_workers=1,
        profile=False,
        progress=False,
        strict=False,
        write_queue=2,
    )


def _run_and_close(config, writer):
    """Run the pipeline the way the CLI does: ``close()`` always follows ``run()``."""
    from discogskit import pipeline

    try:
        return pipeline.run(config, writer)
    finally:
        writer.close()


def _marking_extract(args):
    """The real parse step, then one marker file per parsed chunk in ``<xml dir>/parsed``.

    Module-level so spawned parse workers can unpickle it.
    """
    result = extract_chunk_to_ipc(args)
    (Path(args.file_path).parent / "parsed" / str(args.start)).touch()
    return result


def _dying_once_extract(args):
    """The real parse step, except the first call SIGKILLs its own worker, as the OOM killer would."""
    try:
        (Path(args.file_path).parent / "worker-died").touch(exist_ok=False)
    except FileExistsError:
        return extract_chunk_to_ipc(args)
    os.kill(os.getpid(), signal.SIGKILL)
    raise AssertionError("SIGKILL returned")


def _after_first_write_extract(args):
    """The real parse step, except every call after the first waits for ``<xml dir>/write-started`` to exist."""
    try:
        (Path(args.file_path).parent / "first-parsed").touch(exist_ok=False)
    except FileExistsError:
        deadline = time.monotonic() + 10
        while not (Path(args.file_path).parent / "write-started").exists():
            assert time.monotonic() < deadline, "the first write never started"
            time.sleep(0.01)
    return extract_chunk_to_ipc(args)


# A result far bigger than a pipe's buffer, so the worker sends it in many writes.
_MID_SEND_BYTES = 64 * 1024 * 1024


def _dying_mid_send_extract(args):
    """The real parse step, except the first call returns a large result and SIGKILLs its own worker halfway through
    sending it, as the OOM killer would (#157)."""
    # Connection._send is private and undeclared in the stubs. Pipes and sockets alike send each part of a message
    # through it.
    connection = cast("Any", Connection)

    try:
        (Path(args.file_path).parent / "worker-died").touch(exist_ok=False)
    except FileExistsError:
        return extract_chunk_to_ipc(args)
    send = connection._send

    def send_half_then_die(self, buf, *rest):
        if (
            len(buf) >= _MID_SEND_BYTES
        ):  # the padding table, not a length header or a smaller table
            send(self, memoryview(buf)[: len(buf) // 2], *rest)
            os.kill(os.getpid(), signal.SIGKILL)
        send(self, buf, *rest)

    connection._send = send_half_then_die
    result = extract_chunk_to_ipc(args)
    return result._replace(
        tables={**result.tables, "padding": pa.py_buffer(bytes(_MID_SEND_BYTES))}
    )


def test_parse_worker_sends_tables_without_copying_them(tmp_path):
    """Pickling the result copied every table into one message before sending it, so the worker held its tables
    twice: 0.4-0.9 GiB more at a long-lived worker's peak at 256 MB chunks (issue #31)."""
    table = pa.py_buffer(bytes(32 * 1024 * 1024))
    tasks_r, tasks_w = multiprocessing.Pipe(duplex=False)
    results_r, results_w = multiprocessing.Pipe(duplex=False)
    tasks_w.send(ChunkArgs(end=1, entity="artists", file_path=str(tmp_path), start=0))
    tasks_w.close()
    # Drains the pipe into one preallocated buffer, so the reader allocates nothing tracemalloc would count.
    scratch = bytearray(1024 * 1024)
    received = []

    def drain():
        while n := os.readv(results_r.fileno(), [scratch]):
            received.append(n)

    reader = threading.Thread(target=drain)
    reader.start()
    sigint = signal.getsignal(signal.SIGINT)
    tracemalloc.start()
    try:
        _parse_worker(
            tasks_r, results_w, lambda _: ChunkResult(tables={"t": table}, unknown=[])
        )
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
        # The worker ignores Ctrl+C, which it leaves to the main process.
        signal.signal(signal.SIGINT, sigint)
        results_w.close()
        reader.join()

    assert sum(received) > table.size
    assert peak < table.size // 2


def test_parse_worker_ends_its_pipe_when_a_table_fails_to_send(tmp_path, monkeypatch):
    """Once a chunk's header is out, the main process reads the next messages as its tables: an error reply sent then
    would be taken for one, and the main process would wait for the rest forever."""
    tasks_r, tasks_w = multiprocessing.Pipe(duplex=False)
    results_r, results_w = multiprocessing.Pipe(duplex=False)
    for _ in range(2):
        tasks_w.send(
            ChunkArgs(end=1, entity="artists", file_path=str(tmp_path), start=0)
        )
    tasks_w.close()

    def fail(*_):
        raise MemoryError

    monkeypatch.setattr(Connection, "send_bytes", fail)
    sigint = signal.getsignal(signal.SIGINT)
    try:
        _parse_worker(
            tasks_r,
            results_w,
            lambda _: ChunkResult(tables={"t": pa.py_buffer(b"x")}, unknown=[]),
        )
    finally:
        signal.signal(signal.SIGINT, sigint)

    assert results_r.recv() == _ChunkHeader(tables=["t"], unknown=[])
    with pytest.raises(EOFError):
        results_r.recv_bytes()


class _StubWriter:
    """A writer whose ``write_chunk`` subclasses define; every other method does nothing."""

    def cancel(self):
        pass

    def close(self):
        pass

    def finalize(self, entity):
        pass

    def setup(self, entity):
        pass


# ------------------------------------------------------------------------------------------------------------------------
# Unit tests
# ------------------------------------------------------------------------------------------------------------------------


class TestFmtTime:
    @pytest.mark.parametrize(
        "seconds, expected",
        [
            (0, "0s"),
            (1, "1s"),
            (42, "42s"),
            (59, "59s"),
            (60, "1:00"),
            (61, "1:01"),
            (125, "2:05"),
            (3599, "59:59"),
            (3600, "60:00"),
        ],
    )
    def test_formatting(self, seconds, expected):
        assert _fmt_time(seconds) == expected

    def test_float_truncated(self):
        assert _fmt_time(42.9) == "42s"


class TestElapsedEstTotalColumn:
    @staticmethod
    def _task(*, elapsed, total, completed):
        """Build a real ``rich.progress.Task`` through the public API, clock advanced to ``elapsed``."""
        from rich.progress import Progress

        clock = [0.0]
        progress = Progress(get_time=lambda: clock[0], disable=True)
        progress.add_task("", total=total, completed=completed)
        clock[0] = elapsed
        return progress.tasks[0]

    def test_no_total_shows_elapsed_only(self):
        column = _ElapsedEstTotalColumn()
        task = self._task(elapsed=5.0, total=None, completed=0)
        assert str(column.render(task)) == "5s"

    def test_partial_progress_shows_estimate(self):
        column = _ElapsedEstTotalColumn()
        task = self._task(elapsed=10.0, total=20, completed=10)
        assert str(column.render(task)) == "10s/~20s"

    def test_total_reached_shows_elapsed_only(self):
        column = _ElapsedEstTotalColumn()
        task = self._task(elapsed=20.0, total=20, completed=20)
        assert str(column.render(task)) == "20s"


class TestPipelineRunWriters:
    """``pipeline.run`` calls ``write_chunk`` on the writer thread, unlike the
    per-writer tests, which drive every method from the test thread."""

    def test_jsonl(self, tmp_path, artists_gz):
        from discogskit.writers.jsonl import JSONLWriter

        result = _run_and_close(
            _single_chunk_config(artists_gz), JSONLWriter(str(tmp_path / "out"))
        )

        assert result.total_records == 2
        lines = (
            (tmp_path / "out" / "artists" / "artists.jsonl").read_text().splitlines()
        )
        assert [json.loads(line)["name"] for line in lines] == ["Test", "Other"]

    def test_parquet(self, tmp_path, artists_gz):
        import pyarrow.parquet as pq

        from discogskit.writers.parquet import ParquetWriter

        result = _run_and_close(
            _single_chunk_config(artists_gz), ParquetWriter(str(tmp_path / "out"))
        )

        assert result.total_records == 2
        table = pq.read_table(tmp_path / "out" / "artists" / "artists.parquet")
        assert table.column("name").to_pylist() == ["Test", "Other"]

    def test_sqlite(self, tmp_path, artists_gz):
        import sqlite3

        from discogskit.writers.sqlite import SQLiteWriter

        db_path = tmp_path / "out.db"
        result = _run_and_close(
            _single_chunk_config(artists_gz), SQLiteWriter(str(db_path))
        )

        assert result.total_records == 2
        conn = sqlite3.connect(db_path)
        rows = conn.execute("SELECT id, name FROM artists ORDER BY id").fetchall()
        conn.close()
        assert rows == [(1, "Test"), (2, "Other")]


class TestDuplicateIdTracker:
    """#25: duplicate detection must be correct for every int32 value, not just small positive ones."""

    _INT32_MAX = 2_147_483_647
    _INT32_MIN = -2_147_483_648

    @staticmethod
    def _ids(values):
        return pa.array(values, type=pa.int32())

    def test_distinct_ids_do_not_raise(self):
        tracker = _DuplicateIdTracker()
        tracker.check("artists", self._ids([1, 2, 3]))

    @pytest.mark.parametrize(
        "value",
        [0, -5, _INT32_MAX, _INT32_MIN],
        ids=["zero", "negative", "int32-max", "int32-min"],
    )
    def test_duplicate_raises(self, value):
        tracker = _DuplicateIdTracker()
        tracker.check("artists", self._ids([value]))
        with pytest.raises(ValueError, match=f"Duplicate artists id: {value}"):
            tracker.check("artists", self._ids([value]))

    def test_far_apart_ids_do_not_collide(self):
        """Ids far enough apart to land on different pages must not be mistaken for duplicates."""
        tracker = _DuplicateIdTracker()
        far_apart = [1, 2_000_000_000, -2_000_000_000, self._INT32_MAX, self._INT32_MIN]
        tracker.check("artists", self._ids(far_apart))

        for value in far_apart:
            with pytest.raises(ValueError, match=f"Duplicate artists id: {value}"):
                tracker.check("artists", self._ids([value]))

    def test_missing_id_raises(self):
        tracker = _DuplicateIdTracker()
        with pytest.raises(ValueError, match="Missing artists id"):
            tracker.check("artists", self._ids([None]))


class TestPipelineRunDuplicateIds:
    """#25: a duplicated root id must fail during the load, for every writer."""

    def test_jsonl(self, tmp_path, duplicate_artists_gz):
        from discogskit.writers.jsonl import JSONLWriter

        with pytest.raises(ValueError, match="Duplicate artists id: 1"):
            _run_and_close(
                _single_chunk_config(duplicate_artists_gz),
                JSONLWriter(str(tmp_path / "out")),
            )

    def test_parquet(self, tmp_path, duplicate_artists_gz):
        from discogskit.writers.parquet import ParquetWriter

        with pytest.raises(ValueError, match="Duplicate artists id: 1"):
            _run_and_close(
                _single_chunk_config(duplicate_artists_gz),
                ParquetWriter(str(tmp_path / "out")),
            )

    def test_sqlite(self, tmp_path, duplicate_artists_gz):
        from discogskit.writers.sqlite import SQLiteWriter

        db_path = tmp_path / "out.db"
        with pytest.raises(ValueError, match="Duplicate artists id: 1"):
            _run_and_close(
                _single_chunk_config(duplicate_artists_gz), SQLiteWriter(str(db_path))
            )

    def test_duplicate_across_chunks(self, tmp_path):
        """A duplicate id in a later chunk is still caught, not just within one chunk."""
        from discogskit.writers.sqlite import SQLiteWriter

        gz_path = _many_artists_gz(tmp_path / "artists.xml.gz")
        # Rewrite one far-later id to collide with the very first one.
        xml = gzip.decompress(gz_path.read_bytes())
        xml = xml.replace(f"<id>{_MANY_ARTISTS}</id>".encode(), b"<id>1</id>", 1)
        with gzip.open(gz_path, "wb", compresslevel=1) as f:
            f.write(xml)

        db_path = tmp_path / "out.db"
        writer = SQLiteWriter(str(db_path))
        try:
            with pytest.raises(ValueError, match="Duplicate artists id: 1"):
                _run_or_fail_on_hang(_multi_chunk_config(gz_path), writer)
        finally:
            writer.close()


class TestPipelineRunInputValidation:
    def test_input_without_records_keeps_existing_tables(self, tmp_path, artists_gz):
        """An input with no records fails before ``--overwrite`` drops existing tables."""
        import sqlite3

        from discogskit.writers.sqlite import SQLiteWriter

        db_path = tmp_path / "out.db"
        _run_and_close(_single_chunk_config(artists_gz), SQLiteWriter(str(db_path)))

        empty_gz = tmp_path / "empty" / "artists.xml.gz"
        empty_gz.parent.mkdir()
        with gzip.open(empty_gz, "wb") as f:
            f.write(b"<?xml version='1.0' encoding='UTF-8'?>\n<artists>\n</artists>")

        with pytest.raises(ValueError, match="No <artist> elements found"):
            _run_and_close(
                _single_chunk_config(empty_gz),
                SQLiteWriter(str(db_path), overwrite=True),
            )

        conn = sqlite3.connect(db_path)
        rows = conn.execute("SELECT id, name FROM artists ORDER BY id").fetchall()
        conn.close()
        assert rows == [(1, "Test"), (2, "Other")]

    @pytest.mark.parametrize(
        "option, value, message",
        [
            ("chunk_mb", 0, "chunk_mb must be at least 1, got 0"),
            ("chunk_mb", -1, "chunk_mb must be at least 1, got -1"),
            ("write_queue", -1, "write_queue must be at least 0, got -1"),
        ],
    )
    def test_invalid_option_fails_before_touching_anything(
        self, tmp_path, artists_gz, option, value, message
    ):
        """A run that would split nothing, or parse nothing and report an empty load, fails before decompressing the
        dump or replacing existing output (#213)."""
        from discogskit.writers import get_file_writer

        out = tmp_path / "out"
        config = dataclasses.replace(_single_chunk_config(artists_gz), keep_xml=False)
        _run_and_close(config, get_file_writer(out, "jsonl"))
        before = {p: p.read_bytes() for p in out.rglob("*") if p.is_file()}
        assert before

        with pytest.raises(ValueError, match=f"^{message}$"):
            _run_and_close(
                dataclasses.replace(config, **{option: value}),
                get_file_writer(out, "jsonl", overwrite=True),
            )

        assert {p: p.read_bytes() for p in out.rglob("*") if p.is_file()} == before
        assert not artists_gz.with_suffix("").exists()

    @pytest.mark.parametrize("parse_workers", [0, -1])
    def test_no_parse_workers_fails_before_touching_anything(
        self, tmp_path, artists_gz, parse_workers
    ):
        """Without a parse worker nothing would be parsed: the run must fail, not report an empty load."""

        class SetupTrackingWriter(_StubWriter):
            set_up = False

            def setup(self, entity):
                self.set_up = True

        config = dataclasses.replace(
            _single_chunk_config(artists_gz), parse_workers=parse_workers
        )
        writer = SetupTrackingWriter()

        with pytest.raises(ValueError, match="parse_workers must be at least 1"):
            _run_and_close(config, writer)

        assert not writer.set_up
        assert not artists_gz.with_suffix("").exists()


class TestPipelineRunXmlCleanup:
    @pytest.mark.parametrize("error", [KeyboardInterrupt, RuntimeError])
    def test_failed_run_keeps_xml(self, tmp_path, artists_gz, error):
        """Ctrl+C or any other failure keeps the XML for the next run to reuse."""

        class FailingWriter:
            def close(self):
                pass

            def finalize(self, entity):
                pass

            def setup(self, entity):
                pass

            def write_chunk(self, tables):
                raise error

        config = dataclasses.replace(_single_chunk_config(artists_gz), keep_xml=False)

        with pytest.raises(error):
            _run_and_close(config, FailingWriter())

        assert artists_gz.with_suffix("").read_bytes() == _ARTISTS_GZ_XML


# ------------------------------------------------------------------------------------------------------------------------
# Full pipeline.run() — PostgreSQL cases marked integration, the rest run as unit tests
# ------------------------------------------------------------------------------------------------------------------------


class TestPipelineRun:
    def test_end_to_end_no_progress(self, tmp_path, artists_gz):
        """Full pipeline with progress=False (verbose per-chunk output)."""
        from discogskit import pipeline
        from discogskit.writers.jsonl import JSONLWriter

        out_dir = tmp_path / "out"
        writer = JSONLWriter(str(out_dir))

        config = pipeline.PipelineConfig(
            chunk_mb=1,
            entity="artists",
            gz_path=artists_gz,
            keep_xml=False,
            parse_workers=1,
            profile=False,
            progress=False,
            strict=False,
            write_queue=2,
        )

        try:
            result = pipeline.run(config, writer)
        finally:
            writer.close()

        assert result.total_records == 2
        assert result.t_total > 0
        assert result.profile_data is None

        # Verify data actually reached the output
        artists_jsonl = out_dir / "artists" / "artists.jsonl"
        lines = artists_jsonl.read_text().strip().split("\n")
        assert len(lines) == 2
        assert json.loads(lines[0])["name"] == "Test"

        # XML should be cleaned up
        xml_path = artists_gz.with_suffix("")
        assert not xml_path.exists()

    def test_end_to_end_with_progress(self, monkeypatch, tmp_path, artists_gz):
        """Full pipeline with progress=True (Rich progress bar)."""
        from discogskit import pipeline
        from discogskit.writers.jsonl import JSONLWriter

        # The progress bar shows only on a terminal.
        monkeypatch.setattr(console, "_force_terminal", True)

        out_dir = tmp_path / "out"
        writer = JSONLWriter(str(out_dir))

        config = pipeline.PipelineConfig(
            chunk_mb=1,
            entity="artists",
            gz_path=artists_gz,
            keep_xml=False,
            parse_workers=1,
            profile=False,
            progress=True,
            strict=False,
            write_queue=2,
        )

        try:
            result = pipeline.run(config, writer)
        finally:
            writer.close()

        assert result.total_records == 2

    def test_end_to_end_with_profile(self, tmp_path):
        """Full pipeline with profile=True collects timing data, accumulated across chunks."""
        from discogskit import pipeline
        from discogskit.entities import ENTITIES
        from discogskit.writers.jsonl import JSONLWriter

        gz_path = _many_artists_gz(tmp_path / "artists.xml.gz")
        out_dir = tmp_path / "out"
        writer = JSONLWriter(str(out_dir))

        config = dataclasses.replace(_multi_chunk_config(gz_path), profile=True)

        try:
            result = pipeline.run(config, writer)
        finally:
            writer.close()

        assert result.total_records == _MANY_ARTISTS
        assert result.profile_data is not None
        assert isinstance(result.profile_data.put_blocked, float)
        assert isinstance(result.profile_data.get_wait, float)
        table_timings = result.profile_data.table_timings
        assert isinstance(table_timings, dict)
        assert set(table_timings) == set(ENTITIES["artists"].table_order)
        assert all(v >= 0 for v in table_timings.values())

        # keep_xml=True should preserve the XML
        xml_path = gz_path.with_suffix("")
        assert xml_path.exists()

    def test_profile_table_timings_accumulate_not_overwrite(self, tmp_path):
        """#35 regression: a chunk's timing must be summed into table_timings, not overwrite the running total."""

        class CountingWriter:
            def __init__(self):
                self.calls = 0

            def close(self):
                pass

            def finalize(self, entity):
                pass

            def setup(self, entity):
                pass

            def write_chunk(self, tables):
                self.calls += 1
                return {"artists": 1.0}

        gz_path = _many_artists_gz(tmp_path / "artists.xml.gz")
        writer = CountingWriter()
        config = dataclasses.replace(_multi_chunk_config(gz_path), profile=True)

        result = _run_and_close(config, writer)

        assert writer.calls > 1
        assert result.profile_data is not None
        table_timings = result.profile_data.table_timings
        assert isinstance(table_timings, dict)
        assert table_timings == {"artists": float(writer.calls)}

    def test_parse_error_stops_writer_before_close(self, tmp_path):
        """A worker parse error surfaces as-is and leaves the writer idle for close()."""

        class SlowTrackingWriter:
            """Writer slower than the parser, so writes are queued when parsing fails."""

            def __init__(self):
                self.active_at_close = 0
                self.active_writes = 0
                self.closed = False
                self.writes_after_close = 0

            def close(self):
                self.active_at_close = self.active_writes
                self.closed = True

            def finalize(self, entity):
                pass

            def setup(self, entity):
                pass

            def write_chunk(self, tables):
                if self.closed:
                    self.writes_after_close += 1
                self.active_writes += 1
                time.sleep(0.2)
                self.active_writes -= 1
                return {}

        gz_path = _many_artists_gz(tmp_path / "artists.xml.gz", bad_id_at=_MANY_ARTISTS)
        writer = SlowTrackingWriter()

        with pytest.raises(ValueError, match="invalid id .*, expected an integer"):
            try:
                _run_or_fail_on_hang(_multi_chunk_config(gz_path), writer)
            finally:
                writer.close()  # mirrors the CLI's `finally: writer.close()`

        # A regressed writer is mid-write at close(); give it time to start another.
        time.sleep(0.5)
        assert writer.active_at_close == 0
        assert writer.writes_after_close == 0

    def test_malformed_xml_error_reaches_caller(self, tmp_path):
        """lxml's XMLSyntaxError can't be pickled out of a worker; the error and its location must still arrive (#60)."""
        import re

        from discogskit.writers.jsonl import JSONLWriter

        bad = _MANY_ARTISTS - 10
        gz_path = _many_artists_gz(tmp_path / "artists.xml.gz", bad_close_at=bad)
        writer = JSONLWriter(str(tmp_path / "out"))

        with pytest.raises(ValueError, match="Opening and ending tag mismatch") as info:
            try:
                _run_or_fail_on_hang(_multi_chunk_config(gz_path), writer)
            finally:
                writer.close()

        # The reported chunk and line point at the malformed record in the XML.
        message = str(info.value)
        span = re.search(r"bytes (\d+)-(\d+)", message)
        position = re.search(r", line (\d+), column", message)
        assert span is not None and position is not None, message
        start, end = int(span.group(1)), int(span.group(2))
        line = int(position.group(1))
        chunk = gz_path.with_suffix("").read_bytes()[start:end]
        assert chunk.split(b"\n")[line - 1] == b"  <name>Artist %d</nam>" % bad

    @pytest.mark.integration
    def test_postgresql_writer(self, artists_gz, pg_dsn):
        """``pipeline.run`` with PostgreSQLWriter, whose chunks land on the writer thread."""
        import psycopg

        from discogskit.writers.postgresql import PostgreSQLWriter

        result = _run_and_close(
            _single_chunk_config(artists_gz), PostgreSQLWriter(pg_dsn, overwrite=True)
        )

        assert result.total_records == 2
        with psycopg.connect(pg_dsn) as conn:
            rows = conn.execute("SELECT id, name FROM artists ORDER BY id").fetchall()
        assert rows == [(1, "Test"), (2, "Other")]

    @pytest.mark.integration
    def test_postgresql_writer_no_timing_carry_over_between_entities(
        self, tmp_path, artists_gz, pg_dsn
    ):
        """#35 regression: a second entity's --profile must not include the first entity's tables."""
        from discogskit import pipeline
        from discogskit.entities import ENTITIES
        from discogskit.writers.postgresql import PostgreSQLWriter

        labels_xml = (
            b"<?xml version='1.0' encoding='UTF-8'?>\n"
            b"<labels>\n"
            b"<label>\n  <id>100</id>\n  <name>Test Label</name>\n"
            b"  <data_quality>Correct</data_quality>\n</label>\n"
            b"</labels>"
        )
        labels_gz = tmp_path / "labels.xml.gz"
        with gzip.open(labels_gz, "wb") as f:
            f.write(labels_xml)

        writer = PostgreSQLWriter(pg_dsn, overwrite=True, write_workers=2)
        try:
            artists_config = dataclasses.replace(
                _single_chunk_config(artists_gz), profile=True
            )
            pipeline.run(artists_config, writer)

            labels_config = dataclasses.replace(
                _single_chunk_config(labels_gz), entity="labels", profile=True
            )
            result = pipeline.run(labels_config, writer)
        finally:
            writer.close()

        assert result.profile_data is not None
        table_timings = result.profile_data.table_timings
        assert isinstance(table_timings, dict)
        assert set(table_timings) == set(ENTITIES["labels"].table_order) | {"_commit"}

    @pytest.mark.integration
    def test_postgresql_writer_duplicate_id(self, duplicate_artists_gz, pg_dsn):
        """#25: PostgreSQL catches a duplicate root id during the load, not just at finalize()."""
        from discogskit.writers.postgresql import PostgreSQLWriter

        with pytest.raises(ValueError, match="Duplicate artists id: 1"):
            _run_and_close(
                _single_chunk_config(duplicate_artists_gz),
                PostgreSQLWriter(pg_dsn, overwrite=True),
            )

    @pytest.mark.parametrize("fmt", ["jsonl", "parquet", "sqlite"])
    def test_two_entities_share_one_writer(self, tmp_path, fmt):
        """The CLI loads every input through one writer; per-entity state must not carry over."""
        import sqlite3

        import pyarrow.parquet as pq

        from discogskit import pipeline
        from discogskit.writers.jsonl import JSONLWriter
        from discogskit.writers.parquet import ParquetWriter
        from discogskit.writers.sqlite import SQLiteWriter

        out = tmp_path / "out"
        writer = {
            "jsonl": lambda: JSONLWriter(str(out)),
            "parquet": lambda: ParquetWriter(str(out)),
            "sqlite": lambda: SQLiteWriter(str(tmp_path / "out.db")),
        }[fmt]()
        try:
            for entity, records in (("artists", ARTISTS_XML), ("labels", LABELS_XML)):
                gz_path = tmp_path / f"{entity}.xml.gz"
                with gzip.open(gz_path, "wt", encoding="utf-8") as f:
                    f.write(
                        f"<?xml version='1.0' encoding='UTF-8'?>\n<{entity}>\n{records}</{entity}>"
                    )
                config = dataclasses.replace(
                    _single_chunk_config(gz_path), entity=entity
                )
                pipeline.run(config, writer)
        finally:
            writer.close()

        def rows(entity, table, *columns):
            if fmt == "jsonl":
                lines = (out / entity / f"{table}.jsonl").read_text().splitlines()
                return sorted(
                    tuple(json.loads(line)[c] for c in columns) for line in lines
                )
            if fmt == "parquet":
                t = pq.read_table(out / entity / f"{table}.parquet")
                return sorted(zip(*(t.column(c).to_pylist() for c in columns)))
            conn = sqlite3.connect(tmp_path / "out.db")
            try:
                return sorted(conn.execute(f"SELECT {', '.join(columns)} FROM {table}"))
            finally:
                conn.close()

        assert rows("artists", "artists", "id", "name") == [
            (1, "DJ Test"),
            (2, "Minimal Artist"),
        ]
        assert rows("artists", "artist_members", "artist_id", "member_id") == [
            (1, 30),
            (1, 31),
        ]
        assert rows("labels", "labels", "id", "name") == [
            (1, "Test Label"),
            (2, "Minimal Label"),
        ]
        assert rows("labels", "label_sublabels", "label_id", "sublabel_id") == [
            (1, 10),
            (1, 11),
        ]

    def test_writer_error_does_not_hang(self, monkeypatch, tmp_path):
        """A writer failing on chunk 1 while later chunks are queued behind it re-raises."""
        from discogskit import pipeline

        queued = threading.Event()
        submitted_writes = []

        class SignallingExecutor(pipeline.ThreadPoolExecutor):
            """Sets ``queued`` once a second chunk write is submitted behind the first."""

            def submit(self, fn, /, *args, **kwargs):
                future = super().submit(fn, *args, **kwargs)
                if args:  # a chunk write, not the pipeline's warm-up submit(int)
                    submitted_writes.append(future)
                    if len(submitted_writes) == 2:
                        queued.set()
                return future

        class FailingWriter(_StubWriter):
            def __init__(self):
                self.calls = 0
                self.saw_queued_write = False

            def write_chunk(self, tables):
                self.calls += 1
                self.saw_queued_write = queued.wait(timeout=10)
                raise RuntimeError("disk full")

        monkeypatch.setattr(pipeline, "ThreadPoolExecutor", SignallingExecutor)
        gz_path = _many_artists_gz(tmp_path / "artists.xml.gz")
        writer = FailingWriter()

        with pytest.raises(RuntimeError, match="disk full"):
            _run_or_fail_on_hang(_multi_chunk_config(gz_path), writer)

        assert writer.saw_queued_write
        # Chunks queued behind the failed write must not be written.
        assert writer.calls == 1


class TestStrictReport:
    def test_each_unhandled_element_reported_once_then_run_raises(
        self, capfd, tmp_path
    ):
        """With several workers each seeing the same unknown elements, each is reported once, not once per worker
        (#105), then the run raises before publishing anything (#179). capfd also captures the spawned workers' own
        output."""
        from discogskit import UnhandledElementsError
        from discogskit.writers.jsonl import JSONLWriter

        n = _MANY_ARTISTS
        gz_path = tmp_path / "artists.xml.gz"
        with gzip.open(gz_path, "wb", compresslevel=1) as f:
            f.write(b"<?xml version='1.0' encoding='UTF-8'?>\n<artists>\n")
            for i in range(1, n + 1):
                rare = b"<rare_tag/>" if i % 10_000 == 0 else b""
                f.write(
                    b"<artist>\n  <id>%d</id>\n  <name>Artist %d</name>\n"
                    b"  <new_tag/>%s\n</artist>\n" % (i, i, rare)
                )
            f.write(b"</artists>")
        config = dataclasses.replace(_multi_chunk_config(gz_path), strict=True)
        assert config.parse_workers >= 2

        with pytest.raises(UnhandledElementsError) as info:
            _run_and_close(config, JSONLWriter(str(tmp_path / "out")))

        assert info.value.entity == "artists"
        assert info.value.paths == ["new_tag", "rare_tag"]
        assert str(info.value) == (
            "--strict: 2 unhandled XML paths in <artist>; see the Strict lines above"
        )
        assert not (tmp_path / "out" / "artists").exists()
        out, err = capfd.readouterr()
        # Enough chunks that every worker parsed some.
        assert out.count("  chunk ") > config.parse_workers
        strict = [line.strip() for line in out.splitlines() if "Strict" in line]
        assert strict == [
            "Strict         unhandled XML element <new_tag> in <artist>",
            "Strict         unhandled XML element <rare_tag> in <artist>",
        ]
        assert "unhandled" not in err

    def test_one_unhandled_path_is_singular(self, tmp_path):
        from discogskit import UnhandledElementsError
        from discogskit.writers.jsonl import JSONLWriter

        gz_path = tmp_path / "artists.xml.gz"
        with gzip.open(gz_path, "wb") as f:
            f.write(
                b"<?xml version='1.0' encoding='UTF-8'?>\n<artists>\n"
                b"<artist><id>1</id><new_tag/></artist>\n</artists>"
            )
        config = dataclasses.replace(_single_chunk_config(gz_path), strict=True)

        with pytest.raises(
            UnhandledElementsError,
            match=r"^--strict: 1 unhandled XML path in <artist>; ",
        ):
            _run_and_close(config, JSONLWriter(str(tmp_path / "out")))

    @pytest.mark.parametrize(
        ("path", "kind"),
        [
            ("videos/video/foo", "element"),
            ("@foo", "attribute"),
            ("videos/video/@foo", "attribute"),
            ("urls/text()", "text"),
            # A namespace URI can hold "/" and "@".
            ("@{http://www.w3.org/XML/1998/namespace}lang", "attribute"),
            ("urls/{http://example.com/a@b}url", "element"),
        ],
    )
    def test_strict_line_names_what_the_path_is(self, path, kind):
        assert _kind(path) == kind


class TestPipelineRunParseWorkers:
    """#92: parse-ahead memory is bounded, and a dead worker, Ctrl+C or a failed start ends the run cleanly."""

    def test_parsed_but_unwritten_chunks_are_bounded(self, monkeypatch, tmp_path):
        """A writer slower than parsing must pause the workers, not let parsed chunks pile up in memory."""
        from discogskit import pipeline

        class StalledWriter(_StubWriter):
            """Stalls the first write so parsing can run ahead, then samples parsed-but-unwritten
            chunks at the end of each write, when the count peaks."""

            def __init__(self, marker_dir, bound):
                self.bound = bound
                self.marker_dir = marker_dir
                self.max_unwritten = 0
                self.written = 0

            def _parsed(self):
                return sum(1 for _ in self.marker_dir.iterdir())

            def write_chunk(self, tables):
                if self.written == 0:
                    # An unbounded pipeline parses past the bound within a few chunk parse times;
                    # a bounded one stalls at it until the deadline.
                    deadline = time.monotonic() + 2
                    while self._parsed() <= self.bound and time.monotonic() < deadline:
                        time.sleep(0.01)
                else:
                    time.sleep(0.05)
                self.max_unwritten = max(
                    self.max_unwritten, self._parsed() - self.written
                )
                self.written += 1
                return {}

        n = 150_000
        gz_path = _many_artists_gz(tmp_path / "artists.xml.gz", n=n)
        marker_dir = tmp_path / "parsed"
        marker_dir.mkdir()
        monkeypatch.setattr(pipeline, "extract_chunk_to_ipc", _marking_extract)
        config = dataclasses.replace(_multi_chunk_config(gz_path), write_queue=1)
        bound = config.parse_workers + config.write_queue
        writer = StalledWriter(marker_dir, bound)

        result = _run_and_close(config, writer)

        assert result.total_records == n
        # Enough chunks to overrun an unbounded pipeline.
        assert writer.written > 2 * bound
        assert 0 < writer.max_unwritten <= bound

    @pytest.mark.parametrize(
        "extract",
        [_dying_once_extract, _dying_mid_send_extract],
        ids=["while-parsing", "mid-send"],
    )
    def test_dead_worker_fails_the_run_and_releases_the_xml(
        self, monkeypatch, tmp_path, extract
    ):
        """A SIGKILLed worker loses its chunk; the run must fail promptly instead of waiting for it forever, even when
        the worker dies halfway through sending its result (#157)."""
        from discogskit import pipeline
        from discogskit.writers.jsonl import JSONLWriter

        gz_path = _many_artists_gz(tmp_path / "artists.xml.gz")
        monkeypatch.setattr(pipeline, "extract_chunk_to_ipc", extract)
        writer = JSONLWriter(str(tmp_path / "out"))

        with pytest.raises(BrokenProcessPool):
            try:
                _run_or_fail_on_hang(_multi_chunk_config(gz_path), writer, timeout=15)
            finally:
                writer.close()

        assert (tmp_path / "worker-died").exists()
        assert multiprocessing.active_children() == []
        xml_path = gz_path.with_suffix("")
        assert xml_path.exists()
        lock = ReadWriteLock(
            xml_path.with_name(xml_path.name + ".lock"), is_singleton=False
        )
        try:
            # Raises Timeout if the failed run still holds its read lease.
            lock.acquire_write(blocking=False)
            lock.release()
        finally:
            lock.close()

    @pytest.mark.parametrize("progress", [False, True], ids=["no-progress", "progress"])
    def test_sigint_waits_for_the_in_flight_write(
        self, monkeypatch, tmp_path, progress
    ):
        """Ctrl+C reaches the main thread mid-write: the write finishes, queued work is dropped, workers exit."""

        class InterruptingWriter(_StubWriter):
            """Like a database COPY, the first write can't be cancelled once it has started."""

            def __init__(self):
                self.active = 0
                self.calls = 0

            def write_chunk(self, tables):
                self.active += 1
                self.calls += 1
                if self.calls == 1:
                    os.kill(os.getpid(), signal.SIGINT)
                    time.sleep(0.5)
                self.active -= 1
                return {}

        # The progress bar shows only on a terminal.
        monkeypatch.setattr(console, "_force_terminal", progress)
        gz_path = _many_artists_gz(tmp_path / "artists.xml.gz")
        config = dataclasses.replace(
            _multi_chunk_config(gz_path), keep_xml=False, progress=progress
        )
        writer = InterruptingWriter()

        with pytest.raises(KeyboardInterrupt):
            _run_and_close(config, writer)

        assert writer.active == 0
        assert writer.calls == 1
        assert gz_path.with_suffix("").exists()
        assert multiprocessing.active_children() == []

    def test_sigint_cancels_the_in_flight_write(self, tmp_path):
        """Ctrl+C cancels a write that would otherwise block on, e.g., a stalled server (#110).

        The first cancel is lost, as on a server that gets it before the statement starts; the request repeats.
        """

        class BlockingWriter(_StubWriter):
            """The first write blocks until a second cancel(), then fails as a cancelled COPY does."""

            def __init__(self):
                self.calls = 0
                self.cancels = 0
                self.cancelled = threading.Event()

            def cancel(self):
                self.cancels += 1
                if self.cancels > 1:
                    self.cancelled.set()

            def write_chunk(self, tables):
                self.calls += 1
                os.kill(os.getpid(), signal.SIGINT)
                if not self.cancelled.wait(10):
                    raise AssertionError("write was never cancelled")
                raise RuntimeError("canceling statement due to user request")

        gz_path = _many_artists_gz(tmp_path / "artists.xml.gz")
        config = dataclasses.replace(_multi_chunk_config(gz_path), keep_xml=False)
        writer = BlockingWriter()

        t0 = time.monotonic()
        with pytest.raises(KeyboardInterrupt):
            _run_and_close(config, writer)
        elapsed = time.monotonic() - t0

        assert writer.cancelled.is_set()
        assert writer.calls == 1
        assert elapsed < 5
        assert multiprocessing.active_children() == []

    def test_sigint_during_write_submission_still_cancels_it(
        self, monkeypatch, tmp_path
    ):
        """Ctrl+C right after a write was handed to the writer thread, before its future was recorded (#110).

        The write starts only after the first cancel, which it therefore misses; the request must repeat.
        """
        from discogskit import pipeline

        class LateStartingWriter(_StubWriter):
            def __init__(self):
                self.calls = 0
                self.cancelled = threading.Event()
                self.first_cancel = threading.Event()
                self.started = threading.Event()

            def cancel(self):
                if self.started.is_set():
                    self.cancelled.set()
                else:
                    self.first_cancel.set()

            def write_chunk(self, tables):
                self.calls += 1
                assert self.first_cancel.wait(10)
                self.started.set()
                if not self.cancelled.wait(10):
                    raise AssertionError("write was never cancelled")
                raise RuntimeError("canceling statement due to user request")

        class InterruptingExecutor(pipeline.ThreadPoolExecutor):
            def submit(self, fn, /, *args, **kwargs):
                future = super().submit(fn, *args, **kwargs)
                if fn is not threading.current_thread:
                    # Interrupt only once the writer thread has taken the write: one still queued is dropped
                    # by the shutdown, and then there's nothing to cancel.
                    deadline = time.monotonic() + 10
                    while not future.running() and not future.done():
                        assert time.monotonic() < deadline
                        time.sleep(0.001)
                    raise KeyboardInterrupt
                return future

        monkeypatch.setattr(pipeline, "ThreadPoolExecutor", InterruptingExecutor)
        gz_path = _many_artists_gz(tmp_path / "artists.xml.gz")
        config = dataclasses.replace(_multi_chunk_config(gz_path), keep_xml=False)
        writer = LateStartingWriter()

        t0 = time.monotonic()
        with pytest.raises(KeyboardInterrupt):
            _run_and_close(config, writer)
        elapsed = time.monotonic() - t0

        assert writer.cancelled.is_set()
        assert writer.calls == 1
        assert elapsed < 5
        assert multiprocessing.active_children() == []

    def test_second_sigint_while_cancelling_still_waits_for_the_write(self, tmp_path):
        """A second Ctrl+C while the write is being cancelled mustn't let close() run during the write (#110).

        Before Python 3.13, a Ctrl+C that interrupts ``Thread.join()`` marks the thread as stopped while it runs.
        """

        class TwiceInterruptedWriter(_StubWriter):
            def __init__(self):
                self.active = 0
                self.active_at_close = None
                self.cancelled = threading.Event()
                self.cancels = 0

            def cancel(self):
                self.cancels += 1
                if self.cancels == 1:
                    # The second Ctrl+C lands while the pipeline waits for the write to stop.
                    threading.Timer(0.1, os.kill, (os.getpid(), signal.SIGINT)).start()
                else:
                    self.cancelled.set()

            def close(self):
                self.active_at_close = self.active

            def write_chunk(self, tables):
                self.active += 1
                try:
                    os.kill(os.getpid(), signal.SIGINT)
                    assert self.cancelled.wait(10)
                    # A cancelled statement takes a moment to fail.
                    time.sleep(0.2)
                finally:
                    self.active -= 1
                raise RuntimeError("canceling statement due to user request")

        gz_path = _many_artists_gz(tmp_path / "artists.xml.gz")
        config = dataclasses.replace(_multi_chunk_config(gz_path), keep_xml=False)
        writer = TwiceInterruptedWriter()

        with pytest.raises(KeyboardInterrupt):
            _run_and_close(config, writer)

        assert writer.cancels >= 2
        assert writer.active_at_close == 0

    def test_sigint_after_another_failure_cancels_the_in_flight_write(
        self, monkeypatch, tmp_path
    ):
        """Ctrl+C while the pipeline, failing on a parse error, waits for a blocked write (#212).

        The interrupt must still cancel that write, and close() follow it, not run during it.
        """
        from discogskit import pipeline

        class BlockingWriter(_StubWriter):
            """The first write blocks until cancel(), then fails as a cancelled COPY does."""

            def __init__(self, started):
                self.active = 0
                self.active_at_close = None
                self.cancelled = threading.Event()
                self.cancels = 0
                self.started = started

            def cancel(self):
                self.cancels += 1
                self.cancelled.set()

            def close(self):
                self.active_at_close = self.active

            def write_chunk(self, tables):
                self.active += 1
                try:
                    self.started.touch()
                    if not self.cancelled.wait(10):
                        raise AssertionError("write was never cancelled")
                    # A cancelled statement takes a moment to fail: close() must still wait for it.
                    time.sleep(0.2)
                finally:
                    self.active -= 1
                raise RuntimeError("canceling statement due to user request")

        class InterruptingExecutor(pipeline.ThreadPoolExecutor):
            """Sends SIGINT shortly after the failing pipeline drops the queued writes, while it waits for the
            in-flight one."""

            def shutdown(self, wait=True, *, cancel_futures=False):
                if cancel_futures:
                    threading.Timer(0.3, os.kill, (os.getpid(), signal.SIGINT)).start()
                super().shutdown(cancel_futures=cancel_futures, wait=wait)

        monkeypatch.setattr(pipeline, "ThreadPoolExecutor", InterruptingExecutor)
        monkeypatch.setattr(
            pipeline, "extract_chunk_to_ipc", _after_first_write_extract
        )
        # In the second chunk: the first one is being written when its parse fails.
        gz_path = _many_artists_gz(tmp_path / "artists.xml.gz", bad_id_at=15_000)
        # One worker parses the chunks in order.
        config = dataclasses.replace(_multi_chunk_config(gz_path), parse_workers=1)
        writer = BlockingWriter(tmp_path / "write-started")

        t0 = time.monotonic()
        with pytest.raises(KeyboardInterrupt) as excinfo:
            _run_and_close(config, writer)
        elapsed = time.monotonic() - t0

        assert isinstance(excinfo.value.__context__, ValueError)
        assert writer.cancels >= 1
        assert writer.active_at_close == 0
        assert elapsed < 5
        assert multiprocessing.active_children() == []
        assert not [
            thread.name
            for thread in threading.enumerate()
            if thread.name.startswith("discogskit-writer_")
        ]

    def test_worker_start_failure_restores_the_terminal(self, monkeypatch, tmp_path):
        """A parse worker that fails to start must not leave the progress display running, nor the workers already
        started."""
        from discogskit.writers.jsonl import JSONLWriter

        process_class = multiprocessing.get_context("spawn").Process
        start = process_class.start
        starts = []

        def fail_second_start(self):
            starts.append(self)
            if len(starts) == 2:
                raise OSError(errno.EAGAIN, "Resource temporarily unavailable")
            start(self)

        # Progress redirects stdout/stderr only on a terminal.
        monkeypatch.setattr(console, "_force_terminal", True)
        monkeypatch.setattr(process_class, "start", fail_second_start)
        stdout, stderr = sys.stdout, sys.stderr
        gz_path = _many_artists_gz(tmp_path / "artists.xml.gz")
        config = dataclasses.replace(_multi_chunk_config(gz_path), progress=True)

        with pytest.raises(OSError, match="Resource temporarily unavailable"):
            _run_and_close(config, JSONLWriter(str(tmp_path / "out")))

        assert len(starts) == 2
        assert multiprocessing.active_children() == []
        assert sys.stdout is stdout
        assert sys.stderr is stderr
        for thread in threading.enumerate():
            if isinstance(thread, _RefreshThread):
                thread.join(timeout=2)
                assert not thread.is_alive()


class TestPipelineRunConsole:
    """Library callers choose where ``run()`` prints, through the public API alone."""

    @staticmethod
    def _run(config, tmp_path, console):
        from discogskit import get_writer, run

        writer = get_writer(str(tmp_path / "out.db"))
        try:
            return run(config, writer, console=console)
        finally:
            writer.close()

    @pytest.mark.parametrize("progress", [False, True], ids=["no-progress", "progress"])
    def test_quiet_console_prints_nothing(self, capfd, tmp_path, artists_gz, progress):
        """capfd also captures the spawned workers' output and anything the progress display redirects."""
        from rich.console import Console

        config = dataclasses.replace(
            _single_chunk_config(artists_gz), progress=progress
        )

        result = self._run(
            config, tmp_path, Console(force_terminal=progress, quiet=True)
        )

        assert result.total_records == 2
        assert capfd.readouterr() == ("", "")

    def test_supplied_console_gets_all_output(self, capfd, tmp_path, artists_gz):
        from io import StringIO

        from rich.console import Console

        buffer = StringIO()

        result = self._run(
            _single_chunk_config(artists_gz),
            tmp_path,
            Console(file=buffer, force_terminal=False, width=200),
        )

        assert result.total_records == 2
        assert capfd.readouterr() == ("", "")
        labels = [line.split()[0] for line in buffer.getvalue().splitlines()]
        # Decompress and Chunks from the pipeline, chunk from the writer thread, Create and Index from the writer.
        assert labels == ["Decompress", "Chunks", "Create", "chunk", "Index"]
