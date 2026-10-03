"""Tests for pipeline utility functions."""

from __future__ import annotations

import dataclasses
import gzip
import json
import threading
import time

import pyarrow as pa
import pytest

from discogskit.pipeline import _DuplicateIdTracker, _ElapsedEstTotalColumn, _fmt_time

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


# Enough artists for several 1 MB chunks, so more chunks exist than the write
# backlog (write_queue + the in-flight write) can hold.
_MANY_ARTISTS = 60_000


def _many_artists_gz(gz_path, bad_id_at=None, bad_close_at=None):
    """Write a multi-chunk artists .xml.gz; ``bad_id_at`` gets a non-numeric id,
    ``bad_close_at`` a mismatched ``</nam>`` closing tag."""
    body = b"".join(
        b"<artist>\n  <id>%s</id>\n  <name>Artist %d</%s>\n"
        b"  <data_quality>Correct</data_quality>\n</artist>\n"
        % (
            b"x" if i == bad_id_at else b"%d" % i,
            i,
            b"nam" if i == bad_close_at else b"name",
        )
        for i in range(1, _MANY_ARTISTS + 1)
    )
    # 1 MB chunks: need more than write_queue (2) + 1 in-flight chunks.
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

    def test_end_to_end_with_progress(self, tmp_path, artists_gz):
        """Full pipeline with progress=True (Rich progress bar)."""
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
        from discogskit.entities import get as get_entity
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
        assert "put_blocked" in result.profile_data
        assert "get_wait" in result.profile_data
        table_timings = result.profile_data["table_timings"]
        assert isinstance(table_timings, dict)
        assert set(table_timings) == set(get_entity("artists").table_order)
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
        table_timings = result.profile_data["table_timings"]
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

        with pytest.raises(ValueError, match="invalid literal for int"):
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
        from discogskit.entities import get as get_entity
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
        table_timings = result.profile_data["table_timings"]
        assert isinstance(table_timings, dict)
        assert set(table_timings) == set(get_entity("labels").table_order) | {"_commit"}

    @pytest.mark.integration
    def test_postgresql_writer_duplicate_id(self, duplicate_artists_gz, pg_dsn):
        """#25: PostgreSQL catches a duplicate root id during the load, not just at finalize()."""
        from discogskit.writers.postgresql import PostgreSQLWriter

        with pytest.raises(ValueError, match="Duplicate artists id: 1"):
            _run_and_close(
                _single_chunk_config(duplicate_artists_gz),
                PostgreSQLWriter(pg_dsn, overwrite=True),
            )

    def test_writer_error_does_not_hang(self, tmp_path):
        """A writer failing on chunk 1 of more chunks than the backlog holds re-raises."""

        class FailingWriter:
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
                time.sleep(0.3)  # let later chunks queue behind this write
                raise RuntimeError("disk full")

        gz_path = _many_artists_gz(tmp_path / "artists.xml.gz")
        writer = FailingWriter()

        with pytest.raises(RuntimeError, match="disk full"):
            _run_or_fail_on_hang(_multi_chunk_config(gz_path), writer)

        # Chunks queued behind the failed write must not be written.
        assert writer.calls == 1
