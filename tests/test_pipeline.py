"""Tests for pipeline utility functions."""

from __future__ import annotations

import dataclasses
import gzip
import json
import threading
import time

import pytest

from discogskit.pipeline import _fmt_time

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


# Enough artists for several 1 MB chunks, so more chunks exist than the write
# backlog (write_queue + the in-flight write) can hold.
_MANY_ARTISTS = 60_000


def _many_artists_gz(gz_path, bad_id_at=None):
    """Write a multi-chunk artists .xml.gz; ``bad_id_at`` gets a non-numeric id."""
    body = b"".join(
        b"<artist>\n  <id>%s</id>\n  <name>Artist %d</name>\n"
        b"  <data_quality>Correct</data_quality>\n</artist>\n"
        % (b"x" if i == bad_id_at else b"%d" % i, i)
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
        with sqlite3.connect(db_path) as conn:
            rows = conn.execute("SELECT id, name FROM artists ORDER BY id").fetchall()
        assert rows == [(1, "Test"), (2, "Other")]


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

        with pytest.raises(ValueError, match="No b'<artist>' elements found"):
            _run_and_close(
                _single_chunk_config(empty_gz),
                SQLiteWriter(str(db_path), overwrite=True),
            )

        with sqlite3.connect(db_path) as conn:
            rows = conn.execute("SELECT id, name FROM artists ORDER BY id").fetchall()
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

            def write_chunk(self, ipc_dict, entity, table_timings=None):
                raise error

        config = dataclasses.replace(_single_chunk_config(artists_gz), keep_xml=False)

        with pytest.raises(error):
            _run_and_close(config, FailingWriter())

        assert artists_gz.with_suffix("").read_bytes() == _ARTISTS_GZ_XML


# ------------------------------------------------------------------------------------------------------------------------
# Integration tests — full pipeline.run()
# ------------------------------------------------------------------------------------------------------------------------


@pytest.mark.integration
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

    def test_end_to_end_with_profile(self, tmp_path, artists_gz):
        """Full pipeline with profile=True collects timing data."""
        from discogskit import pipeline
        from discogskit.writers.jsonl import JSONLWriter

        out_dir = tmp_path / "out"
        writer = JSONLWriter(str(out_dir))

        config = pipeline.PipelineConfig(
            chunk_mb=1,
            entity="artists",
            gz_path=artists_gz,
            keep_xml=True,
            parse_workers=1,
            profile=True,
            progress=False,
            strict=False,
            write_queue=2,
        )

        try:
            result = pipeline.run(config, writer)
        finally:
            writer.close()

        assert result.total_records == 2
        assert result.profile_data is not None
        assert "put_blocked" in result.profile_data
        assert "get_wait" in result.profile_data
        assert "table_timings" in result.profile_data

        # keep_xml=True should preserve the XML
        xml_path = artists_gz.with_suffix("")
        assert xml_path.exists()

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

            def write_chunk(self, ipc_dict, entity, table_timings=None):
                if self.closed:
                    self.writes_after_close += 1
                self.active_writes += 1
                time.sleep(0.2)
                self.active_writes -= 1
                return 0

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

            def write_chunk(self, ipc_dict, entity, table_timings=None):
                self.calls += 1
                time.sleep(0.3)  # let later chunks queue behind this write
                raise RuntimeError("disk full")

        gz_path = _many_artists_gz(tmp_path / "artists.xml.gz")
        writer = FailingWriter()

        with pytest.raises(RuntimeError, match="disk full"):
            _run_or_fail_on_hang(_multi_chunk_config(gz_path), writer)

        # Chunks queued behind the failed write must not be written.
        assert writer.calls == 1
