"""Tests for CLI error handling and option validation."""

from __future__ import annotations

import gzip
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import click
import pytest
from typer.testing import CliRunner

from discogskit import pipeline
from discogskit._console import console
from discogskit.cli import app
from discogskit.decompress import DecompressError
from discogskit.writers import OutputExistsError
from tests.conftest import ARTISTS_XML

runner = CliRunner()

# A valid-looking .xml.gz filename so detect_entity() succeeds.
_ENTITY_FILE = "discogs_20260301_artists.xml.gz"


def _make_gz(tmp_path: Path) -> Path:
    """Create a dummy .xml.gz file so path validation passes."""
    gz = tmp_path / _ENTITY_FILE
    gz.write_bytes(b"not a real gz")
    return gz


def _widen_console(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stop Rich wrapping a long path mid-name, so assertions can match it whole.

    The shared console is created at import time, so CliRunner's ``terminal_width`` and ``COLUMNS`` don't reach it.
    """
    monkeypatch.setattr(console, "_width", 1000)


_WRITER_CLOSE_FAILURES = [
    (KeyboardInterrupt(), 130, "Interrupted"),
    (OutputExistsError("x"), 1, "Error: x"),
    (
        DecompressError(Path("discogs_20260301_artists.xml.gz")),
        1,
        "Error: failed to decompress",
    ),
    (RuntimeError("x"), 1, "Error: x"),
]
_WRITER_CLOSE_FAILURE_IDS = [
    "KeyboardInterrupt",
    "OutputExistsError",
    "DecompressError",
    "RuntimeError",
]


class TestNoTracebacks:
    """Every unhandled exception from the pipeline must produce a clean error message."""

    @pytest.mark.parametrize(
        "exc",
        [
            RuntimeError("something broke"),
            TypeError("bad type"),
            ValueError("bad value"),
        ],
        ids=["RuntimeError", "TypeError", "ValueError"],
    )
    def test_convert_unexpected_exception(self, tmp_path: Path, exc: Exception) -> None:
        gz = _make_gz(tmp_path)
        with patch("discogskit.cli.pipeline.run", side_effect=exc):
            result = runner.invoke(app, ["convert", str(gz), "-f", "parquet"])
        assert result.exit_code != 0
        assert "Traceback" not in result.output

    @pytest.mark.parametrize(
        "exc",
        [
            RuntimeError("something broke"),
            TypeError("bad type"),
            ValueError("bad value"),
        ],
        ids=["RuntimeError", "TypeError", "ValueError"],
    )
    def test_load_unexpected_exception(self, tmp_path: Path, exc: Exception) -> None:
        gz = _make_gz(tmp_path)
        with (
            patch("discogskit.cli.pipeline.run", side_effect=exc),
            patch("discogskit.cli.get_writer"),
        ):
            result = runner.invoke(app, ["load", str(gz)])
        assert result.exit_code != 0
        assert "Traceback" not in result.output

    def test_convert_shows_error_message(self, tmp_path: Path) -> None:
        gz = _make_gz(tmp_path)
        with patch(
            "discogskit.cli.pipeline.run", side_effect=RuntimeError("disk full")
        ):
            result = runner.invoke(app, ["convert", str(gz), "-f", "parquet"])
        assert "disk full" in result.output

    def test_load_shows_error_message(self, tmp_path: Path) -> None:
        gz = _make_gz(tmp_path)
        with (
            patch("discogskit.cli.pipeline.run", side_effect=RuntimeError("disk full")),
            patch("discogskit.cli.get_writer"),
        ):
            result = runner.invoke(app, ["load", str(gz)])
        assert "disk full" in result.output


class TestWriterAlwaysClosed:
    """writer.close() must run on every exit path: success, user error, or crash."""

    @pytest.mark.parametrize(
        "exc, expected_exit_code, expected_message",
        _WRITER_CLOSE_FAILURES,
        ids=_WRITER_CLOSE_FAILURE_IDS,
    )
    def test_convert_closes_writer_once(
        self,
        tmp_path: Path,
        exc: BaseException,
        expected_exit_code: int,
        expected_message: str,
    ) -> None:
        gz = _make_gz(tmp_path)
        with (
            patch("discogskit.cli.pipeline.run", side_effect=exc),
            patch("discogskit.writers.parquet.ParquetWriter.close") as close,
        ):
            result = runner.invoke(app, ["convert", str(gz), "-f", "parquet"])
        assert result.exit_code == expected_exit_code
        assert expected_message in click.unstyle(result.output)
        close.assert_called_once()

    def test_convert_closes_writer_once_on_success(self, tmp_path: Path) -> None:
        gz = _make_gz(tmp_path)
        pipeline_result = pipeline.PipelineResult(None, 0.0, 0.0, 0.0, 0.0, 0)
        with (
            patch("discogskit.cli.pipeline.run", return_value=pipeline_result),
            patch("discogskit.writers.parquet.ParquetWriter.close") as close,
        ):
            result = runner.invoke(app, ["convert", str(gz), "-f", "parquet"])
        assert result.exit_code == 0
        assert "0 artists converted" in click.unstyle(result.output)
        close.assert_called_once()

    @pytest.mark.parametrize(
        "exc, expected_exit_code, expected_message",
        _WRITER_CLOSE_FAILURES,
        ids=_WRITER_CLOSE_FAILURE_IDS,
    )
    def test_load_closes_writer_once(
        self,
        tmp_path: Path,
        exc: BaseException,
        expected_exit_code: int,
        expected_message: str,
    ) -> None:
        gz = _make_gz(tmp_path)
        mock_writer = MagicMock()
        with (
            patch("discogskit.cli.pipeline.run", side_effect=exc),
            patch("discogskit.cli.get_writer", return_value=mock_writer),
        ):
            result = runner.invoke(app, ["load", str(gz)])
        assert result.exit_code == expected_exit_code
        assert expected_message in click.unstyle(result.output)
        mock_writer.close.assert_called_once()

    def test_load_closes_writer_once_on_success(self, tmp_path: Path) -> None:
        gz = _make_gz(tmp_path)
        mock_writer = MagicMock()
        pipeline_result = pipeline.PipelineResult(None, 0.0, 0.0, 0.0, 0.0, 0)
        with (
            patch("discogskit.cli.pipeline.run", return_value=pipeline_result),
            patch("discogskit.cli.get_writer", return_value=mock_writer),
        ):
            result = runner.invoke(app, ["load", str(gz)])
        assert result.exit_code == 0
        assert "0 artists loaded" in click.unstyle(result.output)
        mock_writer.close.assert_called_once()


class TestUncompressedXmlInput:
    @pytest.mark.parametrize("command", ["convert", "load"])
    def test_rejected_before_any_work(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, command: str
    ) -> None:
        """`.xml` used to be accepted, then failed decompression (issue #15)."""
        gz = _make_gz(tmp_path)
        xml = tmp_path / "discogs_20260301_labels.xml"
        xml.write_bytes(b"<labels></labels>")
        _widen_console(monkeypatch)
        with (
            patch("discogskit.cli.pipeline.run") as run,
            patch("discogskit.cli.get_writer"),
        ):
            result = runner.invoke(app, [command, str(gz), str(xml)])
        assert result.exit_code == 1
        assert "uncompressed .xml input is not supported" in click.unstyle(
            result.output
        )
        run.assert_not_called()
        assert xml.read_bytes() == b"<labels></labels>"


class TestUnrecognizedFileExtension:
    def test_directory_still_only_globs_xml_gz(self, tmp_path: Path) -> None:
        """Directory arguments keep globbing *.xml.gz; other files there are ignored."""
        gz = _make_gz(tmp_path)
        (tmp_path / "discogs_20260301_labels.xml.bz2").write_bytes(b"not xml")
        with patch("discogskit.cli.pipeline.run") as run:
            runner.invoke(app, ["convert", str(tmp_path), "-f", "parquet"])
        run.assert_called_once()
        [(config, _writer)] = [call.args for call in run.call_args_list]
        assert config.gz_path == gz

    @pytest.mark.parametrize("command", ["convert", "load"])
    def test_rejected_before_any_work(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, command: str
    ) -> None:
        """A typo'd or unsupported extension (e.g. .bz2) used to be silently skipped."""
        gz = _make_gz(tmp_path)
        # Spaces in the name: the message must reproduce the path exactly, not just unwrapped.
        other = tmp_path / "discogs  20260301 labels.xml.bz2"
        other.write_bytes(b"not xml")
        _widen_console(monkeypatch)
        with (
            patch("discogskit.cli.pipeline.run") as run,
            patch("discogskit.cli.get_writer"),
        ):
            result = runner.invoke(app, [command, str(gz), str(other)])
        assert result.exit_code == 1
        assert f"{other}: not a .xml.gz dump" in click.unstyle(result.output)
        run.assert_not_called()


class TestFormatAndCompressionEnums:
    def test_compression_default_for_jsonl_is_none(self, tmp_path: Path) -> None:
        gz = _make_gz(tmp_path)
        with (
            patch("discogskit.cli.pipeline.run"),
            patch("discogskit.writers.jsonl.JSONLWriter") as writer_cls,
        ):
            runner.invoke(app, ["convert", str(gz), "-f", "jsonl"])
        assert writer_cls.call_args.kwargs["compression"] == "none"

    def test_compression_default_for_parquet_is_zstd(self, tmp_path: Path) -> None:
        gz = _make_gz(tmp_path)
        with (
            patch("discogskit.cli.pipeline.run"),
            patch("discogskit.writers.parquet.ParquetWriter") as writer_cls,
        ):
            runner.invoke(app, ["convert", str(gz), "-f", "parquet"])
        assert writer_cls.call_args.kwargs["compression"] == "zstd"

    @pytest.mark.parametrize(
        "fmt, compression",
        [("jsonl", "snappy"), ("jsonl", "zstd"), ("parquet", "bzip2")],
        ids=["jsonl-snappy", "jsonl-zstd", "parquet-bzip2"],
    )
    def test_compression_not_valid_for_format_is_rejected(
        self, tmp_path: Path, fmt: str, compression: str
    ) -> None:
        gz = _make_gz(tmp_path)
        with patch("discogskit.cli.pipeline.run") as run:
            result = runner.invoke(
                app, ["convert", str(gz), "-f", fmt, "--compression", compression]
            )
        assert result.exit_code == 1
        output = click.unstyle(result.output)
        assert f"unsupported compression '{compression}' for {fmt}" in output
        run.assert_not_called()

    def test_compression_value_is_passed_through_to_writer(
        self, tmp_path: Path
    ) -> None:
        gz = _make_gz(tmp_path)
        with (
            patch("discogskit.cli.pipeline.run"),
            patch("discogskit.writers.parquet.ParquetWriter") as writer_cls,
        ):
            runner.invoke(
                app, ["convert", str(gz), "-f", "parquet", "--compression", "gzip"]
            )
        assert writer_cls.call_args.kwargs["compression"] == "gzip"

    def test_format_is_case_insensitive(self, tmp_path: Path) -> None:
        gz = _make_gz(tmp_path)
        with (
            patch("discogskit.cli.pipeline.run"),
            patch("discogskit.writers.parquet.ParquetWriter") as parquet_cls,
            patch("discogskit.writers.jsonl.JSONLWriter") as jsonl_cls,
        ):
            runner.invoke(app, ["convert", str(gz), "-f", "PARQUET"])
        parquet_cls.assert_called_once()
        jsonl_cls.assert_not_called()
        assert parquet_cls.call_args.kwargs["compression"] == "zstd"

    def test_invalid_compression_value_is_a_usage_error(self, tmp_path: Path) -> None:
        gz = _make_gz(tmp_path)
        with patch("discogskit.cli.pipeline.run") as run:
            result = runner.invoke(app, ["convert", str(gz), "--compression", "bogus"])
        assert result.exit_code == 2
        assert "'bogus'" in click.unstyle(result.output)
        run.assert_not_called()

    def test_invalid_format_is_a_usage_error(self, tmp_path: Path) -> None:
        gz = _make_gz(tmp_path)
        with patch("discogskit.cli.pipeline.run") as run:
            result = runner.invoke(app, ["convert", str(gz), "-f", "bogus"])
        assert result.exit_code == 2
        output = click.unstyle(result.output)
        assert "'bogus'" in output
        assert "'parquet'" in output
        assert "'jsonl'" in output
        run.assert_not_called()


_COUNT_OPTIONS = {
    "convert": ["--chunk-mb", "--parse-workers", "--write-queue"],
    "load": [
        "--chunk-mb",
        "--parse-workers",
        "--pg-index-workers",
        "--pg-write-workers",
        "--write-queue",
    ],
}


class TestOptionMinimums:
    @pytest.mark.parametrize(
        "command, option, value",
        [
            (command, option, value)
            for command, options in _COUNT_OPTIONS.items()
            for option in options
            for value in ("0", "-1")
        ],
    )
    def test_below_one_is_a_usage_error(
        self, tmp_path: Path, command: str, option: str, value: str
    ) -> None:
        """Rejected before any work: a negative --chunk-mb used to loop forever."""
        gz = _make_gz(tmp_path)
        with patch("discogskit.cli.pipeline.run") as run:
            result = runner.invoke(app, [command, str(gz), option, value])
        assert result.exit_code == 2
        output = click.unstyle(result.output)
        # Typer colors the error panel when it detects CI (GITHUB_ACTIONS).
        assert option in output
        assert "is not in the range x>=1" in output
        run.assert_not_called()

    @pytest.mark.parametrize(
        "option", ["--pg-fk", "--write-workers", "--index-workers"]
    )
    def test_renamed_options_are_no_such_option(
        self, tmp_path: Path, option: str
    ) -> None:
        """Clean cutover (#38): the old pre-rename flag names are gone, not aliased."""
        gz = _make_gz(tmp_path)
        with patch("discogskit.cli.pipeline.run") as run:
            result = runner.invoke(app, ["load", str(gz), option, "1"])
        assert result.exit_code == 2
        assert "No such option" in click.unstyle(result.output)
        run.assert_not_called()


class TestFkOption:
    def test_fk_enforced_in_sqlite(self, tmp_path: Path) -> None:
        """--fk (renamed from --pg-fk, #38) enforces foreign keys in SQLite too."""
        gz = tmp_path / "discogs_20260301_artists.xml.gz"
        with gzip.open(gz, "wb") as f:
            f.write(b"<?xml version='1.0' encoding='UTF-8'?>\n<artists>\n")
            f.write(ARTISTS_XML.encode())
            f.write(b"</artists>")

        db = tmp_path / "out.db"
        result = runner.invoke(
            app,
            [
                "load",
                str(gz),
                "--dsn",
                str(db),
                "--fk",
                "--parse-workers",
                "1",
                "--no-progress",
            ],
        )
        assert result.exit_code == 0, click.unstyle(result.output)

        conn = sqlite3.connect(db)
        fks = conn.execute("PRAGMA foreign_key_list(artist_aliases)").fetchall()
        conn.close()
        assert len(fks) > 0


class TestLoadRejectsPostgreSqlOnlyOptionsForSqlite:
    """get_writer()'s SQLite rejection must track the CLI's actual flag names, not copied literals."""

    def test_defaults_do_not_trigger_rejection(self, tmp_path: Path) -> None:
        gz = _make_gz(tmp_path)
        db = tmp_path / "out.db"
        pipeline_result = pipeline.PipelineResult(None, 0.0, 0.0, 0.0, 0.0, 0)
        with patch("discogskit.cli.pipeline.run", return_value=pipeline_result):
            result = runner.invoke(app, ["load", str(gz), "--dsn", str(db)])
        assert result.exit_code == 0, click.unstyle(result.output)

    @pytest.mark.parametrize(
        "flag, value",
        [
            ("--pg-create-schema", None),
            ("--pg-index-workers", "4"),
            ("--pg-schema", "custom"),
            ("--pg-unlogged", None),
            ("--pg-write-workers", "3"),
        ],
        ids=["create-schema", "index-workers", "schema", "unlogged", "write-workers"],
    )
    def test_rejects_each_postgresql_only_flag(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        flag: str,
        value: str | None,
    ) -> None:
        _widen_console(monkeypatch)
        gz = _make_gz(tmp_path)
        db = tmp_path / "out.db"
        args = ["load", str(gz), "--dsn", str(db), flag]
        if value is not None:
            args.append(value)
        if flag == "--pg-create-schema":
            # --pg-create-schema requires --pg-schema; add it so the rejection under test
            # (not the --pg-create-schema/--pg-schema pairing check) is the one that fires.
            args += ["--pg-schema", "custom"]
        pipeline_result = pipeline.PipelineResult(None, 0.0, 0.0, 0.0, 0.0, 0)
        with patch("discogskit.cli.pipeline.run", return_value=pipeline_result):
            result = runner.invoke(app, args)
        output = click.unstyle(result.output)
        assert result.exit_code == 1, output
        assert flag in output
        assert "can only be used with PostgreSQL, not a SQLite DSN." in output

    def test_unknown_flag_is_a_typer_error_not_a_rejection(
        self, tmp_path: Path
    ) -> None:
        """A made-up flag must fail via Typer's own parsing (exit 2), not look like our exit-1 rejection."""
        gz = _make_gz(tmp_path)
        db = tmp_path / "out.db"
        result = runner.invoke(
            app, ["load", str(gz), "--dsn", str(db), "--pg-bogus-option"]
        )
        output = click.unstyle(result.output)
        assert result.exit_code == 2, output
        assert "No such option" in output
