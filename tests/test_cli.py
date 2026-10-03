"""Tests for CLI error handling and option validation."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import click
import pytest
from typer.testing import CliRunner

from discogskit import pipeline
from discogskit.cli import app
from discogskit.decompress import DecompressError
from discogskit.writers import OutputExistsError

runner = CliRunner()

# A valid-looking .xml.gz filename so detect_entity() succeeds.
_ENTITY_FILE = "discogs_20260301_artists.xml.gz"


def _make_gz(tmp_path: Path) -> Path:
    """Create a dummy .xml.gz file so path validation passes."""
    gz = tmp_path / _ENTITY_FILE
    gz.write_bytes(b"not a real gz")
    return gz


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
    def test_rejected_before_any_work(self, tmp_path: Path, command: str) -> None:
        """`.xml` used to be accepted, then failed decompression (issue #15)."""
        gz = _make_gz(tmp_path)
        xml = tmp_path / "discogs_20260301_labels.xml"
        xml.write_bytes(b"<labels></labels>")
        with (
            patch("discogskit.cli.pipeline.run") as run,
            patch("discogskit.cli.get_writer"),
        ):
            result = runner.invoke(app, [command, str(gz), str(xml)])
        assert result.exit_code == 1
        output = " ".join(click.unstyle(result.output).split())
        assert "uncompressed .xml input is not supported" in output
        run.assert_not_called()
        assert xml.read_bytes() == b"<labels></labels>"


_COUNT_OPTIONS = {
    "convert": ["--chunk-mb", "--parse-workers", "--write-queue"],
    "load": [
        "--chunk-mb",
        "--index-workers",
        "--parse-workers",
        "--write-queue",
        "--write-workers",
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
        # Typer colors the error panel when it detects CI (GITHUB_ACTIONS).
        assert option in click.unstyle(result.output)
        run.assert_not_called()
