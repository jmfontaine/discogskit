"""Tests for CLI error handling and option validation."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import click
import pytest
from typer.testing import CliRunner

from discogskit.cli import app

runner = CliRunner()

# A valid-looking .xml.gz filename so detect_entity() succeeds.
_ENTITY_FILE = "discogs_20260301_artists.xml.gz"


def _make_gz(tmp_path: Path) -> Path:
    """Create a dummy .xml.gz file so path validation passes."""
    gz = tmp_path / _ENTITY_FILE
    gz.write_bytes(b"not a real gz")
    return gz


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
