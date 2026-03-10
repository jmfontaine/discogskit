"""Tests for CLI error handling — tracebacks must never leak to users."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

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
