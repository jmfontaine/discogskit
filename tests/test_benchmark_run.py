"""Tests for benchmarks/run.py: the discogskit command it times, and how it reports failed runs."""

import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from benchmarks import run
from tests.conftest import ARTISTS_XML, require_or_skip, write_dump

_TIME_CMD = run._get_time_cmd()


def _require_gnu_time() -> None:
    """Skip the test without GNU time, or fail it when ``DISCOGSKIT_REQUIRE_GNU_TIME=1`` (set in CI)."""
    if shutil.which(_TIME_CMD[0]) is None:
        require_or_skip(
            f"GNU time ({_TIME_CMD[0]}) isn't installed", "DISCOGSKIT_REQUIRE_GNU_TIME"
        )


def _python_tool(code: str) -> run.ToolDef:
    """A tool whose run executes ``code`` with this interpreter."""
    return run.ToolDef(
        build_cmd=lambda input_path, output_dir: [sys.executable, "-c", code],
        name="stand-in",
    )


def test_discogskit_command_converts_a_dump(tmp_path: Path) -> None:
    dump = write_dump(
        tmp_path / "discogs_20260301_artists.xml.gz", "artists", ARTISTS_XML
    )
    output = tmp_path / "output"
    output.mkdir()

    result = subprocess.run(
        run.TOOLS["discogskit"].build_cmd(dump, output),
        capture_output=True,
        check=False,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert (output / "artists" / "artists.jsonl").stat().st_size > 0


def test_failed_run_shows_its_stderr_literally(
    capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    _require_gnu_time()
    tool = _python_tool(
        "import sys; sys.stderr.write('bad [bold]input[/bold]\\n'); sys.exit(3)"
    )

    result = run._run_once(tool, tmp_path / "in", tmp_path / "out", 1, _TIME_CMD)

    assert result is not None
    assert result.exit_code == 3
    assert "bad [bold]input[/bold]" in capsys.readouterr().out


def test_signal_killed_run_counts_as_failed(tmp_path: Path) -> None:
    _require_gnu_time()
    tool = _python_tool("import os, signal; os.kill(os.getpid(), signal.SIGKILL)")

    benchmark = run.run_benchmark(tool, tmp_path / "in", tmp_path, 1, _TIME_CMD)

    assert benchmark is not None
    assert benchmark.runs[0].exit_code != 0
    assert benchmark.stats == {}
