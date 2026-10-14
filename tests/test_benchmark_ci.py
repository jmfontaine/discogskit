"""Tests for benchmarks/ci.py: what a run prints and stores about its PostgreSQL DSN."""

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from benchmarks import ci

PASSWORD = "s3cr3t"
DSN = f"postgresql://postgres:{PASSWORD}@localhost:5432/discogskit"

_ENVIRONMENT = {
    "commit": "0" * 40,
    "cpu_count": 1,
    "cpu_model": "test",
    "kernel": "test",
    "memory_total_bytes": 1,
    "runner_image": None,
    "runner_image_version": None,
    "versions": {},
}
runner = CliRunner()


@pytest.fixture
def harness(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> list[str]:
    """Arguments for ``main`` on a one-file subset, with the cgroup check and target resets stubbed out."""
    monkeypatch.setattr(ci, "_check_cgroup", lambda cgroup: None)
    monkeypatch.setattr(ci, "_reset", lambda target, work, dsn: None)
    subset = tmp_path / "subset"
    subset.mkdir()
    (subset / "subset.json").write_text(
        json.dumps(
            {
                "dump_date": "20260301",
                "files": [
                    {
                        "entity": "artists",
                        "file": "artists.xml.gz",
                        "xml_sha256": "0" * 64,
                    }
                ],
                "records": 10,
            }
        )
    )
    cgroup = tmp_path / "cgroup"
    cgroup.mkdir()
    return [
        "--cgroup",
        str(cgroup),
        "--dsn",
        DSN,
        "--output",
        str(tmp_path / "output"),
        "--repeats",
        "1",
        "--subset",
        str(subset),
        "--targets",
        "postgresql",
        "--work",
        str(tmp_path / "work"),
    ]


def test_failed_run_does_not_print_the_password(
    monkeypatch: pytest.MonkeyPatch, harness: list[str]
) -> None:
    monkeypatch.setattr(ci.subprocess, "call", lambda *args, **kwargs: 1)
    result = runner.invoke(ci.app, harness)
    assert result.exit_code == 1
    assert "artists -> postgresql exited with 1" in result.output
    assert PASSWORD not in result.output


def test_results_do_not_store_the_password(
    monkeypatch: pytest.MonkeyPatch, harness: list[str], tmp_path: Path
) -> None:
    monkeypatch.setattr(
        ci,
        "measure",
        lambda *args: ci.Run(
            cpu_system_seconds=1.0,
            cpu_user_seconds=1.0,
            memory_peak_bytes=1,
            wall_seconds=1.0,
        ),
    )
    monkeypatch.setattr(ci, "_environment", lambda targets, dsn: _ENVIRONMENT)
    result = runner.invoke(ci.app, harness)
    assert result.exit_code == 0, result.output
    stored = (tmp_path / "output" / "results.json").read_text()
    assert json.loads(stored)["results"][0]["target"] == "postgresql"
    assert PASSWORD not in stored
    assert PASSWORD not in result.output
