#!/usr/bin/env python3
"""Benchmark discogskit on dump subsets, measuring each run in its own cgroup (Linux, cgroup v2).

Peak memory comes from the run cgroup's ``memory.peak``, which covers the whole process tree (``/usr/bin/time`` only
reports the largest single process), and CPU time from its ``cpu.stat``. The kernel charges a cgroup for the page cache
its processes fill, so ``memory.peak`` includes the XML and output pages they read and write, not just their heap.

A page stays charged to the cgroup that first read it, even after that cgroup is gone, so a later run would read the
dump and discogskit's libraries from cache without being charged for them. The page cache is dropped before every
run, through passwordless ``sudo``, so each run starts cold and repeats measure the same thing.

The harness must run inside ``<cgroup>/harness``, with ``<cgroup>`` delegated to its user and the memory controller
enabled in ``<cgroup>/cgroup.subtree_control``: each run then gets a sibling cgroup, ``<cgroup>/run``, that the
harness can create and move processes into without root. .github/workflows/benchmark.yml sets this up.
"""

from __future__ import annotations

import json
import os
import platform
import shutil
import sqlite3
import statistics
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from importlib.metadata import version
from pathlib import Path
from typing import Annotated

import psycopg
import typer

SCRIPT_DIR = Path(__file__).parent
TARGETS = ("jsonl", "parquet", "postgresql", "sqlite")
# Every package whose code a run executes, plus discogskit itself.
_PACKAGES = (
    "adbc-driver-postgresql",
    "discogskit",
    "lxml",
    "psycopg",
    "pyarrow",
    "rapidgzip",
)
# The wrapper moves itself into the run cgroup before exec'ing discogskit, so the cgroup is charged for the whole
# process tree from its first allocation.
_WRAPPER = 'echo $$ > "$0/cgroup.procs" && exec "$@"'


class BenchmarkError(Exception):
    """A precondition failed or a run didn't succeed."""


@dataclass(frozen=True)
class Run:
    """One measured run."""

    cpu_system_seconds: float
    cpu_user_seconds: float
    memory_peak_bytes: int
    wall_seconds: float


def _check_cgroup(cgroup: Path) -> None:
    """Fail early unless this process runs in ``<cgroup>/harness`` and ``<cgroup>`` can host a memory-limited run."""
    if not (cgroup / "cgroup.procs").is_file():
        raise BenchmarkError(f"{cgroup} is not a cgroup v2 directory")
    # cgroup v2 has a single hierarchy: /proc/self/cgroup is one "0::<path>" line.
    own = Path("/sys/fs/cgroup") / Path(
        "/proc/self/cgroup"
    ).read_text().strip().removeprefix("0::/")
    if own != cgroup / "harness":
        raise BenchmarkError(
            f"the harness runs in {own}, expected {cgroup / 'harness'}"
        )
    if "memory" not in (cgroup / "cgroup.subtree_control").read_text().split():
        raise BenchmarkError(
            f"the memory controller isn't enabled in {cgroup}/cgroup.subtree_control"
        )
    if not os.access(cgroup, os.W_OK) or not os.access(
        cgroup / "cgroup.procs", os.W_OK
    ):
        raise BenchmarkError(f"{cgroup} isn't delegated to this user")


def _cpu_stat(cgroup: Path) -> dict[str, int]:
    return {
        name: int(value)
        for name, value in (
            line.split() for line in (cgroup / "cpu.stat").read_text().splitlines()
        )
    }


def measure(command: list[str], cgroup: Path, log: Path) -> Run:
    """Run ``command`` in a fresh ``cgroup``, with its output in ``log``; raise BenchmarkError if it fails."""
    cgroup.mkdir()
    try:
        with log.open("wb") as output:
            start = time.perf_counter()
            returncode = subprocess.call(
                ["/bin/sh", "-c", _WRAPPER, str(cgroup), *command],
                stderr=subprocess.STDOUT,
                stdout=output,
            )
            wall = time.perf_counter() - start
        if returncode:
            tail = "\n".join(log.read_text(errors="replace").splitlines()[-40:])
            raise BenchmarkError(
                f"{' '.join(command)} exited with {returncode}:\n{tail}"
            )
        cpu = _cpu_stat(cgroup)
        return Run(
            cpu_system_seconds=cpu["system_usec"] / 1e6,
            cpu_user_seconds=cpu["user_usec"] / 1e6,
            memory_peak_bytes=int((cgroup / "memory.peak").read_text()),
            wall_seconds=wall,
        )
    finally:
        # Fails if a process outlived the run, which would also have skewed the next one.
        cgroup.rmdir()


def _command(target: str, path: Path, work: Path, dsn: str) -> list[str]:
    """The discogskit command benchmarked for ``target``, with default tuning and no progress bar."""
    # The venv's entry point directly, not `uv run`, which would add its own startup to every run.
    executable = str(Path(sys.executable).parent / "discogskit")
    if target in ("jsonl", "parquet"):
        return [
            executable,
            "convert",
            "--format",
            target,
            "--no-progress",
            "--output",
            str(work / target),
            str(path),
        ]
    database = dsn if target == "postgresql" else str(work / "discogs.db")
    return [executable, "load", "--dsn", database, "--no-progress", str(path)]


def _reset(target: str, work: Path, dsn: str) -> None:
    """Remove the previous run's output, outside the timed section."""
    shutil.rmtree(work / target, ignore_errors=True)
    (work / "discogs.db").unlink(missing_ok=True)
    (work / target).mkdir(parents=True)
    if target == "postgresql":
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute("DROP SCHEMA public CASCADE")
            conn.execute("CREATE SCHEMA public")
            # Flush the previous run's dirty buffers now rather than during the next run.
            conn.execute("CHECKPOINT")
    os.sync()
    # Every run starts cold, so the first one isn't the only one charged for reading the input and the libraries.
    subprocess.run(
        ["sudo", "-n", "tee", "/proc/sys/vm/drop_caches"],
        check=True,
        input=b"3\n",
        stdout=subprocess.DEVNULL,
    )


def _summary(values: list[float]) -> dict[str, float]:
    return {"max": max(values), "median": statistics.median(values), "min": min(values)}


def _cpu_model() -> str:
    for line in Path("/proc/cpuinfo").read_text().splitlines():
        if line.startswith("model name"):
            return line.split(":", 1)[1].strip()
    return platform.processor() or "unknown"


def _memory_total_bytes() -> int:
    for line in Path("/proc/meminfo").read_text().splitlines():
        if line.startswith("MemTotal:"):
            return int(line.split()[1]) * 1024
    raise BenchmarkError("MemTotal missing from /proc/meminfo")


def _environment(targets: list[str], dsn: str) -> dict[str, object]:
    versions: dict[str, str] = {name: version(name) for name in _PACKAGES}
    versions["python"] = platform.python_version()
    versions["sqlite"] = sqlite3.sqlite_version
    if "postgresql" in targets:
        with psycopg.connect(dsn) as conn:
            row = conn.execute("SELECT version()").fetchone()
            versions["postgresql"] = row[0] if row else "unknown"
    commit = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=SCRIPT_DIR, text=True
    ).strip()
    return {
        "commit": commit,
        "cpu_count": os.cpu_count(),
        "cpu_model": _cpu_model(),
        "kernel": platform.release(),
        "memory_total_bytes": _memory_total_bytes(),
        # GitHub-hosted runners set these; None elsewhere.
        "runner_image": os.environ.get("ImageOS"),
        "runner_image_version": os.environ.get("ImageVersion"),
        "versions": versions,
    }


def _seconds(value: float) -> str:
    minutes, seconds = divmod(value, 60)
    return f"{int(minutes)}:{seconds:05.2f}" if minutes else f"{seconds:.2f}s"


def _gib(value: float) -> str:
    return f"{value / 2**30:.2f} GiB"


def markdown(results: dict) -> str:
    """Render ``results`` as the run page summary."""
    environment = results["environment"]
    subset = results["subset"]
    lines = [
        "## Benchmark results",
        "",
        (
            f"Medians of {results['repeats']} runs, with the fastest and slowest. Runs from different jobs aren't "
            "comparable: shared runners vary, and so can their CPU model."
        ),
        "",
        "| Entity | Target | Wall | Wall min–max | CPU user + sys | memory.peak | memory.peak max |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for result in results["results"]:
        stats = result["stats"]
        wall = stats["wall_seconds"]
        lines.append(
            f"| {result['entity']} | {result['target']} | {_seconds(wall['median'])} "
            f"| {_seconds(wall['min'])}–{_seconds(wall['max'])} | {_seconds(stats['cpu_seconds']['median'])} "
            f"| {_gib(stats['memory_peak_bytes']['median'])} | {_gib(stats['memory_peak_bytes']['max'])} |"
        )
    lines += [
        "",
        (
            "memory.peak covers the whole process tree, including the page cache it fills; every run starts with "
            "the page cache dropped. It excludes the PostgreSQL server, which runs in its own container."
        ),
        "",
        f"- **Dump:** {subset['dump_date']}, first {subset['records']:,} records per entity",
    ]
    lines += [
        f"- **{file['file']}:** SHA-256 of the XML `{file['xml_sha256']}`"
        for file in subset["files"]
    ]
    lines += [
        (
            f"- **Runner:** {environment['runner_image']} {environment['runner_image_version']}, "
            f"{environment['cpu_model']}, {environment['cpu_count']} CPUs, {_gib(environment['memory_total_bytes'])}"
        ),
        f"- **Commit:** `{environment['commit']}`",
        "- **Versions:** "
        + ", ".join(
            f"{name} {value}" for name, value in sorted(environment["versions"].items())
        ),
        "",
    ]
    return "\n".join(lines)


app = typer.Typer(
    help="Benchmark discogskit on dump subsets, measuring each run in its own cgroup."
)


@app.command()
def main(
    cgroup: Annotated[
        Path,
        typer.Option(
            help="Delegated cgroup v2 directory; the harness runs in its harness/."
        ),
    ],
    dsn: Annotated[
        str,
        typer.Option(
            help="PostgreSQL DSN; its public schema is dropped before each run."
        ),
    ],
    output: Annotated[
        Path, typer.Option(help="Directory for results.json and the run logs.")
    ],
    repeats: Annotated[int, typer.Option(help="Runs per entity and target.", min=1)],
    subset: Annotated[Path, typer.Option(help="Directory from `subset.py fetch`.")],
    summary: Annotated[
        Path | None, typer.Option(help="File to append the Markdown summary to.")
    ] = None,
    targets: Annotated[
        str, typer.Option(help=f"Comma-separated targets among {', '.join(TARGETS)}.")
    ] = ",".join(TARGETS),
    work: Annotated[
        Path, typer.Option(help="Scratch directory for the runs' output.")
    ] = SCRIPT_DIR / "output",
) -> None:
    """Run every subset file against every target, REPEATS times, and write results.json."""
    names = [name.strip() for name in targets.split(",") if name.strip()]
    unknown = [name for name in names if name not in TARGETS]
    if unknown or not names:
        raise typer.BadParameter(
            f"expected a comma-separated list of {', '.join(TARGETS)}, got {targets!r}"
        )
    try:
        _check_cgroup(cgroup)
        metadata = json.loads((subset / "subset.json").read_text())
        logs = output / "logs"
        logs.mkdir(parents=True, exist_ok=True)
        runs: dict[tuple[str, str], list[Run]] = {}
        commands: dict[tuple[str, str], list[str]] = {}
        # Repeats are the outer loop, so drift during the job spreads over every configuration instead of
        # landing on whichever ran last.
        for repeat in range(1, repeats + 1):
            for file in metadata["files"]:
                for target in names:
                    key = (file["entity"], target)
                    commands[key] = _command(target, subset / file["file"], work, dsn)
                    _reset(target, work, dsn)
                    typer.echo(f"Run {repeat}/{repeats}: {file['entity']} -> {target}")
                    run = measure(
                        commands[key],
                        cgroup / "run",
                        logs / f"{file['entity']}-{target}-{repeat}.log",
                    )
                    typer.echo(
                        f"  {_seconds(run.wall_seconds)}, memory.peak {_gib(run.memory_peak_bytes)}"
                    )
                    runs.setdefault(key, []).append(run)
        results = {
            "created": datetime.now(timezone.utc).isoformat(),
            "environment": _environment(names, dsn),
            "repeats": repeats,
            "results": [
                {
                    "command": commands[key],
                    "entity": key[0],
                    "runs": [asdict(run) for run in key_runs],
                    "stats": {
                        "cpu_seconds": _summary(
                            [
                                r.cpu_user_seconds + r.cpu_system_seconds
                                for r in key_runs
                            ]
                        ),
                        "memory_peak_bytes": _summary(
                            [r.memory_peak_bytes for r in key_runs]
                        ),
                        "wall_seconds": _summary([r.wall_seconds for r in key_runs]),
                    },
                    "target": key[1],
                }
                for key, key_runs in runs.items()
            ],
            "subset": metadata,
        }
    except BenchmarkError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1) from None
    (output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    text = markdown(results)
    typer.echo(text)
    if summary is not None:
        with summary.open("a") as file:
            file.write(text)


if __name__ == "__main__":
    app()
