#!/usr/bin/env python3
"""Benchmark discogskit against alternative Discogs data processing tools."""

from __future__ import annotations

import json
import os
import platform
import shutil
import statistics
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated, Any

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()

SCRIPT_DIR = Path(__file__).parent
REPO_DIR = SCRIPT_DIR.parent
ALTERNATIVES_DIR = SCRIPT_DIR / "alternatives"
DEFAULT_RESULTS_DIR = SCRIPT_DIR / "results"


# ---------------------------------------------------------------------------
# GNU time helpers
# ---------------------------------------------------------------------------


def _get_time_cmd() -> list[str]:
    """Return the GNU time command for the current platform."""
    if sys.platform == "darwin":
        return ["gtime"]
    return ["/usr/bin/time"]


def _parse_wall_clock(wall_clock: str) -> float:
    """Parse wall clock time string to seconds.

    Formats: ``m:ss.xx``, ``h:mm:ss``, ``h:mm:ss.xx``
    """
    parts = wall_clock.split(":")
    if len(parts) == 2:
        minutes, seconds = parts
        return int(minutes) * 60 + float(seconds)
    if len(parts) == 3:
        hours, minutes, seconds = parts
        return int(hours) * 3600 + int(minutes) * 60 + float(seconds)
    return 0.0


def _parse_gtime_output(text: str) -> dict[str, Any]:
    """Parse GNU time verbose output into a dict of metrics."""
    stats: dict[str, Any] = {}
    for line in text.splitlines():
        line = line.strip()
        if "wall clock" in line:
            raw = line.rsplit(": ", 1)[1].strip()
            stats["wall_clock_seconds"] = _parse_wall_clock(raw)
        elif "Maximum resident" in line:
            kb = int(line.rsplit(": ", 1)[1].strip())
            stats["max_rss_mb"] = kb / 1024
        elif "User time" in line:
            stats["user_time_seconds"] = float(line.rsplit(": ", 1)[1].strip())
        elif "System time" in line:
            stats["system_time_seconds"] = float(line.rsplit(": ", 1)[1].strip())
        elif "Exit status" in line:
            stats["exit_code"] = int(line.rsplit(": ", 1)[1].strip())
    return stats


# ---------------------------------------------------------------------------
# System info
# ---------------------------------------------------------------------------


def _cpu_model() -> str:
    """Return the CPU model string."""
    if sys.platform == "darwin":
        try:
            return subprocess.check_output(
                ["sysctl", "-n", "machdep.cpu.brand_string"],
                text=True,
            ).strip()
        except (subprocess.CalledProcessError, FileNotFoundError):
            pass
    elif sys.platform == "linux":
        try:
            for line in Path("/proc/cpuinfo").read_text().splitlines():
                if line.startswith("model name"):
                    return line.split(":", 1)[1].strip()
        except OSError:
            pass
    return platform.processor() or "unknown"


def _total_ram_gb() -> float:
    """Return total system RAM in GB."""
    if sys.platform == "darwin":
        try:
            raw = subprocess.check_output(
                ["sysctl", "-n", "hw.memsize"], text=True
            ).strip()
            return int(raw) / (1024**3)
        except (subprocess.CalledProcessError, FileNotFoundError, ValueError):
            pass
    elif sys.platform == "linux":
        try:
            for line in Path("/proc/meminfo").read_text().splitlines():
                if line.startswith("MemTotal"):
                    kb = int(line.split()[1])
                    return kb / (1024**2)
        except OSError:
            pass
    return 0.0


def _collect_system_info() -> dict[str, Any]:
    """Collect system information for the results file."""
    return {
        "cpu_model": _cpu_model(),
        "cpu_count": os.cpu_count(),
        "total_ram_gb": round(_total_ram_gb(), 1),
        "os": platform.system(),
        "os_version": platform.release(),
        "python_version": platform.python_version(),
        "hostname": platform.node(),
    }


# ---------------------------------------------------------------------------
# Human-readable file size
# ---------------------------------------------------------------------------


def _human_size(size_bytes: int) -> str:
    """Format byte count as human-readable string."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(size_bytes) < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024  # type: ignore[assignment]
    return f"{size_bytes:.2f} PB"


# ---------------------------------------------------------------------------
# Tool registry
# ---------------------------------------------------------------------------


@dataclass
class ToolDef:
    """Definition of a benchmarkable tool."""

    name: str
    build_cmd: Any  # Callable[[Path, Path], list[str]]
    default_runs: int = 1
    setup: Any = None  # Callable[[], None] | None
    teardown: Any = None  # Callable[[], None] | None
    version_cmd: list[str] | None = None


def _discogskit_cmd(input_path: Path, output_dir: Path) -> list[str]:
    return [
        "uv",
        "run",
        "--directory",
        str(REPO_DIR),
        "discogskit",
        "convert",
        str(input_path),
        "-f",
        "jsonl",
        "-o",
        str(output_dir),
        "--no-progress",
    ]


def _xml2db_python_cmd(input_path: Path, output_dir: Path) -> list[str]:
    venv_python = ALTERNATIVES_DIR / "discogs-xml2db" / ".venv" / "bin" / "python"
    run_script = ALTERNATIVES_DIR / "discogs-xml2db" / "run.py"
    return [
        str(venv_python),
        str(run_script),
        "--output",
        str(output_dir),
        str(input_path),
    ]


TOOLS: dict[str, ToolDef] = {
    "discogskit": ToolDef(
        name="discogskit",
        build_cmd=_discogskit_cmd,
        default_runs=3,
        version_cmd=[
            "uv",
            "run",
            "--directory",
            str(REPO_DIR),
            "discogskit",
            "--version",
        ],
    ),
    "xml2db-python": ToolDef(
        name="xml2db-python",
        build_cmd=_xml2db_python_cmd,
        default_runs=1,
    ),
}


# ---------------------------------------------------------------------------
# Run result types
# ---------------------------------------------------------------------------


@dataclass
class RunResult:
    """Result of a single benchmark run."""

    run_number: int
    command: str
    wall_clock_seconds: float
    user_time_seconds: float
    system_time_seconds: float
    max_rss_mb: float
    exit_code: int


@dataclass
class Stats:
    """Aggregate statistics over multiple runs."""

    median: float
    stddev: float | None
    min: float
    max: float


@dataclass
class BenchmarkResult:
    """Aggregated result for a tool across all runs."""

    tool: str
    version: str
    runs: list[RunResult] = field(default_factory=list)
    stats: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------


def _compute_stats(values: list[float]) -> dict[str, Any]:
    """Compute median, stddev, min, max for a list of values."""
    return {
        "median": statistics.median(values),
        "stddev": statistics.stdev(values) if len(values) > 1 else None,
        "min": min(values),
        "max": max(values),
    }


def _get_version(tool: ToolDef) -> str:
    """Try to get the version string for a tool."""
    if not tool.version_cmd:
        return "unknown"
    try:
        return subprocess.check_output(
            tool.version_cmd, text=True, stderr=subprocess.STDOUT
        ).strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unknown"


def _is_available(tool: ToolDef) -> bool:
    """Check whether a tool is available to run."""
    if tool.name == "discogskit":
        return True
    if tool.name == "xml2db-python":
        venv = ALTERNATIVES_DIR / "discogs-xml2db" / ".venv" / "bin" / "python"
        return venv.exists()
    return False


def _run_once(
    tool: ToolDef,
    input_path: Path,
    output_dir: Path,
    run_number: int,
    time_cmd: list[str],
) -> RunResult | None:
    """Execute a single benchmark run using GNU time."""
    # Clean output directory
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)

    cmd = tool.build_cmd(input_path, output_dir)

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".txt", delete=False
    ) as time_file:
        time_output_path = Path(time_file.name)

    full_cmd = time_cmd + ["-v", "-o", str(time_output_path)] + cmd

    try:
        result = subprocess.run(full_cmd, capture_output=True, text=True)
    except FileNotFoundError:
        if sys.platform == "darwin":
            console.print(
                "[red]Error:[/] gtime not found. "
                "Install with: [cyan]brew install gnu-time[/]"
            )
        else:
            console.print(
                "[red]Error:[/] /usr/bin/time not found. "
                "Install with: [cyan]apt install time[/]"
            )
        return None

    # Read gtime output from file
    try:
        gtime_output = time_output_path.read_text()
    finally:
        time_output_path.unlink(missing_ok=True)

    if not gtime_output.strip():
        console.print(f"[red]Error:[/] no timing data captured for {tool.name}")
        return None

    stats = _parse_gtime_output(gtime_output)

    # If the tool itself failed, note it but still record the timing
    if result.returncode != 0:
        console.print(
            f"  [yellow]Warning:[/] {tool.name} run {run_number} "
            f"exited with code {result.returncode}"
        )

    return RunResult(
        run_number=run_number,
        command=" ".join(cmd),
        wall_clock_seconds=stats.get("wall_clock_seconds", 0.0),
        user_time_seconds=stats.get("user_time_seconds", 0.0),
        system_time_seconds=stats.get("system_time_seconds", 0.0),
        max_rss_mb=stats.get("max_rss_mb", 0.0),
        exit_code=stats.get("exit_code", result.returncode),
    )


def run_benchmark(
    tool: ToolDef,
    input_path: Path,
    output_base: Path,
    runs: int,
    time_cmd: list[str],
) -> BenchmarkResult | None:
    """Run a tool N times and aggregate results."""
    version = _get_version(tool)
    benchmark = BenchmarkResult(tool=tool.name, version=version)

    output_dir = output_base / tool.name

    # Run setup if defined
    if tool.setup:
        try:
            tool.setup()
        except Exception:
            console.print(f"[red]Error:[/] {tool.name} setup failed")
            return None

    try:
        for i in range(1, runs + 1):
            label = f"run {i}/{runs}" if runs > 1 else "run"
            with console.status(f"[bold green]{tool.name}[/] {label}..."):
                result = _run_once(tool, input_path, output_dir, i, time_cmd)
            if result is None:
                return None
            benchmark.runs.append(result)

            # Show per-run wall clock for multi-run benchmarks
            if runs > 1:
                console.print(
                    f"  {tool.name} run {i}: [cyan]{result.wall_clock_seconds:.2f}s[/]"
                )
    finally:
        if tool.teardown:
            tool.teardown()

    # Compute aggregate stats
    successful = [r for r in benchmark.runs if r.exit_code == 0]
    if successful:
        benchmark.stats = {
            "wall_clock": _compute_stats([r.wall_clock_seconds for r in successful]),
            "user_time": _compute_stats([r.user_time_seconds for r in successful]),
            "system_time": _compute_stats([r.system_time_seconds for r in successful]),
            "max_rss_mb": _compute_stats([r.max_rss_mb for r in successful]),
        }

    return benchmark


# ---------------------------------------------------------------------------
# Results output
# ---------------------------------------------------------------------------


def _format_time(seconds: float) -> str:
    """Format seconds as human-readable time."""
    if seconds < 60:
        return f"{seconds:.2f}s"
    minutes = int(seconds) // 60
    secs = seconds - minutes * 60
    return f"{minutes}m{secs:05.2f}s"


def _print_results_table(results: list[BenchmarkResult]) -> None:
    """Print a Rich table summarizing benchmark results."""
    # Find discogskit baseline
    baseline_median: float | None = None
    for r in results:
        if r.tool == "discogskit" and r.stats:
            baseline_median = r.stats["wall_clock"]["median"]
            break

    table = Table(show_header=True, header_style="bold")
    table.add_column("Tool")
    table.add_column("Wall Clock", justify="right")
    table.add_column("User", justify="right")
    table.add_column("System", justify="right")
    table.add_column("Max RSS (MB)", justify="right")
    if baseline_median:
        table.add_column("Ratio", justify="right")

    for r in results:
        if not r.stats:
            continue
        wc = r.stats["wall_clock"]
        wall_str = _format_time(wc["median"])
        if wc["stddev"] is not None:
            wall_str += f" [dim](\u00b1{wc['stddev']:.2f}s)[/]"

        row: list[str] = [
            r.tool,
            wall_str,
            _format_time(r.stats["user_time"]["median"]),
            _format_time(r.stats["system_time"]["median"]),
            f"{r.stats['max_rss_mb']['median']:.1f}",
        ]

        if baseline_median:
            if r.tool == "discogskit":
                row.append("[dim]baseline[/]")
            else:
                ratio = wc["median"] / baseline_median
                row.append(f"{ratio:.2f}x")

        table.add_row(*row)

    console.print()
    console.print(table)


def _save_results(
    results: list[BenchmarkResult],
    input_path: Path,
    results_dir: Path,
) -> Path:
    """Save benchmark results as JSON and return the file path."""
    results_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc)
    file_stem = input_path.stem.replace(".xml", "")
    filename = f"{timestamp.strftime('%Y%m%d_%H%M%S')}_{file_stem}.json"
    output_path = results_dir / filename

    file_size = input_path.stat().st_size

    data = {
        "timestamp": timestamp.isoformat(),
        "system_info": _collect_system_info(),
        "input_file": {
            "path": str(input_path.resolve()),
            "name": input_path.name,
            "size_bytes": file_size,
            "size_human": _human_size(file_size),
        },
        "results": [],
    }

    for r in results:
        entry: dict[str, Any] = {
            "tool": r.tool,
            "version": r.version,
            "runs_requested": len(r.runs),
            "runs": [
                {
                    "run_number": run.run_number,
                    "command": run.command,
                    "wall_clock_seconds": run.wall_clock_seconds,
                    "user_time_seconds": run.user_time_seconds,
                    "system_time_seconds": run.system_time_seconds,
                    "max_rss_mb": run.max_rss_mb,
                    "exit_code": run.exit_code,
                }
                for run in r.runs
            ],
            "stats": r.stats,
        }
        data["results"].append(entry)

    output_path.write_text(json.dumps(data, indent=2) + "\n")
    return output_path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

app = typer.Typer(help="Benchmark discogskit against alternatives.")


@app.command()
def main(
    input_file: Annotated[
        Path,
        typer.Option("--input", "-i", help="Path to Discogs dump file.", exists=True),
    ],
    runs: Annotated[
        int,
        typer.Option(help="Number of runs per tool (0 = use per-tool defaults)."),
    ] = 0,
    tools: Annotated[
        str,
        typer.Option(help="Comma-separated list of tools to benchmark (empty = all)."),
    ] = "",
    results_dir: Annotated[
        Path,
        typer.Option(help="Directory to save results JSON."),
    ] = DEFAULT_RESULTS_DIR,
) -> None:
    """Run benchmarks against alternative tools."""
    # Resolve tool list
    if tools:
        tool_names = [t.strip() for t in tools.split(",")]
        for name in tool_names:
            if name not in TOOLS:
                raise typer.BadParameter(
                    f"Unknown tool: {name}. Available: {', '.join(TOOLS)}"
                )
    else:
        tool_names = [name for name in TOOLS if _is_available(TOOLS[name])]

    if not tool_names:
        console.print("[red]Error:[/] no tools available to benchmark.")
        raise typer.Exit(1)

    # Check gtime is available
    time_cmd = _get_time_cmd()
    try:
        subprocess.run(
            time_cmd + ["--version"],
            capture_output=True,
            check=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        if sys.platform == "darwin":
            console.print(
                "[red]Error:[/] gtime not found. "
                "Install with: [cyan]brew install gnu-time[/]"
            )
        else:
            console.print(
                "[red]Error:[/] /usr/bin/time not found. "
                "Install with: [cyan]apt install time[/]"
            )
        raise typer.Exit(1) from None

    # Display header
    system_info = _collect_system_info()
    console.print(
        Panel(
            f"[bold]{input_file.name}[/]\n"
            f"Tools: {', '.join(tool_names)}\n"
            f"System: {system_info['cpu_model']} "
            f"({system_info['cpu_count']} cores, "
            f"{system_info['total_ram_gb']:.0f} GB RAM)",
        )
    )

    # Create output base directory
    output_base = SCRIPT_DIR / "output"
    output_base.mkdir(parents=True, exist_ok=True)

    # Run benchmarks
    results: list[BenchmarkResult] = []
    for name in tool_names:
        tool = TOOLS[name]
        if not _is_available(tool):
            console.print(
                f"[yellow]Skipping {name}:[/] not set up. "
                "Run [cyan]just bench-setup[/] first."
            )
            continue

        n_runs = runs if runs > 0 else tool.default_runs
        console.print(f"\n[bold]{name}[/] ({n_runs} run{'s' if n_runs > 1 else ''})")

        result = run_benchmark(tool, input_file, output_base, n_runs, time_cmd)
        if result:
            results.append(result)

    if not results:
        console.print("[red]No results collected.[/]")
        raise typer.Exit(1)

    # Print table
    _print_results_table(results)

    # Save results
    saved_path = _save_results(results, input_file, results_dir)
    console.print(f"\nResults saved to [cyan]{saved_path}[/]")


if __name__ == "__main__":
    app()
