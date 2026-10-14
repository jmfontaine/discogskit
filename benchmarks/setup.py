#!/usr/bin/env python3
"""Set up benchmark competitor tools."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

import typer
from rich.console import Console

console = Console()

SCRIPT_DIR = Path(__file__).parent
ALTERNATIVES_DIR = SCRIPT_DIR / "alternatives"

TOOLS: dict[str, dict[str, Any]] = {
    "xml2db-python": {
        # Pinned so every setup benchmarks the same code: a commit, not a branch, and exact versions of the packages
        # its requirements.txt names (unversioned there) and their dependencies.
        "commit": "1f7cd478b3b157b5554699ff19f050d56572806e",
        "dir": "discogs-xml2db",
        "install": [
            ".venv/bin/pip",
            "install",
            "-q",
            "certifi==2026.7.22",
            "charset-normalizer==3.5.2",
            "docopt==0.6.2",
            "idna==3.20",
            "lxml==6.1.3",
            "requests==2.34.2",
            "tqdm==4.70.1",
            "urllib3==2.8.0",
        ],
        "repo": "https://github.com/philipmat/discogs-xml2db.git",
        "setup": ["python3", "-m", "venv", ".venv"],
    },
}


def _run(cmd: list[str], cwd: Path | None = None) -> bool:
    """Run a command, return True on success."""
    try:
        subprocess.run(cmd, capture_output=True, check=True, cwd=cwd)
        return True
    except subprocess.CalledProcessError as e:
        console.print(f"  [red]Error:[/] {e.stderr.decode()[:200]}")
        return False
    except FileNotFoundError:
        return False


def _setup_tool(name: str, config: dict[str, Any], *, force: bool = False) -> bool:
    """Set up a single competitor tool."""
    console.print(f"[bold]=== {name} ===[/]")
    tool_dir = ALTERNATIVES_DIR / config["dir"]

    # Re-clone if asked
    if tool_dir.exists() and force:
        console.print("  Removing existing directory...")
        import shutil

        shutil.rmtree(tool_dir)

    # Fetch only the pinned commit; this also moves an existing checkout to it.
    console.print(f"  Checking out {config['repo']} at {config['commit']}...")
    tool_dir.mkdir(exist_ok=True, parents=True)
    if not (
        _run(["git", "init", "-q"], cwd=tool_dir)
        and _run(
            ["git", "fetch", "-q", "--depth", "1", config["repo"], config["commit"]],
            cwd=tool_dir,
        )
        and _run(["git", "checkout", "-q", "--detach", "FETCH_HEAD"], cwd=tool_dir)
    ):
        console.print("  [red]Failed to check out[/]")
        return False

    # Run setup commands
    if "setup" in config:
        console.print("  Setting up...")
        _run(config["setup"], cwd=tool_dir)

    if "install" in config:
        console.print("  Installing dependencies...")
        if not _run(config["install"], cwd=tool_dir):
            console.print("  [red]Failed to install dependencies[/]")
            return False

    console.print("  [green]Done[/]")
    return True


app = typer.Typer(help="Set up benchmark competitor tools.")


@app.command()
def main(
    force: bool = typer.Option(False, help="Re-clone tools even if they exist."),
) -> None:
    """Set up all benchmark competitor tools."""
    ALTERNATIVES_DIR.mkdir(exist_ok=True, parents=True)

    console.print("[bold]Setting up benchmark alternatives...[/]\n")

    results: dict[str, bool] = {}
    for name, config in TOOLS.items():
        results[name] = _setup_tool(name, config, force=force)
        console.print()

    # Summary
    console.print("[bold]Summary:[/]")
    for name, success in results.items():
        status = "[green]OK[/]" if success else "[red]FAILED[/]"
        console.print(f"  {name}: {status}")


if __name__ == "__main__":
    app()
