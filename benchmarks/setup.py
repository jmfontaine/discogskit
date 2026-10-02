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
        "repo": "https://github.com/philipmat/discogs-xml2db.git",
        "dir": "discogs-xml2db",
        "setup": ["python3", "-m", "venv", ".venv"],
        "install": [".venv/bin/pip", "install", "-q", "-r", "requirements.txt"],
    },
}


def _run(cmd: list[str], cwd: Path | None = None) -> bool:
    """Run a command, return True on success."""
    try:
        subprocess.run(cmd, cwd=cwd, check=True, capture_output=True)
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

    # Clone if needed
    if tool_dir.exists() and force:
        console.print("  Removing existing directory...")
        import shutil

        shutil.rmtree(tool_dir)

    if not tool_dir.exists():
        console.print(f"  Cloning {config['repo']}...")
        if not _run(["git", "clone", "--depth", "1", config["repo"], str(tool_dir)]):
            console.print("  [red]Failed to clone[/]")
            return False
    else:
        console.print("  Already cloned, skipping...")

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
    ALTERNATIVES_DIR.mkdir(parents=True, exist_ok=True)

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
