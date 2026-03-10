"""Shared Rich console for all discogskit output."""

from __future__ import annotations

from rich.console import Console

console = Console()


def status(label: str, detail: str = "", timing: str = "") -> None:
    """Print a consistently formatted status line.

    Output format: ``  Label         detail [timing]``
    """
    parts = [f"  [bold]{label:<14s}[/]"]
    if detail:
        parts.append(detail)
    if timing:
        parts.append(f"[dim]{timing}[/]")
    console.print(" ".join(parts))
