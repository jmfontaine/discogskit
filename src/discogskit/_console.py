"""Shared Rich console for all discogskit output."""

from __future__ import annotations

from rich.console import Console
from rich.text import Text

console = Console()


def error(message: str) -> None:
    """Print ``Error: message``, with ``message`` printed literally (see ``print_text``)."""
    print_text(Text.assemble(("Error:", "red"), " ", message))


def print_text(text: Text) -> None:
    """Print ``text`` highlighted the way ``console.print`` highlights a str.

    A str passed to ``console.print`` is parsed as Rich markup and emoji codes, so
    dynamic text (exceptions, paths, Discogs data with ``[b]…[/b]``-style tags) can
    lose words or raise ``MarkupError``. A ``Text`` is never parsed; this keeps the
    highlighting a str would get, with ``text``'s own styles on top.
    """
    highlighted = console.highlighter(text.plain)
    highlighted.copy_styles(text)
    console.print(highlighted)


def status(label: str, detail: str = "", timing: str = "", *, style: str = "") -> None:
    """Print a consistently formatted status line.

    Output format: ``  Label         detail [timing]``

    ``detail`` and ``timing`` print literally (see ``print_text``); ``style`` styles
    the whole detail.
    """
    line = Text.assemble("  ", (f"{label:<14s}", "bold"))
    if detail:
        line.append(" ")
        line.append(detail, style=style)
    if timing:
        line.append(" ")
        line.append(timing, style="dim")
    print_text(line)
