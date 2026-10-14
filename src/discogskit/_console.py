"""Shared Rich console for all discogskit output.

Everything discogskit prints goes through ``current()``: ``console``, the default, unless ``using()`` set another one
for the current context. ``pipeline.run()`` sets the console its caller passes, so a library caller can silence or
redirect a run's output.
"""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar

from rich.console import Console
from rich.text import Text

console = Console()

_current: ContextVar[Console] = ContextVar("discogskit_console", default=console)


def current() -> Console:
    """The console output goes to in the current context."""
    return _current.get()


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
    out = current()
    highlighted = out.highlighter(text.plain)
    highlighted.copy_styles(text)
    out.print(highlighted)


def set_current(target: Console) -> None:
    """Send the current context's output to ``target``, e.g. as a new thread's initializer."""
    _current.set(target)


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


@contextmanager
def using(target: Console) -> Generator[None, None, None]:
    """Send the current context's output to ``target`` until the block ends."""
    token = _current.set(target)
    try:
        yield
    finally:
        _current.reset(token)
