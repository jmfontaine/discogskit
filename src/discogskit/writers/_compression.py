"""Compression argument checks the file writers run in ``__init__``, before any file exists."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Collection


def check_compression(
    output_format: str, compression: str, valid: Collection[str]
) -> None:
    """Raise ``ValueError`` if ``compression`` isn't one of the codecs in ``valid``."""
    if compression not in valid:
        listed = ", ".join([*sorted(c for c in valid if c != "none"), "none"])
        raise ValueError(
            f"unsupported compression {compression!r} for {output_format}. Valid: {listed}."
        )


def check_compression_level(
    compression: str, level: int | None, level_range: tuple[int, int] | None
) -> None:
    """Raise ``ValueError`` if ``level`` is set and ``compression`` has no levels (``level_range`` None) or it's out of range."""
    if level is None:
        return
    if level_range is None:
        raise ValueError(f"{compression} does not support a compression level.")
    lo, hi = level_range
    if not lo <= level <= hi:
        raise ValueError(
            f"compression level {level} is out of range for {compression} ({lo} to {hi})."
        )
