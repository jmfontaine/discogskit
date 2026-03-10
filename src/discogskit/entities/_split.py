"""Shared mmap-based XML split utility.

Splits a decompressed XML dump into byte-range chunks so that multiple processes can parse them independently
with no coordination.

Uses mmap for zero-copy scanning. We never parse XML just to find split points. Instead we search for the
closing tag byte pattern (e.g. ``</release>\\n``) to locate element boundaries.  Each returned chunk ``[start, end)``
is guaranteed to contain only complete top-level elements, so workers can wrap the bytes in an XML envelope
and parse with iterparse.
"""

from __future__ import annotations

import mmap
from typing import Callable


def make_split_finder(
    start_pattern: bytes, end_tag: bytes
) -> Callable[[str, int], list[tuple[int, int]]]:
    """Return a find_split_points function for the given element tags."""

    def _find_data_region(mm: mmap.mmap) -> tuple[int, int]:
        start = mm.find(start_pattern)
        if start == -1:
            raise ValueError(f"No {start_pattern!r} elements found")
        end = mm.rfind(end_tag)
        if end == -1:
            raise ValueError(f"No {end_tag!r} boundary found")
        return start, end + len(end_tag)

    def find_split_points(
        file_path: str, target_chunk_bytes: int
    ) -> list[tuple[int, int]]:
        with open(file_path, "rb") as f:
            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            try:
                data_start, data_end = _find_data_region(mm)
                splits = []
                pos = data_start
                while pos < data_end:
                    boundary = mm.find(end_tag, pos + target_chunk_bytes)
                    if boundary == -1 or boundary >= data_end:
                        splits.append((pos, data_end))
                        break
                    boundary += len(end_tag)
                    splits.append((pos, boundary))
                    pos = boundary
                return splits
            finally:
                mm.close()

    return find_split_points
