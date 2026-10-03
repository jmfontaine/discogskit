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

# Bytes that can follow "<tag" in a record's opening tag.
_TAG_NAME_END = frozenset(b"> \t\r\n/")


def _find_data_region(mm: mmap.mmap, tag: bytes, end_tag: bytes) -> tuple[int, int]:
    # The bare "<tag" prefix also matches the container element ("<artist" in "<artists>"), so skip matches that
    # continue the tag name. Searching for "<tag>" and "<tag " separately would scan the whole file for whichever
    # form the dump never uses.
    prefix = b"<" + tag
    after = len(prefix)
    start = mm.find(prefix)
    while start != -1 and (
        start + after >= len(mm) or mm[start + after] not in _TAG_NAME_END
    ):
        start = mm.find(prefix, start + 1)
    if start == -1:
        raise ValueError(f"No <{tag.decode()}> elements found")
    end = mm.rfind(end_tag)
    if end == -1:
        raise ValueError(f"No {end_tag!r} boundary found")
    return start, end + len(end_tag)


def find_split_points(
    file_path: str, target_chunk_bytes: int, tag: str
) -> list[tuple[int, int]]:
    """Split the ``<tag>`` records in ``file_path`` into byte ranges of about ``target_chunk_bytes``."""
    # A negative size makes the search below step backwards forever.
    if target_chunk_bytes <= 0:
        raise ValueError(
            f"target_chunk_bytes must be positive, got {target_chunk_bytes}"
        )
    tag_bytes = tag.encode()
    end_tag = b"</" + tag_bytes + b">\n"
    with open(file_path, "rb") as f:
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        try:
            data_start, data_end = _find_data_region(mm, tag_bytes, end_tag)
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
