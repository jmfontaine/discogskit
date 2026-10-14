"""Shared mmap-based XML split utility.

Splits a decompressed XML dump into byte-range chunks so that multiple processes can parse them independently
with no coordination.

One compiled regex walks the mmap looking for every ``<tag>`` / ``<tag .../>`` / ``</tag>`` occurrence of the
record tag, so the per-byte work of locating candidate markup and parsing a start tag's attribute grammar runs in
C instead of a Python loop. A record's own markup can contain further elements with the *same* tag name (e.g. a
label's ``<sublabels>`` holds more ``<label>`` elements), so Python only has to track nesting depth from the
events the regex yields: an opening tag increments it, a closing tag decrements it, and a self-closing
``<tag .../>`` doesn't change it unless it's at depth zero, in which case it's a complete record on its own. A
closing tag only ends a complete top-level record when it brings the depth back to zero. We make no assumption
about formatting — indentation, line endings, a separator between records, or ``>`` inside a quoted attribute
value — depth is what defines a boundary, not surrounding bytes. Comments, CDATA sections, processing
instructions and DOCTYPE declarations are matched and skipped whole, since they can contain tag-like text that
isn't a real element. Anything that starts like one of those constructs but doesn't close — an unterminated
comment, a malformed start tag, a stray or missing closing tag — is a hard error, as is anything before the first
record other than XML whitespace, an XML 1.0 UTF-8 declaration at byte 0 and the container's bare start tag, and
anything after the last record other than whitespace, a comment, a processing instruction, and exactly one
occurrence of the container's own closing tag: we fail loudly instead of guessing what unrecognized content means
or silently dropping it.
Each returned chunk ``[start, end)`` is guaranteed to contain only complete top-level elements, so workers can wrap
the bytes in an XML envelope and parse with iterparse.
"""

from __future__ import annotations

import mmap
import re

# Zero or more "name=value" attributes (double- or single-quoted), consuming the quotes so a literal ">" or "/>"
# inside a value is never mistaken for the end of the tag.
_ATTRS = rb"(?:\s+[^\s=/>]+\s*=\s*(?:\"[^\"]*\"|'[^']*'))*\s*"

# The constructs allowed around the container's closing tag after the last record, each matched whole. The main
# scan in ``_compile_pattern`` matches these, CDATA sections and DOCTYPE declarations with its own inline copies.
_COMMENT = rb"<!--(?:[^-]*(?:-(?!->)[^-]*)*-->)"
_PI = rb"<\?(?:[^?]*(?:\?(?!>)[^?]*)*\?>)"

# XML's own whitespace production (S ::= (#x20 | #x9 | #xD | #xA)+), not Python's broader \s (which also matches
# \f and \v, neither of which XML allows).
_XML_WS = rb"[\x20\x09\x0d\x0a]"

# What may surround the container's closing tag: whitespace, comments and processing instructions. After the closer
# that's XML's ``Misc`` production; before it, a CDATA section or DOCTYPE would be content no chunk carries or
# markup XML doesn't allow there. Each repetition consumes exactly one whitespace byte so no two ways of matching
# the same bytes exist: with ``\s+`` instead, a failed match tried every way to split a whitespace run, which took
# exponential time in its length (issue #104).
_MISC = rb"(?:" + _XML_WS + rb"|" + _COMMENT + rb"|" + _PI + rb")*"


def _compile_pattern(tag: bytes) -> re.Pattern[bytes]:
    """Compile a pattern matching every ``<tag>``, ``<tag .../>`` and ``</tag>`` occurrence of ``tag``, plus every
    comment, CDATA section, processing instruction and DOCTYPE declaration, each matched whole or not at all.

    A tag name that only shares ``tag`` as a prefix (``<label`` inside ``<labelfoo>`` or the container
    ``<labels>``) fails every branch below and is silently skipped, the same way a plain non-matching byte is.
    A `bad_*` group fires when a construct recognizably starts (the tag name boundary is right, or the construct
    opener matched) but doesn't close properly — an unterminated comment/CDATA/PI, or a malformed start or end
    tag — so the caller can raise instead of guessing where it actually ends.
    """
    t = re.escape(tag)
    return re.compile(
        rb"<(?:"
        + t
        + rb"(?:"
        + _ATTRS
        + rb"(?:(?P<open>>)|(?P<empty>/>))|(?P<bad_open>(?![^\s/>])))"
        + rb"|/"
        + t
        + rb"(?:\s*(?P<close>>)|(?P<bad_close>(?![^\s>])))"
        + rb"|!(?:--(?:[^-]*(?:-(?!->)[^-]*)*-->|(?P<bad_comment>))"
        + rb"|\[CDATA\[(?:[^\]]*(?:\](?!\]>)[^\]]*)*\]\]>|(?P<bad_cdata>))"
        + rb"|DOCTYPE(?:[^\[>]*(?:\[[^\]]*\])?\s*>|(?P<bad_doctype>)))"
        + rb"|\?(?:[^?]*(?:\?(?!>)[^?]*)*\?>|(?P<bad_pi>))"
        + rb")"
    )


def _scan_records(
    mm: mmap.mmap, tag: bytes, container: bytes, target_chunk_bytes: int
) -> tuple[list[tuple[int, int]], int]:
    """Scan the whole file for ``<tag>`` records, tracking nesting depth, and split them into byte ranges.

    A complete top-level record ends at a closing tag that brings the depth back to zero, or at a self-closing tag
    found at depth zero. Each range starts at the first record or where the previous range ended, and ends at the
    first record end at least ``target_chunk_bytes`` past its start, except the last, which ends after the last
    record. Only the open range's start is kept, not every record's end: the releases dump has tens of millions
    of records (issue #122).

    Returns the ranges and the byte offset right after the last record. Raises ``ValueError`` for unsupported
    content before the first record (checked as soon as the scan reaches it, see ``_check_leading_content``), a
    closing tag with no matching opener, an unclosed element at end of file, malformed or unterminated markup, or
    no ``<tag>`` elements at all.
    """
    pattern = _compile_pattern(tag)
    group_index = pattern.groupindex
    open_group = group_index["open"]
    empty_group = group_index["empty"]
    close_group = group_index["close"]
    bad_groups = frozenset(v for k, v in group_index.items() if k.startswith("bad_"))

    depth = 0
    range_start = -1
    data_end = -1
    splits: list[tuple[int, int]] = []
    for m in pattern.finditer(mm):
        g = m.lastindex
        if range_start < 0:
            # Validate what precedes the first record before reading further (issue #79): a releases file is
            # tens of GB, and a record tag inside unsupported content (e.g. a container attribute value) must be
            # reported as that content, not as whatever structural error it causes later.
            if g == open_group or g == empty_group:
                range_start = m.start()
                _check_leading_content(mm, range_start, tag, container)
            elif (g == close_group or g in bad_groups) and m.start() > 0:
                _check_leading_content(mm, m.start(), tag, container)
        if g == open_group:
            depth += 1
            continue
        if g == close_group:
            depth -= 1
            if depth < 0:
                raise ValueError(
                    f"</{tag.decode()}> at byte {m.start()} has no matching <{tag.decode()}>"
                )
        elif g != empty_group:
            if g in bad_groups:
                raise ValueError(
                    f"Malformed or unterminated markup at byte {m.start()}: "
                    f"{mm[m.start() : m.start() + 40]!r}"
                )
            # A comment, CDATA section, processing instruction or DOCTYPE matched whole: no depth change.
            continue
        if depth == 0:
            data_end = m.end()
            if data_end - range_start >= target_chunk_bytes:
                splits.append((range_start, data_end))
                range_start = data_end

    if depth != 0:
        raise ValueError(f"{depth} unclosed <{tag.decode()}> element(s) at end of file")
    if range_start < 0:
        raise ValueError(f"No <{tag.decode()}> elements found")
    if range_start < data_end:
        splits.append((range_start, data_end))
    return splits, data_end


# What the worker's injected envelope reproduces exactly before the first record (issue #79): XML whitespace, at most
# an XML declaration for version 1.0 in UTF-8 (the one the envelope itself injects), and the container's start tag
# without attributes. Anything else (a byte-order mark, comments, processing instructions, a DOCTYPE, attributes on
# the container) would be dropped or reinterpreted by the envelope, so it's rejected rather than silently lost.
_EQ = _XML_WS + rb"*=" + _XML_WS + rb"*"
_XML_DECL = (
    rb"<\?xml"
    + _XML_WS
    + rb"+version"
    + _EQ
    + rb"(?:'1\.0'|\"1\.0\")"
    + rb"(?:"
    + _XML_WS
    + rb"+encoding"
    + _EQ
    + rb"(?:'(?i:utf-8)'|\"(?i:utf-8)\"))?"
    + rb"(?:"
    + _XML_WS
    + rb"+standalone"
    + _EQ
    + rb"(?:'(?:yes|no)'|\"(?:yes|no)\"))?"
    + _XML_WS
    + rb"*\?>"
)


def _describe_leading(mm: mmap.mmap, pos: int, container: bytes) -> str:
    """Name the unsupported construct starting at ``pos`` for the error message."""
    head = mm[pos : pos + 64]
    # Only a UTF-8 byte-order mark can get here: a UTF-16 or UTF-32 file has no ASCII ``<tag>`` for the record scan
    # to find, so it already failed with "No <tag> elements found".
    if head[:3] == b"\xef\xbb\xbf":
        return "a byte-order mark"
    if re.match(rb"<\?[xX][mM][lL](?:" + _XML_WS + rb"|\?>)", head):
        return "an XML declaration other than version 1.0 in UTF-8 at the start of the file"
    if head.startswith(b"<!DOCTYPE"):
        return "a DOCTYPE"
    if head.startswith(b"<!--"):
        return "a comment"
    if head.startswith(b"<?"):
        return "a processing instruction"
    # An attribute must be preceded by whitespace; ``<container/>`` or ``<containerx>`` aren't attribute lists.
    if re.match(
        rb"<" + re.escape(container) + _XML_WS + rb"+[^\x20\x09\x0d\x0a>]", head
    ):
        return f"attributes on <{container.decode()}> (e.g. a namespace declaration)"
    return f"unexpected content {head[:40]!r}"


def _check_leading_content(
    mm: mmap.mmap, data_start: int, tag: bytes, container: bytes
) -> None:
    """Raise unless everything before the first record is something the worker's injected envelope reproduces.

    Workers parse each chunk inside their own ``<?xml version='1.0' encoding='UTF-8'?><container>`` envelope, so a
    different encoding, a DOCTYPE, or attributes on the container (namespace declarations included) would be
    misread or silently dropped. Fail before any parsing instead, naming what was found.
    """
    parts = (
        # XML only allows the declaration at byte 0, so whitespace may follow it but not precede it.
        re.compile(rb"(?:" + _XML_DECL + rb")?" + _XML_WS + rb"*"),
        re.compile(rb"<" + re.escape(container) + _XML_WS + rb"*>"),
        re.compile(_XML_WS + rb"*"),
    )
    # Every part must match, in order, and together cover exactly [0, data_start). Checking only the final position
    # would accept a missing container tag when the first record starts at byte 0.
    pos = 0
    complete = True
    for part in parts:
        match = part.match(mm, pos, data_start)
        if match is None:
            complete = False
            break
        pos = match.end()
    if not complete or pos != data_start:
        what = (
            f"missing <{container.decode()}> start tag before the first record"
            if pos == data_start
            else _describe_leading(mm, pos, container)
        )
        raise ValueError(
            f"Unsupported content before the first <{tag.decode()}> record at byte {pos}: {what}"
        )


def _trailing_content_pattern(container: bytes) -> re.Pattern[bytes]:
    """Compile a pattern for exactly what may follow the last record: the container's own closing tag — not any
    tag, not duplicated, not missing — preceded and followed by whitespace, comments and processing instructions.
    """
    return re.compile(_MISC + rb"</" + re.escape(container) + _XML_WS + rb"*>" + _MISC)


def _check_trailing_content(
    mm: mmap.mmap, data_end: int, tag: bytes, container: bytes
) -> None:
    """Raise unless what follows the last record is exactly the container's closing tag, once.

    A missing closer looks like a file truncated right at a record boundary — the likeliest and most dangerous
    way this check can be wrong, since nothing else here would catch it. A different or repeated tag name, or
    anything else this scan doesn't recognize, is rejected the same way: this is never a guess at what valid
    content would mean, only at what the splitter can and cannot classify.
    """
    length = len(mm)
    match = _trailing_content_pattern(container).match(mm, data_end, length)
    end = match.end() if match is not None else -1
    if end != length:
        trailing = mm[data_end : min(length, data_end + 40)]
        raise ValueError(
            f"Expected </{container.decode()}> after the last <{tag.decode()}> record at byte {data_end}, "
            f"found: {trailing!r}"
        )


def find_split_points(
    file_path: str, target_chunk_bytes: int, tag: str, container: str
) -> list[tuple[int, int]]:
    """Split the ``<tag>`` records in ``file_path`` into byte ranges of about ``target_chunk_bytes``.

    ``container`` is the enclosing element (``<container>...<tag/>...</container>``): its closing tag is the
    only thing that may follow the last record.
    """
    # Zero or less would silently put every record in a range of its own.
    if target_chunk_bytes <= 0:
        raise ValueError(
            f"target_chunk_bytes must be positive, got {target_chunk_bytes}"
        )
    tag_bytes = tag.encode()
    container_bytes = container.encode()
    try:
        with open(file_path, "rb") as f:
            mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
            try:
                splits, data_end = _scan_records(
                    mm, tag_bytes, container_bytes, target_chunk_bytes
                )
                _check_trailing_content(mm, data_end, tag_bytes, container_bytes)
                return splits
            finally:
                mm.close()
    except ValueError as exc:
        # Callers pass only a path, and mmap's own errors (an empty file) don't name it.
        raise ValueError(f"{file_path}: {exc}") from exc
