"""Shared chunk worker: XML byte range -> normalized Arrow tables -> IPC bytes.

``extract_chunk_to_ipc`` runs in a separate PROCESS (a parse worker the pipeline starts). It must stay a module-level
function (not a closure or method) so that pickle can send it to the workers, and it receives ``ChunkArgs`` with the
entity name rather than the ``EntityDef`` itself.

Workers build up columns as Python lists (one list per column per table), then convert them to Arrow RecordBatches at
the end of each chunk. This is faster than appending to Arrow arrays incrementally because:
  - Python list.append is O(1) amortized
  - RecordBatch.from_pydict does a single bulk conversion
  - Avoids intermediate Arrow allocations per row
"""

from __future__ import annotations

from collections.abc import Callable
from typing import NamedTuple, NoReturn, Protocol, TypeVar, cast
from xml.parsers import expat

import pyarrow as pa
from lxml import etree

from discogskit._ipc import serialize_batch
from discogskit.entities import ENTITIES
from discogskit.entities._def import (
    XML_SPACE,
    ChunkArgs,
    Cols,
    ElementValueError,
    has_text,
)

# Bytes of the chunk read and fed to the parser at a time. Reading the whole chunk and wrapping it in its container
# held a copy of it for the whole parse, and two while wrapping: 256 MiB each at the default chunk size (issue #30).
# Measured on 256 MiB chunks of every entity, 64 KiB slices parse no slower than the whole chunk did, while 1 MiB
# slices parsed one labels chunk 22-57% slower (#30).
_FEED_BYTES = 64 * 1024
_T = TypeVar("_T")


class ChunkResult(NamedTuple):
    """One parsed chunk."""

    # Table name -> one serialized RecordBatch, in the entity's table order.
    tables: dict[str, pa.Buffer]
    # Sorted paths of the elements, attributes and text no parser handled, relative to the record (e.g.
    # ``videos/video/foo``, ``videos/video/@foo``, ``urls/text()``); only collected with ``--strict``.
    unknown: list[str]


class _Clearable(Protocol):
    """``etree._Element.clear``, whose ``keep_tail`` argument lxml's stubs leave out."""

    def clear(self, keep_tail: bool = False) -> None: ...


class _ScanResult(Exception):
    """Carries a visitor's result out of ``_scan_lines``."""


def _first_stray(xml_data: bytes, root_tag: str) -> tuple[str, str, int] | None:
    """The container's first direct content that isn't a ``<root_tag>`` record, or None if expat finds none or rejects
    the chunk: ``("element", its name as lxml writes it, the line its start tag begins on)`` or ``("text", its first
    line, the line that is on)``.

    lxml's ``sourceline`` gives the line where an element's start tag ends, past line 65535 only an estimate, and no
    line for text; see ``_scan_lines``.
    """

    def on_start(
        name: str, line: int, ancestors: list[str]
    ) -> tuple[str, str, int] | None:
        # Only the container is open above a direct child. A namespaced name is "<uri> <local name>".
        if len(ancestors) != 1 or name == root_tag:
            return None
        uri, _, local = name.rpartition(" ")
        return "element", f"{{{uri}}}{local}" if uri else local, line

    def on_text(
        data: str, line: int, ancestors: list[str]
    ) -> tuple[str, str, int] | None:
        if len(ancestors) != 1 or not (text := data.strip(XML_SPACE)):
            return None
        # expat can hand over the line breaks before the text in the same piece.
        leading = data[: len(data) - len(data.lstrip(XML_SPACE))]
        return "text", _snippet(text), line + leading.count("\n")

    return _scan_lines(xml_data, on_start, on_text=on_text)


def _line(element: etree._Element) -> str:
    return "unknown" if element.sourceline is None else str(element.sourceline)


def _record_lines(
    xml_data: bytes,
    container: str,
    record: etree._Element,
    ordinal: int,
    element: etree._Element,
) -> tuple[str, str]:
    """Return the chunk lines where ``record``, the ``ordinal``-th record (0-based) the worker parsed, and ``element``,
    the record itself or an element inside it, start.

    lxml's ``sourceline`` isn't enough: past line 65535 it reports what a bounded search inside the element finds, or
    65535, e.g. for a compact record whose first text is nested deep; see ``_scan_lines``.
    """
    root_tag = record.tag
    # The element's position among the record's elements in document order, which expat counts too. A record that
    # holds records of its own (under a nested container) lost their content when the worker cleared them, so the
    # count would no longer match expat's.
    elements = [e for e in record.iter() if isinstance(e.tag, str)]
    nested_records = any(
        e.tag == root_tag and (p := e.getparent()) is not None and p.tag == container
        for e in elements[1:]
    )
    offset = (
        None
        if nested_records
        else next((i for i, e in enumerate(elements) if e is element), None)
    )
    # One per open record: [its start line, elements started in it so far, the element's start line or 0].
    open_records: list[list[int]] = []
    seen = 0

    def is_record(name: str, ancestors: list[str]) -> bool:
        # The worker's selection: record tags whose parent is the container tag.
        return name == root_tag and bool(ancestors) and ancestors[-1] == container

    def on_end(name: str, ancestors: list[str]) -> tuple[int, int] | None:
        nonlocal seen
        if not is_record(name, ancestors):
            return None
        record_line, _, element_line = open_records.pop()
        # The worker handles records in end-event order.
        if seen == ordinal:
            return record_line, element_line
        seen += 1
        return None

    def on_start(name: str, line: int, ancestors: list[str]) -> None:
        for counts in open_records:
            if counts[1] == offset:
                counts[2] = line
            counts[1] += 1
        if is_record(name, ancestors):
            open_records.append([line, 1, line if offset == 0 else 0])

    found = _scan_lines(xml_data, on_start, on_end)
    if found is not None:
        record_line, element_line = found
        return str(record_line), str(element_line) if element_line else "unknown"
    # expat rejected the chunk (or missed the record). From line 65535 on, lxml can report another node's line, even a
    # smaller one. Its lines are the elements' own only when the chunk can't reach that line; counting CRs too keeps
    # the bound safe whatever the line breaks. They're where the start tags end rather than where they start.
    if xml_data.count(b"\n") + xml_data.count(b"\r") < 65534:
        return _line(record), _line(element)
    return "unknown", "unknown"


def _scan_lines(
    xml_data: bytes,
    on_start: Callable[[str, int, list[str]], _T | None],
    on_end: Callable[[str, list[str]], _T | None] | None = None,
    *,
    on_text: Callable[[str, int, list[str]], _T | None] | None = None,
) -> _T | None:
    """Re-read the chunk with expat, which counts lines exactly, until a visitor returns a value; return that value.

    lxml's ``sourceline`` can't be used for this: libxml2 stores element lines in 16 bits, so past line 65535 it only
    estimates, and a chunk can run to millions of lines. ``on_start`` gets each element's name, the line where its
    start tag begins and the names of its open ancestors; ``on_end`` gets the name and the ancestors; ``on_text`` gets
    a piece of character data, the line where it begins and the names of the elements it's in. Names of namespaced
    elements are "<uri> <local name>", so they never equal a bare tag, just as lxml's never do.

    Returns None when no visitor returns a value, or when expat rejects the chunk: it accepts fewer names than libxml2
    (e.g. U+0370), so it can fail on a chunk lxml parsed. This re-reads the whole chunk, so it's for failure paths only.
    """
    parser = expat.ParserCreate(namespace_separator=" ")
    ancestors: list[str] = []

    def end(name: str) -> None:
        ancestors.pop()
        if on_end is not None and (result := on_end(name, ancestors)) is not None:
            raise _ScanResult(result)

    def start(name: str, _attrs: object) -> None:
        if (result := on_start(name, parser.CurrentLineNumber, ancestors)) is not None:
            raise _ScanResult(result)
        ancestors.append(name)

    parser.EndElementHandler = end
    parser.StartElementHandler = start
    if on_text is not None:
        visit_text = on_text

        def text(data: str) -> None:
            if (
                result := visit_text(data, parser.CurrentLineNumber, ancestors)
            ) is not None:
                raise _ScanResult(result)

        parser.CharacterDataHandler = text
    try:
        parser.Parse(xml_data, True)
    except _ScanResult as found:
        return found.args[0]
    except expat.ExpatError:
        return None
    return None


def _snippet(text: str) -> str:
    """The start of ``text``, stray text, short enough for an error message."""
    return text.strip(XML_SPACE).splitlines()[0][:40]


def _to_batch(
    args: ChunkArgs, columns: dict[str, list], name: str, schema: pa.Schema
) -> pa.RecordBatch:
    """Convert one table's column lists to a RecordBatch, naming the table and chunk when a value doesn't fit."""
    try:
        return pa.RecordBatch.from_pydict(columns, schema=schema)
    except (OverflowError, pa.ArrowInvalid) as exc:
        raise ValueError(
            f"Invalid {name} table values in {args.file_path} (chunk bytes {args.start}-{args.end}): {exc}"
        ) from exc


def _chunk_xml(args: ChunkArgs, header: bytes, footer: bytes) -> bytes:
    """The whole chunk in its envelope, read again for the error messages that scan it for exact lines."""
    with open(args.file_path, "rb") as f:
        f.seek(args.start)
        return header + f.read(args.end - args.start) + footer


def extract_chunk_to_ipc(args: ChunkArgs) -> ChunkResult:
    """Parse one chunk of ``<root_tag>`` records into one serialized RecordBatch per table.

    Feeds the byte range from the decompressed XML to lxml's pull parser, wrapped in the ``<name>`` container so it is
    a valid document, ``_FEED_BYTES`` at a time. Fails on anything but records and whitespace directly in the
    container. With ``args.strict``, also returns the paths of the elements, attributes and text no parser handled;
    the main process reports them once per run, so workers print nothing.
    """
    entity = ENTITIES[args.entity]
    container = entity.name.encode()
    # No newline after the container's opening tag, so the chunk starts on line 1 and lxml's line numbers count from
    # the chunk's first byte.
    header = b"<?xml version='1.0' encoding='UTF-8'?><" + container + b">"
    footer = b"\n</" + container + b">"
    cols: Cols = {
        name: {col_name: [] for col_name in schema.names}
        for name, schema in entity.schemas.items()
    }
    unknown: set[str] | None = set() if args.strict else None

    def reject_stray(kind: str, value: str, near: etree._Element) -> NoReturn:
        # The tag filter below only matches records without a namespace (lxml names <artist xmlns="urn:x"> as
        # "{urn:x}artist"), so any other direct child of the container, and any text between records, would be
        # dropped silently. Its meaning can't be guessed, so fail instead. lxml found ``value``, an element's name or
        # text, near ``near``; expat finds the chunk's first such content and its exact line.
        where = f"near line {_line(near)}"
        found = _first_stray(_chunk_xml(args, header, footer), entity.root_tag)
        if found is not None:
            kind, value, line = found
            where = f"on line {line}"
        prefix = (
            f"Unsupported {entity.name} XML in {args.file_path}, chunk at bytes {args.start}-{args.end}"
            f" (line numbers count from byte {args.start}): "
        )
        if kind == "text":
            content = f"text {value!r}"
        elif value[:1] == "{" and value.endswith("}" + entity.root_tag):
            raise ValueError(
                f"{prefix}namespaced record element <{value}> {where}; namespaces aren't supported"
            )
        else:
            content = f"element <{value}>"
        raise ValueError(
            f"{prefix}{content} {where}; only <{entity.root_tag}> records may appear in <{entity.name}>"
        )

    def check_top_level(child: etree._Element) -> None:
        if child.tag != entity.root_tag:
            reject_stray("element", child.tag, child)
        if (tail := child.tail) is not None and has_text(tail):
            reject_stray("text", _snippet(tail), child)

    def check_top_text(root: etree._Element) -> None:
        # Text before the container's first child; the text after each child is that child's tail.
        if (text := root.text) is not None and has_text(text):
            reject_stray("text", _snippet(text), root[0] if len(root) else root)

    ordinal = 0

    def parse_records(parser: etree.XMLPullParser) -> None:
        nonlocal ordinal
        for _, event_elem in parser.read_events():
            # Only "end" events are asked for, and each carries an element; the stubs type every kind of event.
            elem = cast("etree._Element", event_elem)
            # Only records directly under the container are records; e.g. <label> also appears inside <sublabels>.
            parent = elem.getparent()
            if parent is None or parent.tag != entity.name:
                continue
            try:
                entity.append_record(cols, elem, unknown)
            except (OverflowError, ValueError) as exc:
                # Without the file, chunk and record, a bad value in a 60 GB dump can't be found (#93). The new error
                # carries only a string, so it pickles back to the parent.
                record_id = entity.record_id(elem)
                id_part = "" if record_id is None else f", id={record_id!r}"
                element = exc.element if isinstance(exc, ElementValueError) else elem
                line, element_line = _record_lines(
                    _chunk_xml(args, header, footer),
                    entity.name,
                    elem,
                    ordinal,
                    element,
                )
                detail = (
                    exc if element is elem else f"{exc} at chunk line {element_line}"
                )
                raise ValueError(
                    f"Invalid <{entity.root_tag}> record in {args.file_path} at chunk line {line}"
                    f" (chunk bytes {args.start}-{args.end}{id_part}): {detail}"
                ) from exc
            ordinal += 1
            # Standard lxml memory optimization for incremental parsing: free each element after processing to
            # prevent the entire tree from accumulating. Without this, a 256 MiB chunk would build a multi-GB tree.
            # Keeping the tail, the text up to the next record, lets the loop below check it: by this end event the
            # parser has usually read it already, and clear() alone would delete it.
            cast(_Clearable, elem).clear(keep_tail=True)
            # Each child of the container before this record is checked and deleted here, in document order, with
            # the text after it; those after the chunk's last record are checked once parsing ends. A nested element
            # named like the container isn't the container, so its children aren't checked.
            top_level = parent.getparent() is None
            if top_level:
                check_top_text(parent)
            while elem.getprevious() is not None:
                if top_level:
                    check_top_level(parent[0])
                del parent[0]

    # libxml2's error log is shared by every parser in the thread, and a syntax error copies it whole: cleared here,
    # the copy holds only this chunk's errors.
    etree.clear_error_log()
    # Comments and processing instructions aren't elements; kept, their callable .tag would reach the parsers'
    # unknown-element checks. The other options are lxml's hardening, passed explicitly because the defaults vary by
    # lxml version (6.0.x still resolves entities).
    parser = etree.XMLPullParser(
        events=("end",),
        huge_tree=False,
        load_dtd=False,
        no_network=True,
        remove_comments=True,
        remove_pis=True,
        resolve_entities=False,
        tag=entity.root_tag,
    )
    try:
        parser.feed(header)
        with open(args.file_path, "rb") as f:
            f.seek(args.start)
            remaining = args.end - args.start
            while remaining > 0:
                data = f.read(min(_FEED_BYTES, remaining))
                # The file is shorter than when it was split. Its records may still end cleanly where it stops, so
                # parsing what's left could silently drop the rest of the chunk.
                if not data:
                    raise ValueError(
                        f"{args.file_path} ended at byte {args.end - remaining}, inside the chunk at bytes"
                        f" {args.start}-{args.end}: it changed after it was split"
                    )
                remaining -= len(data)
                parser.feed(data)
                parse_records(parser)
        parser.feed(footer)
        root = parser.close()
        parse_records(parser)
        check_top_text(root)
        for child in root:
            check_top_level(child)
    except etree.XMLSyntaxError as exc:
        # The slice that holds the malformed XML may also end records before it, whose events feed() queued before
        # raising. Handle them first, so the error reported is the first one in the document, whatever the slices.
        parse_records(parser)
        # XMLSyntaxError carries an error log that can't be pickled, so the main process would only report "cannot
        # pickle '_ListErrorLog'" and the real error would be lost (#60). Re-raise as a plain ValueError with the location.
        # Only parsing raises it; errors from append_record get their own context above.
        # exc.msg is the last error libxml2 logged, which can be a follow-on of the first, e.g. at the envelope's
        # footer after an undeclared entity. The first fatal error is the cause. lxml leaves the column out when it's 0.
        first = next(iter(exc.error_log.filter_from_fatals()), None)
        if first is None:
            detail = exc.msg
        else:
            column = f", column {first.column}" if first.column else ""
            detail = f"{first.message}, line {first.line}{column}"
        raise ValueError(
            f"Malformed {entity.name} XML in {args.file_path}, chunk at bytes {args.start}-{args.end}"
            f" (line numbers count from byte {args.start}): {detail}"
        ) from exc

    return ChunkResult(
        tables={
            name: serialize_batch(_to_batch(args, cols[name], name, schema), schema)
            for name, schema in entity.schemas.items()
        },
        unknown=sorted(unknown) if unknown else [],
    )
