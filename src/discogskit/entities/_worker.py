"""Shared chunk worker: XML byte range -> normalized Arrow tables -> IPC bytes.

``extract_chunk_to_ipc`` runs in a separate PROCESS (via ``multiprocessing.Pool``). It must stay a module-level function
(not a closure or method) so that pickle can send it to the workers, and it receives ``ChunkArgs`` with the entity name
rather than the ``EntityDef`` itself.

Workers build up columns as Python lists (one list per column per table), then convert them to Arrow RecordBatches at
the end of each chunk. This is faster than appending to Arrow arrays incrementally because:
  - Python list.append is O(1) amortized
  - RecordBatch.from_pydict does a single bulk conversion
  - Avoids intermediate Arrow allocations per row
"""

from __future__ import annotations

import warnings
from collections.abc import Callable
from io import BytesIO
from typing import TypeVar
from xml.parsers import expat

import pyarrow as pa
from lxml import etree
from pyarrow import ipc

from discogskit.entities import ChunkArgs, Cols, get

_T = TypeVar("_T")


class _ScanResult(Exception):
    """Carries a visitor's result out of ``_scan_lines``."""


def _first_namespaced_child_line(xml_data: bytes) -> int | None:
    """Line where the container's first namespaced direct child starts, or None if expat finds none or rejects the chunk.

    lxml's ``sourceline`` gives the line where an element's start tag ends, and past line 65535 only an estimate; see
    ``_scan_lines``.
    """

    def on_start(name: str, line: int, ancestors: list[str]) -> int | None:
        # Only the container is open above a direct child; a namespaced name contains the separator.
        return line if len(ancestors) == 1 and " " in name else None

    return _scan_lines(xml_data, on_start)


def _record_line(
    xml_data: bytes, container: str, record: etree._Element, ordinal: int
) -> str:
    """Return the chunk line where ``record``, the ``ordinal``-th record (0-based) the worker parsed, starts.

    lxml's ``sourceline`` isn't enough: past line 65535 it reports what a bounded search inside the element finds, or
    65535, e.g. for a compact record whose first text is nested deep; see ``_scan_lines``.
    """
    root_tag = record.tag
    # Start line of each open record.
    record_lines: list[int] = []
    seen = 0

    def is_record(name: str, ancestors: list[str]) -> bool:
        # The worker's selection: record tags whose parent is the container tag.
        return name == root_tag and bool(ancestors) and ancestors[-1] == container

    def on_end(name: str, ancestors: list[str]) -> int | None:
        nonlocal seen
        if not is_record(name, ancestors):
            return None
        line = record_lines.pop()
        # The worker handles records in end-event order.
        if seen == ordinal:
            return line
        seen += 1
        return None

    def on_start(name: str, line: int, ancestors: list[str]) -> None:
        if is_record(name, ancestors):
            record_lines.append(line)

    found = _scan_lines(xml_data, on_start, on_end)
    if found is not None:
        return str(found)
    # expat rejected the chunk (or missed the record). From line 65535 on, lxml can report another node's line, even a
    # smaller one. Its line is the record's own only when the chunk can't reach that line; counting CRs too keeps the
    # bound safe whatever the line breaks. It's where the start tag ends rather than where it starts.
    line = record.sourceline
    if line is not None and xml_data.count(b"\n") + xml_data.count(b"\r") < 65534:
        return str(line)
    return "unknown"


def _scan_lines(
    xml_data: bytes,
    on_start: Callable[[str, int, list[str]], _T | None],
    on_end: Callable[[str, list[str]], _T | None] | None = None,
) -> _T | None:
    """Re-read the chunk with expat, which counts lines exactly, until a visitor returns a value; return that value.

    lxml's ``sourceline`` can't be used for this: libxml2 stores element lines in 16 bits, so past line 65535 it only
    estimates, and a chunk can run to millions of lines. ``on_start`` gets each element's name, the line where its
    start tag begins and the names of its open ancestors; ``on_end`` gets the name and the ancestors. Names of
    namespaced elements are "<uri> <local name>", so they never equal a bare tag, just as lxml's never do.

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
    try:
        parser.Parse(xml_data, True)
    except _ScanResult as found:
        return found.args[0]
    except expat.ExpatError:
        return None
    return None


def _serialize_batch(batch: pa.RecordBatch, schema: pa.Schema) -> bytes:
    sink = pa.BufferOutputStream()
    writer = ipc.new_stream(sink, schema)
    writer.write_batch(batch)
    writer.close()
    return sink.getvalue().to_pybytes()


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


def extract_chunk_to_ipc(args: ChunkArgs) -> dict[str, bytes]:
    """Parse one chunk of ``<root_tag>`` records into one serialized RecordBatch per table.

    Reads the byte range from the decompressed XML, wraps it in the ``<name>`` container so it is a valid document,
    and parses it with lxml iterparse.
    """
    entity = get(args.entity)
    with open(args.file_path, "rb") as f:
        f.seek(args.start)
        data = f.read(args.end - args.start)

    container = entity.name.encode()
    # No newline after the container's opening tag, so the chunk starts on line 1 and lxml's line numbers count from
    # the chunk's first byte.
    header = b"<?xml version='1.0' encoding='UTF-8'?><" + container + b">"
    footer = b"\n</" + container + b">"
    # One copy of the chunk; header + data + footer would copy it twice.
    xml_data = b"".join((header, data, footer))
    # Free the chunk itself now rather than holding both copies for the whole parse (issue #122).
    del data
    cols: Cols = {
        name: {col_name: [] for col_name in schema.names}
        for name, schema in entity.schemas.items()
    }
    unknown: set[str] | None = set() if args.strict else None

    def reject_namespaced(child: etree._Element) -> None:
        # The tag filter below only matches records without a namespace (lxml names <artist xmlns="urn:x"> as
        # "{urn:x}artist"), so any namespaced direct child of the container would be dropped silently. Its meaning
        # can't be guessed, so fail instead.
        if child.tag[:1] == "{":
            # Direct children are checked in document order, so this is the first namespaced one.
            line = _first_namespaced_child_line(xml_data)
            where = (
                f"near line {child.sourceline}" if line is None else f"on line {line}"
            )
            raise ValueError(
                f"Unsupported {entity.name} XML in {args.file_path}, chunk at bytes {args.start}-{args.end}"
                f" (line numbers count from byte {args.start}): namespaced record element <{child.tag}>"
                f" {where}; namespaces aren't supported"
            )

    ordinal = 0
    try:
        # Comments and processing instructions aren't elements; kept, their callable .tag would reach the parsers'
        # unknown-element checks. The other options are lxml's hardening, passed explicitly because the defaults vary
        # by lxml version (6.0.x still resolves entities).
        records = etree.iterparse(
            BytesIO(xml_data),
            events=("end",),
            huge_tree=False,
            load_dtd=False,
            no_network=True,
            remove_comments=True,
            remove_pis=True,
            resolve_entities=False,
            tag=entity.root_tag,
        )
        for _, elem in records:
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
                line = _record_line(xml_data, entity.name, elem, ordinal)
                raise ValueError(
                    f"Invalid <{entity.root_tag}> record in {args.file_path} at chunk line {line}"
                    f" (chunk bytes {args.start}-{args.end}{id_part}): {exc}"
                ) from exc
            ordinal += 1
            # Standard lxml memory optimization for iterparse: free each element after processing to prevent the
            # entire tree from accumulating. Without this, a 256 MB chunk would build a multi-GB tree in memory.
            elem.clear()
            # Each child of the container before this record is checked and deleted here, in document order; those
            # after the chunk's last record are checked once parsing ends. A nested element named like the container
            # isn't the container, so its children aren't checked.
            top_level = parent.getparent() is None
            while elem.getprevious() is not None:
                if top_level:
                    reject_namespaced(parent[0])
                del parent[0]
        for child in records.root:
            reject_namespaced(child)
    except etree.XMLSyntaxError as exc:
        # XMLSyntaxError carries an error log that can't be pickled, so the pool would only report "cannot pickle
        # '_ListErrorLog'" and the real error would be lost (#60). Re-raise as a plain ValueError with the location.
        # Only parsing raises it; errors from append_record get their own context above.
        detail = exc.msg
        raise ValueError(
            f"Malformed {entity.name} XML in {args.file_path}, chunk at bytes {args.start}-{args.end}"
            f" (line numbers count from byte {args.start}): {detail}"
        ) from exc

    if unknown:
        for tag in sorted(unknown):
            warnings.warn(
                f"unhandled XML element <{tag}> in <{entity.root_tag}>", stacklevel=1
            )

    return {
        name: _serialize_batch(_to_batch(args, cols[name], name, schema), schema)
        for name, schema in entity.schemas.items()
    }
