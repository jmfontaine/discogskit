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
from io import BytesIO
from xml.parsers import expat

import pyarrow as pa
from lxml import etree
from pyarrow import ipc

from discogskit.entities import ChunkArgs, Cols, ElementValueError, get


def _line(element: etree._Element) -> str:
    return "unknown" if element.sourceline is None else str(element.sourceline)


class _RecordFound(Exception):
    """Stops the expat scan in ``_record_lines`` once it reaches the record; carries both lines."""


def _record_lines(
    xml_data: bytes,
    container: str,
    record: etree._Element,
    ordinal: int,
    element: etree._Element,
) -> tuple[str, str]:
    """Return the chunk lines where ``record``, the ``ordinal``-th record (0-based) the worker parsed, and ``element``,
    the record itself or an element inside it, start.

    lxml's ``sourceline`` isn't enough: libxml2 stores element lines in 16 bits, and past line 65535 it reports what a
    bounded search inside the element finds, or 65535, e.g. for a compact record whose first text is nested deep.
    expat counts lines exactly. Only the error path calls this, so the second parse costs nothing on valid data.
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
    # Namespaced names carry their URI ("urn:x artist"), so they never equal a bare tag, just as lxml's never do.
    parser = expat.ParserCreate(namespace_separator=" ")
    # (name, start line) of each open element.
    open_elements: list[tuple[str, int]] = []
    # One per open record: [its start line, elements started in it so far, the element's start line or 0].
    open_records: list[list[int]] = []
    seen = 0

    def is_record(name: str) -> bool:
        # The worker's selection: record tags whose parent is the container tag.
        return (
            name == root_tag
            and bool(open_elements)
            and open_elements[-1][0] == container
        )

    def end(name: str) -> None:
        nonlocal seen
        open_elements.pop()
        if is_record(name):
            record_line, _, element_line = open_records.pop()
            # The worker handles records in end-event order.
            if seen == ordinal:
                raise _RecordFound(record_line, element_line)
            seen += 1

    def start(name: str, _attrs: dict[str, str]) -> None:
        line = parser.CurrentLineNumber
        for counts in open_records:
            if counts[1] == offset:
                counts[2] = line
            counts[1] += 1
        if is_record(name):
            open_records.append([line, 1, line if offset == 0 else 0])
        open_elements.append((name, line))

    parser.EndElementHandler = end
    parser.StartElementHandler = start
    try:
        parser.Parse(xml_data, True)
    except _RecordFound as found:
        record_line, element_line = found.args
        return str(record_line), str(element_line) if element_line else "unknown"
    # expat rejects some names lxml accepts, e.g. with characters beyond U+FFFF.
    except expat.ExpatError:
        pass
    # From line 65535 on, lxml can report another node's line, even a smaller one. Its lines are the elements' own only
    # when the chunk can't reach that line; counting CRs too keeps the bound safe whatever the line breaks. They're
    # where the start tags end rather than where they start.
    if xml_data.count(b"\n") + xml_data.count(b"\r") < 65534:
        return _line(record), _line(element)
    return "unknown", "unknown"


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
    cols: Cols = {
        name: {col_name: [] for col_name in schema.names}
        for name, schema in entity.schemas.items()
    }
    unknown: set[str] | None = set() if args.strict else None

    ordinal = 0
    try:
        for _, elem in etree.iterparse(
            BytesIO(xml_data), events=("end",), tag=entity.root_tag
        ):
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
                    xml_data, entity.name, elem, ordinal, element
                )
                detail = (
                    exc if element is elem else f"{exc} at chunk line {element_line}"
                )
                raise ValueError(
                    f"Invalid <{entity.root_tag}> record in {args.file_path} at chunk line {line}"
                    f" (chunk bytes {args.start}-{args.end}{id_part}): {detail}"
                ) from exc
            ordinal += 1
            # Standard lxml memory optimization for iterparse: free each element after processing to prevent the
            # entire tree from accumulating. Without this, a 256 MB chunk would build a multi-GB tree in memory.
            elem.clear()
            while elem.getprevious() is not None:
                del parent[0]
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
