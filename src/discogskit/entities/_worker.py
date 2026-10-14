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

from discogskit.entities import ChunkArgs, Cols, get


class _RecordFound(Exception):
    """Stops the expat scan in ``_record_line`` once it reaches the record."""


def _record_line(
    xml_data: bytes, container: str, record: etree._Element, ordinal: int
) -> str:
    """Return the chunk line where ``record``, the ``ordinal``-th record (0-based) the worker parsed, starts.

    lxml's ``sourceline`` isn't enough: libxml2 stores element lines in 16 bits, and past line 65535 it reports what a
    bounded search inside the element finds, or 65535, e.g. for a compact record whose first text is nested deep.
    expat counts lines exactly. Only the error path calls this, so the second parse costs nothing on valid data.
    """
    root_tag = record.tag
    # Namespaced names carry their URI ("urn:x artist"), so they never equal a bare tag, just as lxml's never do.
    parser = expat.ParserCreate(namespace_separator=" ")
    # (name, start line) of each open element.
    open_elements: list[tuple[str, int]] = []
    seen = 0

    def end(name: str) -> None:
        nonlocal seen
        _, line = open_elements.pop()
        # The worker's selection, in its order: end events of record tags whose parent is the container tag.
        if name == root_tag and open_elements and open_elements[-1][0] == container:
            if seen == ordinal:
                raise _RecordFound(line)
            seen += 1

    def start(name: str, _attrs: dict[str, str]) -> None:
        open_elements.append((name, parser.CurrentLineNumber))

    parser.EndElementHandler = end
    parser.StartElementHandler = start
    try:
        parser.Parse(xml_data, True)
    except _RecordFound as found:
        return str(found.args[0])
    # expat rejects some names lxml accepts, e.g. with characters beyond U+FFFF.
    except expat.ExpatError:
        pass
    # From line 65535 on, lxml can report another node's line, even a smaller one. Its line is the record's own only
    # when the chunk can't reach that line; counting CRs too keeps the bound safe whatever the line breaks. It's where
    # the start tag ends rather than where it starts.
    line = record.sourceline
    if line is not None and xml_data.count(b"\n") + xml_data.count(b"\r") < 65534:
        return str(line)
    return "unknown"


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
                line = _record_line(xml_data, entity.name, elem, ordinal)
                raise ValueError(
                    f"Invalid <{entity.root_tag}> record in {args.file_path} at chunk line {line}"
                    f" (chunk bytes {args.start}-{args.end}{id_part}): {exc}"
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
