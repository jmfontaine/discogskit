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


class _Located(Exception):
    """Stops the expat scan in ``_first_namespaced_child_line`` once it has the line."""


def _first_namespaced_child_line(xml_data: bytes) -> int | None:
    """Line where the container's first namespaced direct child starts, or None if expat finds none or rejects the chunk.

    lxml's ``sourceline`` gives the line where an element's start tag ends, and past line 65535 only an estimate, since
    libxml2 stores element lines in 16 bits; a chunk can run to millions of lines. Expat reports the line where the start
    tag begins and counts lines in full, but accepts fewer names than libxml2 (e.g. U+0370), so it can fail on a chunk
    lxml parsed. This re-reads the chunk, so it's for the failure path only.
    """
    parser = expat.ParserCreate(namespace_separator=" ")
    depth = 0

    def end(_name: str) -> None:
        nonlocal depth
        depth -= 1

    def start(name: str, _attrs: object) -> None:
        nonlocal depth
        depth += 1
        # With a separator, expat names a namespaced element "<uri> <local name>".
        if depth == 2 and " " in name:
            raise _Located(parser.CurrentLineNumber)

    parser.EndElementHandler = end
    parser.StartElementHandler = start
    try:
        parser.Parse(xml_data, True)
    except _Located as located:
        return located.args[0]
    except expat.ExpatError:
        return None
    return None


def _serialize_batch(batch: pa.RecordBatch, schema: pa.Schema) -> bytes:
    sink = pa.BufferOutputStream()
    writer = ipc.new_stream(sink, schema)
    writer.write_batch(batch)
    writer.close()
    return sink.getvalue().to_pybytes()


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
            entity.append_record(cols, elem, unknown)
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
        # Only parsing raises it; other errors (e.g. a bug in append_record) propagate unchanged.
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
        name: _serialize_batch(
            pa.RecordBatch.from_pydict(cols[name], schema=schema), schema
        )
        for name, schema in entity.schemas.items()
    }
