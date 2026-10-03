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

import pyarrow as pa
from lxml import etree
from pyarrow import ipc

from discogskit.entities import ChunkArgs, Cols, get


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
    header = b"<?xml version='1.0' encoding='UTF-8'?>\n<" + container + b">\n"
    footer = b"\n</" + container + b">"
    # One copy of the chunk; header + data + footer would copy it twice.
    xml_data = b"".join((header, data, footer))
    cols: Cols = {
        name: {col_name: [] for col_name in schema.names}
        for name, schema in entity.schemas.items()
    }
    unknown: set[str] | None = set() if args.strict else None

    for _, elem in etree.iterparse(
        BytesIO(xml_data), events=("end",), tag=entity.root_tag
    ):
        # Only records directly under the container are records; e.g. <label> also appears inside <sublabels>.
        parent = elem.getparent()
        if parent is None or parent.tag != entity.name:
            continue
        entity.append_record(cols, elem, unknown)
        # Standard lxml memory optimization for iterparse: free each element after processing to prevent the entire
        # tree from accumulating. Without this, a 256 MB chunk would build a multi-GB tree in memory.
        elem.clear()
        while elem.getprevious() is not None:
            del parent[0]

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
