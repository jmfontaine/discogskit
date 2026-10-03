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


def _read_envelope(args: ChunkArgs, container: bytes) -> tuple[bytes, bytes, bytes]:
    """Return ``(header, data, footer)`` for one chunk.

    When the splitter found real envelope boundaries (``prolog_end``/``footer_start``), the header and footer are
    read from the file itself — XML declaration, DOCTYPE, comments, and the container's actual start and end tags
    with their real attributes (e.g. namespace declarations) and declared encoding — instead of a guessed one.
    Both are offsets, not bytes, so a large DOCTYPE isn't copied into every chunk's pickled ``ChunkArgs``; this
    reads them directly from disk, which is cheap next to reading the chunk's own data.

    Without them (e.g. a bare record snippet in a test, with no real prolog or container to preserve), a minimal
    envelope is synthesized, as before.
    """
    with open(args.file_path, "rb") as f:
        if args.prolog_end is not None and args.footer_start is not None:
            header = f.read(args.prolog_end)
            f.seek(args.start)
            data = f.read(args.end - args.start)
            f.seek(args.footer_start)
            footer = f.read()
        else:
            header = b"<?xml version='1.0' encoding='UTF-8'?><" + container + b">"
            f.seek(args.start)
            data = f.read(args.end - args.start)
            footer = b"\n</" + container + b">"
    return header, data, footer


def extract_chunk_to_ipc(args: ChunkArgs) -> dict[str, bytes]:
    """Parse one chunk of ``<root_tag>`` records into one serialized RecordBatch per table.

    Reads the byte range from the decompressed XML, wraps it in the real (or, failing that, a synthesized) ``<name>``
    envelope so it is a valid document, and parses it with lxml iterparse.
    """
    entity = get(args.entity)
    header, data, footer = _read_envelope(args, entity.name.encode())
    # One copy of the chunk; header + data + footer would copy it twice.
    xml_data = b"".join((header, data, footer))
    cols: Cols = {
        name: {col_name: [] for col_name in schema.names}
        for name, schema in entity.schemas.items()
    }
    unknown: set[str] | None = set() if args.strict else None

    try:
        for _, elem in etree.iterparse(
            BytesIO(xml_data),
            events=("end",),
            tag=entity.root_tag,
            # The real prolog can now carry a DOCTYPE from the dump, which could declare external entities (a
            # local file or a URL). Both are lxml defaults already (as of lxml 6.1 and 2.0 respectively) — spelled
            # out so a future lxml release changing either default can't silently start reading files or hitting
            # the network on our behalf. resolve_entities="internal" expands only entities the DOCTYPE itself
            # defines inline, leaving external ones unresolved (and erroring if referenced); no_network blocks the
            # network as a last resort for anything that would otherwise reach out for an external DTD/entity.
            resolve_entities="internal",
            no_network=True,
        ):
            # Only records directly under the container are records; e.g. <label> also appears inside <sublabels>.
            parent = elem.getparent()
            if parent is None or parent.tag != entity.name:
                continue
            entity.append_record(cols, elem, unknown)
            # Standard lxml memory optimization for iterparse: free each element after processing to prevent the
            # entire tree from accumulating. Without this, a 256 MB chunk would build a multi-GB tree in memory.
            elem.clear()
            while elem.getprevious() is not None:
                del parent[0]
    except etree.XMLSyntaxError as exc:
        # XMLSyntaxError carries an error log that can't be pickled, so the pool would only report "cannot pickle
        # '_ListErrorLog'" and the real error would be lost (#60). Re-raise as a plain ValueError with the location.
        # Only parsing raises it; other errors (e.g. a bug in append_record) propagate unchanged.
        #
        # The header's own line count matters now that it can be the dump's real (possibly multi-line) prolog
        # instead of always a single line: lxml counts lines from 1 at the start of the whole envelope, so this
        # chunk's own data — and the byte offset we can actually point at — only starts at line (header lines + 1).
        header_lines = header.count(b"\n")
        if header_lines:
            position_note = (
                f"line {header_lines + 1} begins at byte {args.start}; subtract {header_lines} from any other "
                f"line number below to get the line within this chunk"
            )
        else:
            position_note = f"line numbers count from byte {args.start}"
        raise ValueError(
            f"Malformed {entity.name} XML in {args.file_path}, chunk at bytes {args.start}-{args.end}"
            f" ({position_note}): {exc.msg}"
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
