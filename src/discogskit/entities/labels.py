"""Labels entity definition: Arrow schemas, XML parsing, IPC worker.

See ``releases.py`` for detailed comments on the shared patterns: column accumulators, iterparse memory optimization,
and IPC serialization.
"""

from __future__ import annotations

import warnings
from collections.abc import Callable
from io import BytesIO

import pyarrow as pa
import pyarrow.ipc as ipc
from lxml import etree

from discogskit.entities import ChunkArgs, EntityDef, register
from discogskit.entities._split import make_split_finder

_Cols = dict[str, dict[str, list]]

# ------------------------------------------------------------------------------------------------------------------------
# Constants
# ------------------------------------------------------------------------------------------------------------------------

TABLE_ORDER = [
    "labels",
    "label_sublabels",
]

# ------------------------------------------------------------------------------------------------------------------------
# Arrow schemas
# ------------------------------------------------------------------------------------------------------------------------

SCHEMAS = {
    "labels": pa.schema(
        [
            pa.field("id", pa.int32(), nullable=False),
            pa.field("contactinfo", pa.utf8(), nullable=False),
            pa.field("data_quality", pa.utf8(), nullable=False),
            pa.field("name", pa.utf8(), nullable=False),
            pa.field("parent_label_id", pa.int32()),
            pa.field("parent_label_name", pa.utf8(), nullable=False),
            pa.field("profile", pa.utf8(), nullable=False),
            pa.field("urls", pa.list_(pa.utf8()), nullable=False),
        ]
    ),
    "label_sublabels": pa.schema(
        [
            pa.field("label_id", pa.int32(), nullable=False),
            pa.field("sublabel_id", pa.int32()),
            pa.field("name", pa.utf8(), nullable=False),
        ]
    ),
}

TABLE_WEIGHTS = {
    "labels": 0.70,
    "label_sublabels": 0.30,
}

# ------------------------------------------------------------------------------------------------------------------------
# XML constants
# ------------------------------------------------------------------------------------------------------------------------

_LABEL_END = b"</label>\n"
_XML_HEADER = b"<?xml version='1.0' encoding='UTF-8'?>\n<labels>\n"
_XML_FOOTER = b"\n</labels>"

# ------------------------------------------------------------------------------------------------------------------------
# XML splitting
# ------------------------------------------------------------------------------------------------------------------------

find_split_points: Callable[[str, int], list[tuple[int, int]]] = make_split_finder(
    b"<label>", _LABEL_END
)

# ------------------------------------------------------------------------------------------------------------------------
# IPC helpers
# ------------------------------------------------------------------------------------------------------------------------


def _serialize_batch(batch: pa.RecordBatch, schema: pa.Schema) -> bytes:
    sink = pa.BufferOutputStream()
    writer = ipc.new_stream(sink, schema)
    writer.write_batch(batch)
    writer.close()
    return sink.getvalue().to_pybytes()


# ------------------------------------------------------------------------------------------------------------------------
# Column accumulators
# ------------------------------------------------------------------------------------------------------------------------


def _new_cols() -> _Cols:
    return {
        name: {col_name: [] for col_name in schema.names}
        for name, schema in SCHEMAS.items()
    }


def _cols_to_ipc(cols: _Cols) -> dict[str, bytes]:
    result = {}
    for name, schema in SCHEMAS.items():
        batch = pa.RecordBatch.from_pydict(cols[name], schema=schema)
        result[name] = _serialize_batch(batch, schema)
    return result


# ------------------------------------------------------------------------------------------------------------------------
# XML parsing
# ------------------------------------------------------------------------------------------------------------------------


def _append_label(
    cols: _Cols, elem: etree._Element, unknown: set[str] | None = None
) -> None:
    """Parse a <label> element and append all data to column accumulators."""
    id_text = elem.findtext("id")
    if not id_text:
        warnings.warn("skipping <label> with missing id", stacklevel=2)
        return
    label_id = int(id_text)

    contact_info = ""
    data_quality = ""
    name = ""
    parent_label_id = None
    parent_label_name = ""
    profile = ""
    urls = []

    for child in elem:
        tag = child.tag
        if tag == "id":
            pass  # already extracted via findtext above
        elif tag == "contactinfo":
            contact_info = child.text or ""
        elif tag == "data_quality":
            data_quality = child.text or ""
        elif tag == "name":
            name = child.text or ""
        elif tag == "parentLabel":
            pl_id_str = child.get("id")
            parent_label_id = int(pl_id_str) if pl_id_str else None
            parent_label_name = child.text or ""
        elif tag == "profile":
            profile = child.text or ""
        elif tag == "sublabels":
            for sub in child.findall("label"):
                sub_id_str = sub.get("id")
                sub_id = int(sub_id_str) if sub_id_str else None
                sub_name = sub.text or ""
                row = cols["label_sublabels"]
                row["label_id"].append(label_id)
                row["sublabel_id"].append(sub_id)
                row["name"].append(sub_name)
        elif tag == "urls":
            urls = [u.text for u in child.findall("url") if u.text]
        elif unknown is not None:
            unknown.add(tag)

    r = cols["labels"]
    r["id"].append(label_id)
    r["contactinfo"].append(contact_info)
    r["data_quality"].append(data_quality)
    r["name"].append(name)
    r["parent_label_id"].append(parent_label_id)
    r["parent_label_name"].append(parent_label_name)
    r["profile"].append(profile)
    r["urls"].append(urls)


# ------------------------------------------------------------------------------------------------------------------------
# Chunk worker
# ------------------------------------------------------------------------------------------------------------------------


def extract_chunk_to_ipc(args: ChunkArgs) -> dict[str, bytes]:
    """Worker: parse XML chunk -> 2 normalized RecordBatches -> IPC bytes dict."""
    with open(args.file_path, "rb") as f:
        f.seek(args.start)
        data = f.read(args.end - args.start)

    xml_data = _XML_HEADER + data + _XML_FOOTER
    cols = _new_cols()
    unknown: set[str] | None = set() if args.strict else None

    for _, elem in etree.iterparse(BytesIO(xml_data), events=("end",), tag="label"):
        # Skip nested <label> elements inside <sublabels> — only process top-level <label> children of the root
        # <labels> element.
        if elem.getparent().tag != "labels":
            continue
        _append_label(cols, elem, unknown)
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]

    if unknown:
        for tag in sorted(unknown):
            warnings.warn(f"unhandled XML element <{tag}> in <label>", stacklevel=1)

    return _cols_to_ipc(cols)


# ------------------------------------------------------------------------------------------------------------------------
# Import-time integrity assertion
# ------------------------------------------------------------------------------------------------------------------------

assert set(SCHEMAS) == set(TABLE_ORDER), (
    f"Table key mismatch: schemas={set(SCHEMAS) - set(TABLE_ORDER)}, "
    f"order={set(TABLE_ORDER) - set(SCHEMAS)}"
)
assert set(TABLE_WEIGHTS) == set(TABLE_ORDER), (
    f"Table weight mismatch: weights={set(TABLE_WEIGHTS) - set(TABLE_ORDER)}, "
    f"order={set(TABLE_ORDER) - set(TABLE_WEIGHTS)}"
)

# ------------------------------------------------------------------------------------------------------------------------
# Register
# ------------------------------------------------------------------------------------------------------------------------

LABELS_ENTITY = EntityDef(
    name="labels",
    root_tag="label",
    table_order=TABLE_ORDER,
    schemas=SCHEMAS,
    table_weights=TABLE_WEIGHTS,
    extract_chunk_to_ipc=extract_chunk_to_ipc,
    find_split_points=find_split_points,
    fk_column="label_id",
)

register(LABELS_ENTITY)
