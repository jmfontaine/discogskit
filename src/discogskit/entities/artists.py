"""Artists entity definition: Arrow schemas, XML parsing, IPC worker.

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
    "artists",
    "artist_aliases",
    "artist_groups",
    "artist_members",
]

# ------------------------------------------------------------------------------------------------------------------------
# Arrow schemas
# ------------------------------------------------------------------------------------------------------------------------

SCHEMAS = {
    "artists": pa.schema(
        [
            pa.field("id", pa.int32(), nullable=False),
            pa.field("data_quality", pa.utf8(), nullable=False),
            pa.field("name", pa.utf8(), nullable=False),
            pa.field("namevariations", pa.list_(pa.utf8()), nullable=False),
            pa.field("profile", pa.utf8(), nullable=False),
            pa.field("realname", pa.utf8(), nullable=False),
            pa.field("urls", pa.list_(pa.utf8()), nullable=False),
        ]
    ),
    "artist_aliases": pa.schema(
        [
            pa.field("artist_id", pa.int32(), nullable=False),
            pa.field("alias_id", pa.int32()),
            pa.field("name", pa.utf8(), nullable=False),
        ]
    ),
    "artist_groups": pa.schema(
        [
            pa.field("artist_id", pa.int32(), nullable=False),
            pa.field("group_id", pa.int32()),
            pa.field("name", pa.utf8(), nullable=False),
        ]
    ),
    "artist_members": pa.schema(
        [
            pa.field("artist_id", pa.int32(), nullable=False),
            pa.field("member_id", pa.int32()),
            pa.field("name", pa.utf8(), nullable=False),
        ]
    ),
}

TABLE_WEIGHTS = {
    "artists": 0.50,
    "artist_aliases": 0.20,
    "artist_groups": 0.15,
    "artist_members": 0.15,
}

# ------------------------------------------------------------------------------------------------------------------------
# XML constants
# ------------------------------------------------------------------------------------------------------------------------

_ARTIST_END = b"</artist>\n"
_XML_HEADER = b"<?xml version='1.0' encoding='UTF-8'?>\n<artists>\n"
_XML_FOOTER = b"\n</artists>"

# ------------------------------------------------------------------------------------------------------------------------
# XML splitting
# ------------------------------------------------------------------------------------------------------------------------

find_split_points: Callable[[str, int], list[tuple[int, int]]] = make_split_finder(
    b"<artist>", _ARTIST_END
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


def _parse_refs(
    cols: _Cols, table: str, artist_id: int, parent: etree._Element
) -> None:
    """Parse artist ref children (aliases, groups, members)."""
    for name_elem in parent.findall("name"):
        ref_id_str = name_elem.get("id")
        ref_id = int(ref_id_str) if ref_id_str else None
        name = name_elem.text or ""
        id_field = {
            "artist_aliases": "alias_id",
            "artist_groups": "group_id",
            "artist_members": "member_id",
        }[table]
        row = cols[table]
        row["artist_id"].append(artist_id)
        row[id_field].append(ref_id)
        row["name"].append(name)


def _append_artist(
    cols: _Cols, elem: etree._Element, unknown: set[str] | None = None
) -> None:
    """Parse an <artist> element and append all data to column accumulators."""
    id_text = elem.findtext("id")
    if not id_text:
        warnings.warn("skipping <artist> with missing id", stacklevel=2)
        return
    artist_id = int(id_text)

    data_quality = ""
    name = ""
    name_variations = []
    profile = ""
    real_name = ""
    urls = []

    for child in elem:
        tag = child.tag
        if tag == "id":
            pass  # already extracted via findtext above
        elif tag == "aliases":
            _parse_refs(cols, "artist_aliases", artist_id, child)
        elif tag == "data_quality":
            data_quality = child.text or ""
        elif tag == "groups":
            _parse_refs(cols, "artist_groups", artist_id, child)
        elif tag == "members":
            _parse_refs(cols, "artist_members", artist_id, child)
        elif tag == "name":
            name = child.text or ""
        elif tag == "namevariations":
            name_variations = [n.text for n in child.findall("name") if n.text]
        elif tag == "profile":
            profile = child.text or ""
        elif tag == "realname":
            real_name = child.text or ""
        elif tag == "urls":
            urls = [u.text for u in child.findall("url") if u.text]
        elif unknown is not None:
            unknown.add(tag)

    r = cols["artists"]
    r["id"].append(artist_id)
    r["data_quality"].append(data_quality)
    r["name"].append(name)
    r["namevariations"].append(name_variations)
    r["profile"].append(profile)
    r["realname"].append(real_name)
    r["urls"].append(urls)


# ------------------------------------------------------------------------------------------------------------------------
# Chunk worker
# ------------------------------------------------------------------------------------------------------------------------


def extract_chunk_to_ipc(args: ChunkArgs) -> dict[str, bytes]:
    """Worker: parse XML chunk -> 4 normalized RecordBatches -> IPC bytes dict."""

    with open(args.file_path, "rb") as f:
        f.seek(args.start)
        data = f.read(args.end - args.start)

    xml_data = _XML_HEADER + data + _XML_FOOTER
    cols = _new_cols()
    unknown: set[str] | None = set() if args.strict else None

    for _, elem in etree.iterparse(BytesIO(xml_data), events=("end",), tag="artist"):
        _append_artist(cols, elem, unknown)
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]

    if unknown:
        for tag in sorted(unknown):
            warnings.warn(f"unhandled XML element <{tag}> in <artist>", stacklevel=1)

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

ARTISTS_ENTITY = EntityDef(
    name="artists",
    root_tag="artist",
    table_order=TABLE_ORDER,
    schemas=SCHEMAS,
    table_weights=TABLE_WEIGHTS,
    extract_chunk_to_ipc=extract_chunk_to_ipc,
    find_split_points=find_split_points,
    fk_column="artist_id",
)

register(ARTISTS_ENTITY)
