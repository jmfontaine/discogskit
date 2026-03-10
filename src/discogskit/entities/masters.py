"""Masters entity definition: Arrow schemas, XML parsing, IPC worker.

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
    "masters",
    "master_artists",
    "master_videos",
]

# ------------------------------------------------------------------------------------------------------------------------
# Arrow schemas
# ------------------------------------------------------------------------------------------------------------------------

SCHEMAS = {
    "masters": pa.schema(
        [
            pa.field("id", pa.int32(), nullable=False),
            pa.field("data_quality", pa.utf8(), nullable=False),
            pa.field("genres", pa.list_(pa.utf8()), nullable=False),
            pa.field("main_release", pa.int32()),
            pa.field("notes", pa.utf8(), nullable=False),
            pa.field("styles", pa.list_(pa.utf8()), nullable=False),
            pa.field("title", pa.utf8(), nullable=False),
            pa.field("year", pa.int32()),
        ]
    ),
    "master_artists": pa.schema(
        [
            pa.field("master_id", pa.int32(), nullable=False),
            pa.field("artist_id", pa.int32()),
            pa.field("anv", pa.utf8(), nullable=False),
            pa.field("join", pa.utf8(), nullable=False),
            pa.field("name", pa.utf8(), nullable=False),
        ]
    ),
    "master_videos": pa.schema(
        [
            pa.field("master_id", pa.int32(), nullable=False),
            pa.field("description", pa.utf8(), nullable=False),
            pa.field("duration", pa.int32()),
            pa.field("embed", pa.bool_()),
            pa.field("src", pa.utf8(), nullable=False),
            pa.field("title", pa.utf8(), nullable=False),
        ]
    ),
}

TABLE_WEIGHTS = {
    "masters": 0.60,
    "master_artists": 0.20,
    "master_videos": 0.20,
}

# ------------------------------------------------------------------------------------------------------------------------
# XML constants
# ------------------------------------------------------------------------------------------------------------------------

_MASTER_END = b"</master>\n"
_XML_HEADER = b"<?xml version='1.0' encoding='UTF-8'?>\n<masters>\n"
_XML_FOOTER = b"\n</masters>"

# ------------------------------------------------------------------------------------------------------------------------
# XML splitting
# ------------------------------------------------------------------------------------------------------------------------

find_split_points: Callable[[str, int], list[tuple[int, int]]] = make_split_finder(
    b"<master ", _MASTER_END
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


def _append_master(
    cols: _Cols, elem: etree._Element, unknown: set[str] | None = None
) -> None:
    """Parse a <master> element and append all data to column accumulators."""
    id_text = elem.get("id")
    if not id_text:
        warnings.warn("skipping <master> with missing id", stacklevel=2)
        return
    master_id = int(id_text)

    data_quality = ""
    main_release = None
    notes = ""
    title = ""
    year = None
    genres = []
    styles = []

    for child in elem:
        tag = child.tag
        if tag == "artists":
            for artist_elem in child:
                if artist_elem.tag != "artist":
                    continue
                artist_id = None
                anv = ""
                join_text = ""
                aname = ""
                for ac in artist_elem:
                    at = ac.tag
                    if at == "id":
                        artist_id = int(ac.text) if ac.text else None
                    elif at == "anv":
                        anv = ac.text or ""
                    elif at == "join":
                        join_text = ac.text or ""
                    elif at == "name":
                        aname = ac.text or ""
                    elif unknown is not None:
                        unknown.add(f"artists/artist/{at}")
                row = cols["master_artists"]
                row["master_id"].append(master_id)
                row["artist_id"].append(artist_id)
                row["anv"].append(anv)
                row["join"].append(join_text)
                row["name"].append(aname)
        elif tag == "data_quality":
            data_quality = child.text or ""
        elif tag == "genres":
            genres = [g.text for g in child.findall("genre") if g.text]
        elif tag == "main_release":
            if child.text:
                main_release = int(child.text)
        elif tag == "notes":
            notes = child.text or ""
        elif tag == "styles":
            styles = [s.text for s in child.findall("style") if s.text]
        elif tag == "title":
            title = child.text or ""
        elif tag == "videos":
            for video in child:
                if video.tag != "video":
                    continue
                src = video.get("src") or ""
                dur_str = video.get("duration")
                duration = int(dur_str) if dur_str else None
                embed_str = video.get("embed")
                embed = embed_str == "true" if embed_str else None
                vdesc = ""
                vtitle = ""
                for vc in video:
                    if vc.tag == "description":
                        vdesc = vc.text or ""
                    elif vc.tag == "title":
                        vtitle = vc.text or ""
                    elif unknown is not None:
                        unknown.add(f"videos/video/{vc.tag}")
                row = cols["master_videos"]
                row["master_id"].append(master_id)
                row["description"].append(vdesc)
                row["duration"].append(duration)
                row["embed"].append(embed)
                row["src"].append(src)
                row["title"].append(vtitle)
        elif tag == "year":
            if child.text:
                year = int(child.text)
        elif unknown is not None:
            unknown.add(tag)

    r = cols["masters"]
    r["id"].append(master_id)
    r["data_quality"].append(data_quality)
    r["main_release"].append(main_release)
    r["notes"].append(notes)
    r["title"].append(title)
    r["year"].append(year)
    r["genres"].append(genres)
    r["styles"].append(styles)


# ------------------------------------------------------------------------------------------------------------------------
# Chunk worker
# ------------------------------------------------------------------------------------------------------------------------


def extract_chunk_to_ipc(args: ChunkArgs) -> dict[str, bytes]:
    """Worker: parse XML chunk -> 3 RecordBatches -> IPC bytes dict."""
    with open(args.file_path, "rb") as f:
        f.seek(args.start)
        data = f.read(args.end - args.start)

    xml_data = _XML_HEADER + data + _XML_FOOTER
    cols = _new_cols()
    unknown: set[str] | None = set() if args.strict else None

    for _, elem in etree.iterparse(BytesIO(xml_data), events=("end",), tag="master"):
        _append_master(cols, elem, unknown)
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]

    if unknown:
        for tag in sorted(unknown):
            warnings.warn(f"unhandled XML element <{tag}> in <master>", stacklevel=1)

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

MASTERS_ENTITY = EntityDef(
    name="masters",
    root_tag="master",
    table_order=TABLE_ORDER,
    schemas=SCHEMAS,
    table_weights=TABLE_WEIGHTS,
    extract_chunk_to_ipc=extract_chunk_to_ipc,
    find_split_points=find_split_points,
    fk_column="master_id",
)

register(MASTERS_ENTITY)
