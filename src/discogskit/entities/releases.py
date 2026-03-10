"""Releases entity definition: Arrow schemas, XML parsing, IPC worker.

The XML has deeply nested structure (release → tracks → artists). Denormalizing into one flat table would create massive
row duplication. 12 normalized tables with release_id foreign keys keep storage compact and enable efficient joins.
The schema mirrors the XML structure.
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
_ArtistRec = tuple[int | None, str, str, str]
_ExtraArtistRec = tuple[int | None, str, str, str, str]

# ------------------------------------------------------------------------------------------------------------------------
# Constants
# ------------------------------------------------------------------------------------------------------------------------

TABLE_ORDER = [
    "releases",
    "release_artists",
    "release_extraartists",
    "release_labels",
    "release_series",
    "release_formats",
    "release_identifiers",
    "release_videos",
    "release_companies",
    "release_tracks",
    "release_track_artists",
    "release_track_extraartists",
]

# ------------------------------------------------------------------------------------------------------------------------
# Arrow schemas (single source of truth for column definitions)
#
# These schemas serve two purposes:
#   1. RecordBatch construction: pa.RecordBatch.from_pydict(cols, schema=...) enforces types and catches mismatches
#      at serialization time.
#   2. DDL generation: the PostgreSQL writer derives CREATE TABLE DDL from these schemas so column types stay in sync
#      automatically.
#
# Arrow types map to PostgreSQL types:
#   pa.int32() → INTEGER    pa.bool_() → BOOLEAN    pa.utf8() → TEXT    pa.list_(utf8()) → TEXT[]
# ------------------------------------------------------------------------------------------------------------------------

SCHEMAS = {
    "releases": pa.schema(
        [
            pa.field("id", pa.int32(), nullable=False),
            pa.field("country", pa.utf8(), nullable=False),
            pa.field("data_quality", pa.utf8(), nullable=False),
            pa.field("genres", pa.list_(pa.utf8()), nullable=False),
            pa.field("is_main_release", pa.bool_()),
            pa.field("master_id", pa.int32()),
            pa.field("notes", pa.utf8(), nullable=False),
            pa.field("released", pa.utf8(), nullable=False),
            pa.field("status", pa.utf8(), nullable=False),
            pa.field("styles", pa.list_(pa.utf8()), nullable=False),
            pa.field("title", pa.utf8(), nullable=False),
        ]
    ),
    "release_artists": pa.schema(
        [
            pa.field("release_id", pa.int32(), nullable=False),
            pa.field("artist_id", pa.int32()),
            pa.field("anv", pa.utf8(), nullable=False),
            pa.field("join", pa.utf8(), nullable=False),
            pa.field("name", pa.utf8(), nullable=False),
        ]
    ),
    "release_extraartists": pa.schema(
        [
            pa.field("release_id", pa.int32(), nullable=False),
            pa.field("artist_id", pa.int32()),
            pa.field("anv", pa.utf8(), nullable=False),
            pa.field("name", pa.utf8(), nullable=False),
            pa.field("role", pa.utf8(), nullable=False),
            pa.field("tracks", pa.utf8(), nullable=False),
        ]
    ),
    "release_labels": pa.schema(
        [
            pa.field("release_id", pa.int32(), nullable=False),
            pa.field("label_id", pa.int32()),
            pa.field("catno", pa.utf8(), nullable=False),
            pa.field("name", pa.utf8(), nullable=False),
        ]
    ),
    "release_series": pa.schema(
        [
            pa.field("release_id", pa.int32(), nullable=False),
            pa.field("series_id", pa.int32()),
            pa.field("catno", pa.utf8(), nullable=False),
            pa.field("name", pa.utf8(), nullable=False),
        ]
    ),
    "release_formats": pa.schema(
        [
            pa.field("release_id", pa.int32(), nullable=False),
            pa.field("descriptions", pa.list_(pa.utf8()), nullable=False),
            pa.field("name", pa.utf8(), nullable=False),
            pa.field("qty", pa.utf8(), nullable=False),
            pa.field("text", pa.utf8(), nullable=False),
        ]
    ),
    "release_identifiers": pa.schema(
        [
            pa.field("release_id", pa.int32(), nullable=False),
            pa.field("description", pa.utf8(), nullable=False),
            pa.field("type", pa.utf8(), nullable=False),
            pa.field("value", pa.utf8(), nullable=False),
        ]
    ),
    "release_videos": pa.schema(
        [
            pa.field("release_id", pa.int32(), nullable=False),
            pa.field("description", pa.utf8(), nullable=False),
            pa.field("duration", pa.int32(), nullable=False),
            pa.field("embed", pa.bool_(), nullable=False),
            pa.field("src", pa.utf8(), nullable=False),
            pa.field("title", pa.utf8(), nullable=False),
        ]
    ),
    "release_companies": pa.schema(
        [
            pa.field("release_id", pa.int32(), nullable=False),
            pa.field("company_id", pa.int32()),
            pa.field("catno", pa.utf8(), nullable=False),
            pa.field("entity_type_name", pa.utf8(), nullable=False),
            pa.field("entity_type", pa.utf8(), nullable=False),
            pa.field("name", pa.utf8(), nullable=False),
        ]
    ),
    "release_tracks": pa.schema(
        [
            pa.field("release_id", pa.int32(), nullable=False),
            pa.field("track_idx", pa.int32(), nullable=False),
            pa.field("parent_idx", pa.int32()),
            pa.field("duration", pa.utf8(), nullable=False),
            pa.field("position", pa.utf8(), nullable=False),
            pa.field("title", pa.utf8(), nullable=False),
        ]
    ),
    "release_track_artists": pa.schema(
        [
            pa.field("release_id", pa.int32(), nullable=False),
            pa.field("track_idx", pa.int32(), nullable=False),
            pa.field("artist_id", pa.int32()),
            pa.field("anv", pa.utf8(), nullable=False),
            pa.field("join", pa.utf8(), nullable=False),
            pa.field("name", pa.utf8(), nullable=False),
        ]
    ),
    "release_track_extraartists": pa.schema(
        [
            pa.field("release_id", pa.int32(), nullable=False),
            pa.field("track_idx", pa.int32(), nullable=False),
            pa.field("artist_id", pa.int32()),
            pa.field("anv", pa.utf8(), nullable=False),
            pa.field("name", pa.utf8(), nullable=False),
            pa.field("role", pa.utf8(), nullable=False),
        ]
    ),
}

# Relative flush time per table (%), profiled on the full 19M releases dataset. Used by _split_table_groups in
# the PostgreSQL writer to balance work across write workers so no single group becomes the bottleneck.
# If the schema changes, re-profile with --profile and update these weights.
TABLE_WEIGHTS = {
    "release_tracks": 22.3,
    "release_extraartists": 15.0,
    "release_track_extraartists": 13.5,
    "releases": 13.1,
    "release_track_artists": 7.3,
    "release_companies": 6.0,
    "release_artists": 5.2,
    "release_identifiers": 4.6,
    "release_formats": 4.6,
    "release_videos": 4.1,
    "release_labels": 3.7,
    "release_series": 0.5,
}

# ------------------------------------------------------------------------------------------------------------------------
# XML constants
# ------------------------------------------------------------------------------------------------------------------------

_RELEASE_END = b"</release>\n"
_XML_HEADER = b"<?xml version='1.0' encoding='UTF-8'?>\n<releases>\n"
_XML_FOOTER = b"\n</releases>"

# ------------------------------------------------------------------------------------------------------------------------
# XML splitting
# ------------------------------------------------------------------------------------------------------------------------

find_split_points: Callable[[str, int], list[tuple[int, int]]] = make_split_finder(
    b"<release ", _RELEASE_END
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
#
# Workers build up columns as Python lists (one list per column per table), then convert to Arrow RecordBatches at the
# end of each chunk. This is faster than appending to Arrow arrays incrementally because:
#   - Python list.append is O(1) amortized
#   - RecordBatch.from_pydict does a single bulk conversion
#   - Avoids intermediate Arrow allocations per row
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
# XML parsing helpers
#
# Uses manual iteration over child elements (``for ch in elem``) rather than XPath/find() because it's faster for
# elements with many children: we visit each child exactly once instead of scanning the child list per query.
# ------------------------------------------------------------------------------------------------------------------------


def _parse_artist_children(
    parent_elem: etree._Element, unknown: set[str] | None = None
) -> list[_ArtistRec]:
    result = []
    for a in parent_elem:
        if a.tag != "artist":
            continue
        aid = None
        name = None
        anv = ""
        join_text = ""
        for ch in a:
            t = ch.tag
            if t == "id":
                aid = int(ch.text) if ch.text else None
            elif t == "name":
                name = ch.text
            elif t == "anv":
                anv = ch.text or ""
            elif t == "join":
                join_text = ch.text or ""
            elif unknown is not None:
                unknown.add(f"artist/{t}")
        if name:
            result.append((aid, name, anv, join_text))
    return result


def _parse_extraartist_children(
    parent_elem: etree._Element, unknown: set[str] | None = None
) -> list[_ExtraArtistRec]:
    result = []
    for a in parent_elem:
        if a.tag != "artist":
            continue
        aid = None
        name = None
        anv = ""
        role = ""
        tracks_text = ""
        for ch in a:
            t = ch.tag
            if t == "id":
                aid = int(ch.text) if ch.text else None
            elif t == "name":
                name = ch.text
            elif t == "anv":
                anv = ch.text or ""
            elif t == "role":
                role = ch.text or ""
            elif t == "tracks":
                tracks_text = ch.text or ""
            elif unknown is not None:
                unknown.add(f"extraartist/{t}")
        if name:
            result.append((aid, name, anv, role, tracks_text))
    return result


def _parse_single_track(
    track_elem: etree._Element,
    unknown: set[str] | None = None,
) -> tuple[
    str, str, str, list[_ArtistRec], list[_ExtraArtistRec], etree._Element | None
]:
    position = ""
    title = ""
    duration = ""
    artists = []
    extraartists = []
    sub_tracks_elem = None
    for ch in track_elem:
        tag = ch.tag
        if tag == "position":
            position = ch.text or ""
        elif tag == "title":
            title = ch.text or ""
        elif tag == "duration":
            duration = ch.text or ""
        elif tag == "artists":
            artists = _parse_artist_children(ch, unknown)
        elif tag == "extraartists":
            extraartists = _parse_extraartist_children(ch, unknown)
        elif tag == "sub_tracks":
            sub_tracks_elem = ch
        elif unknown is not None:
            unknown.add(f"track/{tag}")
    return position, title, duration, artists, extraartists, sub_tracks_elem


def _append_release(
    cols: _Cols, elem: etree._Element, unknown: set[str] | None = None
) -> None:
    """Parse a <release> element and append all data to column accumulators."""
    id_text = elem.get("id")
    if not id_text:
        warnings.warn("skipping <release> with missing id", stacklevel=2)
        return
    rid = int(id_text)
    status = elem.get("status") or ""

    title = ""
    country = ""
    released = ""
    notes = ""
    data_quality = ""
    master_id = None
    is_main_release = None
    genres = []
    styles = []

    r_artists = []
    r_extraartists = []
    r_labels = []
    r_series = []
    r_formats = []
    r_identifiers = []
    r_videos = []
    r_companies = []
    r_tracks = []

    for child in elem:
        tag = child.tag
        if tag == "title":
            title = child.text or ""
        elif tag == "country":
            country = child.text or ""
        elif tag == "released":
            released = child.text or ""
        elif tag == "notes":
            notes = child.text or ""
        elif tag == "data_quality":
            data_quality = child.text or ""
        elif tag == "master_id":
            master_id = int(child.text) if child.text else None
            attr = child.get("is_main_release")
            if attr is not None:
                is_main_release = attr == "true"
        elif tag == "genres":
            genres = [g.text for g in child.findall("genre") if g.text]
        elif tag == "styles":
            styles = [s.text for s in child.findall("style") if s.text]
        elif tag == "artists":
            r_artists = _parse_artist_children(child, unknown)
        elif tag == "extraartists":
            r_extraartists = _parse_extraartist_children(child, unknown)
        elif tag == "labels":
            for la in child.findall("label"):
                la_id_str = la.get("id")
                r_labels.append(
                    (
                        int(la_id_str) if la_id_str else None,
                        la.get("name") or "",
                        la.get("catno") or "",
                    )
                )
        elif tag == "series":
            for sr in child.findall("series"):
                sr_id_str = sr.get("id")
                r_series.append(
                    (
                        int(sr_id_str) if sr_id_str else None,
                        sr.get("name") or "",
                        sr.get("catno") or "",
                    )
                )
        elif tag == "formats":
            for ff in child.findall("format"):
                descs = [
                    d.text for d in ff.findall("descriptions/description") if d.text
                ]
                r_formats.append(
                    (
                        ff.get("name") or "",
                        ff.get("qty") or "",
                        ff.get("text") or "",
                        descs,
                    )
                )
        elif tag == "identifiers":
            for ident in child.findall("identifier"):
                r_identifiers.append(
                    (
                        ident.get("type") or "",
                        ident.get("description") or "",
                        ident.get("value") or "",
                    )
                )
        elif tag == "videos":
            for vid in child.findall("video"):
                dur_str = vid.get("duration") or "0"
                try:
                    vid_dur = int(dur_str)
                except ValueError:
                    vid_dur = 0
                vid_title = ""
                vid_desc = ""
                for vch in vid:
                    if vch.tag == "title":
                        vid_title = vch.text or ""
                    elif vch.tag == "description":
                        vid_desc = vch.text or ""
                    elif unknown is not None:
                        unknown.add(f"videos/video/{vch.tag}")
                r_videos.append(
                    (
                        vid.get("src") or "",
                        vid_dur,
                        vid_title,
                        vid_desc,
                        vid.get("embed") == "true",
                    )
                )
        elif tag == "companies":
            for co in child:
                if co.tag != "company":
                    continue
                cid = None
                cname = ""
                ccatno = ""
                cetype = ""
                cetype_name = ""
                for cch in co:
                    ct = cch.tag
                    if ct == "id":
                        cid = int(cch.text) if cch.text else None
                    elif ct == "name":
                        cname = cch.text or ""
                    elif ct == "catno":
                        ccatno = cch.text or ""
                    elif ct == "entity_type":
                        cetype = cch.text or ""
                    elif ct == "entity_type_name":
                        cetype_name = cch.text or ""
                    elif unknown is not None:
                        unknown.add(f"companies/company/{ct}")
                r_companies.append((cid, cname, ccatno, cetype, cetype_name))
        elif tag == "tracklist":
            # Tracks use a flat index (track_idx) with an optional parent_idx for sub-tracks. This avoids nested
            # arrays in PG while preserving the hierarchical structure for queries.
            track_idx = 0
            for tr in child:
                if tr.tag != "track":
                    continue
                pos, ttl, dur, t_art, t_ea, sub_elem = _parse_single_track(tr, unknown)
                parent = track_idx
                r_tracks.append((track_idx, None, pos, ttl, dur, t_art, t_ea))
                track_idx += 1
                if sub_elem is not None:
                    for sub in sub_elem:
                        if sub.tag != "track":
                            continue
                        s_pos, s_ttl, s_dur, s_art, s_ea, _ = _parse_single_track(
                            sub, unknown
                        )
                        r_tracks.append(
                            (track_idx, parent, s_pos, s_ttl, s_dur, s_art, s_ea)
                        )
                        track_idx += 1
        elif unknown is not None:
            unknown.add(tag)

    # -- Append to accumulators --

    r = cols["releases"]
    r["id"].append(rid)
    r["status"].append(status)
    r["title"].append(title)
    r["country"].append(country)
    r["released"].append(released)
    r["notes"].append(notes)
    r["data_quality"].append(data_quality)
    r["master_id"].append(master_id)
    r["is_main_release"].append(is_main_release)
    r["genres"].append(genres)
    r["styles"].append(styles)

    a = cols["release_artists"]
    for aid, name, anv, join_text in r_artists:
        a["release_id"].append(rid)
        a["artist_id"].append(aid)
        a["name"].append(name)
        a["anv"].append(anv)
        a["join"].append(join_text)

    ea = cols["release_extraartists"]
    for aid, name, anv, role, tracks_text in r_extraartists:
        ea["release_id"].append(rid)
        ea["artist_id"].append(aid)
        ea["name"].append(name)
        ea["anv"].append(anv)
        ea["role"].append(role)
        ea["tracks"].append(tracks_text)

    lb = cols["release_labels"]
    for lid, lname, lcatno in r_labels:
        lb["release_id"].append(rid)
        lb["label_id"].append(lid)
        lb["name"].append(lname)
        lb["catno"].append(lcatno)

    sr = cols["release_series"]
    for sid, sname, scatno in r_series:
        sr["release_id"].append(rid)
        sr["series_id"].append(sid)
        sr["name"].append(sname)
        sr["catno"].append(scatno)

    fm = cols["release_formats"]
    for fname, fqty, ftext, fdescs in r_formats:
        fm["release_id"].append(rid)
        fm["name"].append(fname)
        fm["qty"].append(fqty)
        fm["text"].append(ftext)
        fm["descriptions"].append(fdescs)

    ident = cols["release_identifiers"]
    for itype, idesc, ival in r_identifiers:
        ident["release_id"].append(rid)
        ident["type"].append(itype)
        ident["description"].append(idesc)
        ident["value"].append(ival)

    v = cols["release_videos"]
    for vsrc, vdur, vtitle, vdesc, vembed in r_videos:
        v["release_id"].append(rid)
        v["src"].append(vsrc)
        v["duration"].append(vdur)
        v["title"].append(vtitle)
        v["description"].append(vdesc)
        v["embed"].append(vembed)

    co = cols["release_companies"]
    for cid, cname, ccatno, cetype, cetype_name in r_companies:
        co["release_id"].append(rid)
        co["company_id"].append(cid)
        co["name"].append(cname)
        co["catno"].append(ccatno)
        co["entity_type"].append(cetype)
        co["entity_type_name"].append(cetype_name)

    t = cols["release_tracks"]
    ta = cols["release_track_artists"]
    te = cols["release_track_extraartists"]
    for tidx, pidx, tpos, ttitle, tdur, t_artists, t_extraartists in r_tracks:
        t["release_id"].append(rid)
        t["track_idx"].append(tidx)
        t["parent_idx"].append(pidx)
        t["position"].append(tpos)
        t["title"].append(ttitle)
        t["duration"].append(tdur)

        for aid, name, anv, join_text in t_artists:
            ta["release_id"].append(rid)
            ta["track_idx"].append(tidx)
            ta["artist_id"].append(aid)
            ta["name"].append(name)
            ta["anv"].append(anv)
            ta["join"].append(join_text)

        for aid, name, anv, role, _tracks in t_extraartists:
            te["release_id"].append(rid)
            te["track_idx"].append(tidx)
            te["artist_id"].append(aid)
            te["name"].append(name)
            te["anv"].append(anv)
            te["role"].append(role)


# ------------------------------------------------------------------------------------------------------------------------
# Chunk worker (module-level, picklable for multiprocessing)
#
# This function runs in a separate PROCESS (via multiprocessing.Pool). It must be a module-level function (not a
# closure or method) so that pickle can serialize it for the worker process.
# ------------------------------------------------------------------------------------------------------------------------


def extract_chunk_to_ipc(args: ChunkArgs) -> dict[str, bytes]:
    """Worker: parse XML chunk -> 12 normalized RecordBatches -> IPC bytes dict.

    Reads a byte range from the decompressed XML, wraps it in a valid XML envelope, parses with lxml iterparse, and
    returns serialized Arrow IPC.
    """
    with open(args.file_path, "rb") as f:
        f.seek(args.start)
        data = f.read(args.end - args.start)

    xml_data = _XML_HEADER + data + _XML_FOOTER
    cols = _new_cols()
    unknown: set[str] | None = set() if args.strict else None

    for _, elem in etree.iterparse(BytesIO(xml_data), events=("end",), tag="release"):
        _append_release(cols, elem, unknown)
        # Standard lxml memory optimization for iterparse: free each element after processing to prevent the entire
        # tree from accumulating. Without this, a 256 MB chunk would build a multi-GB tree in memory.
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]

    if unknown:
        for tag in sorted(unknown):
            warnings.warn(f"unhandled XML element <{tag}> in <release>", stacklevel=1)

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

RELEASES_ENTITY = EntityDef(
    name="releases",
    root_tag="release",
    table_order=TABLE_ORDER,
    schemas=SCHEMAS,
    table_weights=TABLE_WEIGHTS,
    extract_chunk_to_ipc=extract_chunk_to_ipc,
    find_split_points=find_split_points,
    fk_column="release_id",
)

register(RELEASES_ENTITY)
