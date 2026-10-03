"""Masters entity definition: Arrow schemas and ``<master>`` record parsing.

The chunk worker that drives ``append_record`` is shared; see ``_worker.py``.
"""

from __future__ import annotations

import warnings

import pyarrow as pa
from lxml import etree

from discogskit.entities import Cols, EntityDef, register

# ------------------------------------------------------------------------------------------------------------------------
# Arrow schemas (the first table is the root table)
# ------------------------------------------------------------------------------------------------------------------------

SCHEMAS = {
    "masters": pa.schema(
        [
            pa.field("id", pa.int32(), nullable=False),
            pa.field("data_quality", pa.utf8()),
            pa.field("genres", pa.list_(pa.utf8())),
            pa.field("main_release", pa.int32()),
            pa.field("notes", pa.utf8()),
            pa.field("styles", pa.list_(pa.utf8())),
            pa.field("title", pa.utf8()),
            pa.field("year", pa.int32()),
        ]
    ),
    "master_artists": pa.schema(
        [
            pa.field("master_id", pa.int32(), nullable=False),
            pa.field("artist_id", pa.int32()),
            pa.field("anv", pa.utf8()),
            pa.field("join", pa.utf8()),
            pa.field("name", pa.utf8()),
        ]
    ),
    "master_videos": pa.schema(
        [
            pa.field("master_id", pa.int32(), nullable=False),
            pa.field("description", pa.utf8()),
            pa.field("duration", pa.int32()),
            pa.field("embed", pa.bool_()),
            pa.field("src", pa.utf8()),
            pa.field("title", pa.utf8()),
        ]
    ),
}

TABLE_WEIGHTS = {
    "masters": 0.60,
    "master_artists": 0.20,
    "master_videos": 0.20,
}


def append_record(
    cols: Cols, elem: etree._Element, unknown: set[str] | None = None
) -> None:
    """Parse a <master> element and append all data to column accumulators."""
    id_text = elem.get("id")
    if not id_text:
        warnings.warn("skipping <master> with missing id", stacklevel=2)
        return
    master_id = int(id_text)

    data_quality = None
    main_release = None
    notes = None
    title = None
    year = None
    genres = None
    styles = None

    for child in elem:
        tag = child.tag
        if tag == "artists":
            for artist_elem in child:
                if artist_elem.tag != "artist":
                    continue
                artist_id = None
                anv = None
                join_text = None
                aname = None
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
            genres = [g.text or "" for g in child.findall("genre")]
        elif tag == "main_release":
            if child.text:
                main_release = int(child.text)
        elif tag == "notes":
            notes = child.text or ""
        elif tag == "styles":
            styles = [s.text or "" for s in child.findall("style")]
        elif tag == "title":
            title = child.text or ""
        elif tag == "videos":
            for video in child:
                if video.tag != "video":
                    continue
                src = video.get("src")
                dur_str = video.get("duration")
                duration = int(dur_str) if dur_str else None
                embed_str = video.get("embed")
                embed = embed_str == "true" if embed_str else None
                vdesc = None
                vtitle = None
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


register(
    EntityDef(
        append_record=append_record,
        fk_column="master_id",
        name="masters",
        root_tag="master",
        schemas=SCHEMAS,
        table_weights=TABLE_WEIGHTS,
    )
)
