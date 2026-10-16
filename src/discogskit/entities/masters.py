"""Masters entity definition: Arrow schemas and ``<master>`` record parsing.

The chunk worker that drives ``append_record`` is shared; see ``_worker.py``.
"""

from __future__ import annotations

import pyarrow as pa
from lxml import etree

from discogskit.entities._def import (
    Cols,
    EntityDef,
    check_leaf,
    child_elements,
    leaf_text,
    leaf_texts,
    parse_bool,
)

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
    id_text = record_id(elem)
    if not id_text:
        raise ValueError("missing or empty id")
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
            for artist_elem in child_elements(child, "artist", "", unknown):
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
                    else:
                        if unknown is not None:
                            unknown.add(f"artists/artist/{at}")
                        continue
                    # Every child handled above is text-only.
                    if unknown is not None:
                        check_leaf(ac, "artists/artist/", unknown)
                row = cols["master_artists"]
                row["master_id"].append(master_id)
                row["artist_id"].append(artist_id)
                row["anv"].append(anv)
                row["join"].append(join_text)
                row["name"].append(aname)
        elif tag == "data_quality":
            data_quality = leaf_text(child, "", unknown)
        elif tag == "genres":
            genres = leaf_texts(child, "genre", "", unknown)
        elif tag == "main_release":
            main_release_text = leaf_text(child, "", unknown)
            if main_release_text:
                main_release = int(main_release_text)
        elif tag == "notes":
            notes = leaf_text(child, "", unknown)
        elif tag == "styles":
            styles = leaf_texts(child, "style", "", unknown)
        elif tag == "title":
            title = leaf_text(child, "", unknown)
        elif tag == "videos":
            for video in child_elements(child, "video", "", unknown):
                src = video.get("src")
                dur_str = video.get("duration")
                duration = None if dur_str is None else int(dur_str)
                embed = parse_bool("embed", video.get("embed"))
                vdesc = None
                vtitle = None
                for vc in video:
                    if vc.tag == "description":
                        vdesc = vc.text or ""
                    elif vc.tag == "title":
                        vtitle = vc.text or ""
                    else:
                        if unknown is not None:
                            unknown.add(f"videos/video/{vc.tag}")
                        continue
                    # Every child handled above is text-only.
                    if unknown is not None:
                        check_leaf(vc, "videos/video/", unknown)
                row = cols["master_videos"]
                row["master_id"].append(master_id)
                row["description"].append(vdesc)
                row["duration"].append(duration)
                row["embed"].append(embed)
                row["src"].append(src)
                row["title"].append(vtitle)
        elif tag == "year":
            year_text = leaf_text(child, "", unknown)
            if year_text:
                year = int(year_text)
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


def record_id(elem: etree._Element) -> str | None:
    """Return a <master> element's id as written in the XML, or None when it has none."""
    return elem.get("id")


ENTITY = EntityDef(
    append_record=append_record,
    fk_column="master_id",
    name="masters",
    record_id=record_id,
    root_tag="master",
    schemas=SCHEMAS,
    table_weights=TABLE_WEIGHTS,
)
