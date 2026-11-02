"""Masters entity definition: Arrow schemas and ``<master>`` record parsing.

The chunk worker that drives ``append_record`` is shared; see ``_worker.py``.
"""

from __future__ import annotations

import pyarrow as pa
from lxml import etree

from discogskit.entities._def import (
    ID_ATTR,
    ArtistRec,
    Cols,
    EntityDef,
    VideoRec,
    check_attrs,
    check_text,
    child_elements,
    leaf_text,
    leaf_texts,
    parse_artist_children,
    parse_int,
    parse_video,
    second_copy,
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
    master_id = parse_int("id", id_text)
    if unknown is not None:
        check_attrs(elem, "", unknown, ID_ATTR)
        check_text(elem, "", unknown)

    data_quality = None
    main_release_text = None
    notes = None
    title = None
    year_text = None
    genres = None
    styles = None

    # The artists container stays None until read, so a second copy is told apart from an empty first.
    m_artists: list[ArtistRec] | None = None
    m_videos: list[VideoRec] = []

    for child in elem:
        tag = child.tag
        if tag == "artists":
            if m_artists is not None:
                raise second_copy(child)
            m_artists = parse_artist_children(child, "", unknown)
        elif tag == "data_quality":
            if data_quality is not None:
                raise second_copy(child)
            data_quality = leaf_text(child, "", unknown)
        elif tag == "genres":
            if genres is not None:
                raise second_copy(child)
            genres = leaf_texts(child, "genre", "", unknown)
        elif tag == "main_release":
            if main_release_text is not None:
                raise second_copy(child)
            main_release_text = leaf_text(child, "", unknown)
        elif tag == "notes":
            if notes is not None:
                raise second_copy(child)
            notes = leaf_text(child, "", unknown)
        elif tag == "styles":
            if styles is not None:
                raise second_copy(child)
            styles = leaf_texts(child, "style", "", unknown)
        elif tag == "title":
            if title is not None:
                raise second_copy(child)
            title = leaf_text(child, "", unknown)
        elif tag == "videos":
            for video in child_elements(child, "video", "", unknown):
                m_videos.append(parse_video(video, "", unknown))
        elif tag == "year":
            if year_text is not None:
                raise second_copy(child)
            year_text = leaf_text(child, "", unknown)
        elif unknown is not None:
            unknown.add(tag)

    # Parsed after the loop, as an artist credit's <id> is, so a second copy fails first, even after an empty one.
    main_release = parse_int("main release", main_release_text)
    year = parse_int("year", year_text)
    row = cols["master_artists"]
    for artist_id, name, anv, join_text in m_artists or ():
        row["master_id"].append(master_id)
        row["artist_id"].append(artist_id)
        row["anv"].append(anv)
        row["join"].append(join_text)
        row["name"].append(name)
    row = cols["master_videos"]
    for src, duration, vtitle, description, embed in m_videos:
        row["master_id"].append(master_id)
        row["description"].append(description)
        row["duration"].append(duration)
        row["embed"].append(embed)
        row["src"].append(src)
        row["title"].append(vtitle)
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
