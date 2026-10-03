"""Artists entity definition: Arrow schemas and ``<artist>`` record parsing.

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
    "artists": pa.schema(
        [
            pa.field("id", pa.int32(), nullable=False),
            pa.field("data_quality", pa.utf8()),
            pa.field("name", pa.utf8()),
            pa.field("namevariations", pa.list_(pa.utf8())),
            pa.field("profile", pa.utf8()),
            pa.field("realname", pa.utf8()),
            pa.field("urls", pa.list_(pa.utf8())),
        ]
    ),
    "artist_aliases": pa.schema(
        [
            pa.field("artist_id", pa.int32(), nullable=False),
            pa.field("alias_id", pa.int32()),
            pa.field("name", pa.utf8()),
        ]
    ),
    "artist_groups": pa.schema(
        [
            pa.field("artist_id", pa.int32(), nullable=False),
            pa.field("group_id", pa.int32()),
            pa.field("name", pa.utf8()),
        ]
    ),
    "artist_members": pa.schema(
        [
            pa.field("artist_id", pa.int32(), nullable=False),
            pa.field("member_id", pa.int32()),
            pa.field("name", pa.utf8()),
        ]
    ),
}

TABLE_WEIGHTS = {
    "artists": 0.50,
    "artist_aliases": 0.20,
    "artist_groups": 0.15,
    "artist_members": 0.15,
}


def _parse_refs(cols: Cols, table: str, artist_id: int, parent: etree._Element) -> None:
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


def append_record(
    cols: Cols, elem: etree._Element, unknown: set[str] | None = None
) -> None:
    """Parse an <artist> element and append all data to column accumulators."""
    id_text = elem.findtext("id")
    if not id_text:
        warnings.warn("skipping <artist> with missing id", stacklevel=2)
        return
    artist_id = int(id_text)

    data_quality = None
    name = None
    name_variations = None
    profile = None
    real_name = None
    urls = None

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
            name_variations = [n.text or "" for n in child.findall("name")]
        elif tag == "profile":
            profile = child.text or ""
        elif tag == "realname":
            real_name = child.text or ""
        elif tag == "urls":
            urls = [u.text or "" for u in child.findall("url")]
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


register(
    EntityDef(
        append_record=append_record,
        fk_column="artist_id",
        name="artists",
        root_tag="artist",
        schemas=SCHEMAS,
        table_weights=TABLE_WEIGHTS,
    )
)
