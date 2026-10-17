"""Labels entity definition: Arrow schemas and ``<label>`` record parsing.

The chunk worker that drives ``append_record`` is shared; see ``_worker.py``.
"""

from __future__ import annotations

import pyarrow as pa
from lxml import etree

from discogskit.entities._def import (
    ID_ATTR,
    Cols,
    EntityDef,
    check_attrs,
    check_leaf,
    check_text,
    child_elements,
    leaf_text,
    leaf_texts,
    parse_int,
    second_copy,
)

# ------------------------------------------------------------------------------------------------------------------------
# Arrow schemas (the first table is the root table)
# ------------------------------------------------------------------------------------------------------------------------

SCHEMAS = {
    "labels": pa.schema(
        [
            pa.field("id", pa.int32(), nullable=False),
            pa.field("contactinfo", pa.utf8()),
            pa.field("data_quality", pa.utf8()),
            pa.field("name", pa.utf8()),
            pa.field("parent_label_id", pa.int32()),
            pa.field("parent_label_name", pa.utf8()),
            pa.field("profile", pa.utf8()),
            pa.field("urls", pa.list_(pa.utf8())),
        ]
    ),
    "label_sublabels": pa.schema(
        [
            pa.field("label_id", pa.int32(), nullable=False),
            pa.field("sublabel_id", pa.int32()),
            pa.field("name", pa.utf8()),
        ]
    ),
}

TABLE_WEIGHTS = {
    "labels": 0.70,
    "label_sublabels": 0.30,
}


def append_record(
    cols: Cols, elem: etree._Element, unknown: set[str] | None = None
) -> None:
    """Parse a <label> element and append all data to column accumulators."""
    id_text = record_id(elem)
    if not id_text:
        raise ValueError("missing or empty id")
    label_id = parse_int("id", id_text)
    if unknown is not None:
        # <label> records carry no attributes.
        check_attrs(elem, "", unknown)
        check_text(elem, "", unknown)

    contact_info = None
    data_quality = None
    id_seen = False
    name = None
    parent_label_id = None
    parent_label_name = None
    profile = None
    urls = None

    for child in elem:
        tag = child.tag
        if tag == "id":
            # Already extracted via findtext above, which reads the first <id>.
            if id_seen:
                raise second_copy(child)
            id_seen = True
            if unknown is not None:
                check_leaf(child, "", unknown)
        elif tag == "contactinfo":
            if contact_info is not None:
                raise second_copy(child)
            contact_info = leaf_text(child, "", unknown)
        elif tag == "data_quality":
            if data_quality is not None:
                raise second_copy(child)
            data_quality = leaf_text(child, "", unknown)
        elif tag == "name":
            if name is not None:
                raise second_copy(child)
            name = leaf_text(child, "", unknown)
        elif tag == "parentLabel":
            if parent_label_name is not None:
                raise second_copy(child)
            parent_label_id = parse_int("parent label id", child.get("id"))
            parent_label_name = child.text or ""
            if unknown is not None:
                check_leaf(child, "", unknown, ID_ATTR)
        elif tag == "profile":
            if profile is not None:
                raise second_copy(child)
            profile = leaf_text(child, "", unknown)
        elif tag == "sublabels":
            for sub in child_elements(child, "label", "", unknown):
                sub_id = parse_int("sublabel id", sub.get("id"))
                sub_name = sub.text or ""
                if unknown is not None:
                    check_leaf(sub, "sublabels/", unknown, ID_ATTR)
                row = cols["label_sublabels"]
                row["label_id"].append(label_id)
                row["sublabel_id"].append(sub_id)
                row["name"].append(sub_name)
        elif tag == "urls":
            if urls is not None:
                raise second_copy(child)
            urls = leaf_texts(child, "url", "", unknown)
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


def record_id(elem: etree._Element) -> str | None:
    """Return a <label> element's id as written in the XML, or None when it has none."""
    return elem.findtext("id")


ENTITY = EntityDef(
    append_record=append_record,
    fk_column="label_id",
    name="labels",
    record_id=record_id,
    root_tag="label",
    schemas=SCHEMAS,
    table_weights=TABLE_WEIGHTS,
)
