"""Labels entity definition: Arrow schemas and ``<label>`` record parsing.

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
    id_text = elem.findtext("id")
    if not id_text:
        warnings.warn("skipping <label> with missing id", stacklevel=2)
        return
    label_id = int(id_text)

    contact_info = None
    data_quality = None
    name = None
    parent_label_id = None
    parent_label_name = None
    profile = None
    urls = None

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
            urls = [u.text or "" for u in child.findall("url")]
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


register(
    EntityDef(
        append_record=append_record,
        fk_column="label_id",
        name="labels",
        root_tag="label",
        schemas=SCHEMAS,
        table_weights=TABLE_WEIGHTS,
    )
)
