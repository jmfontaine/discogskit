"""Entity registry for Discogs dump entity types.

Each entity module (artists, labels, masters, and releases) defines its Arrow
schemas, table weights and an ``append_record`` parser, then calls
``register()`` at import time.  Splitting and the chunk worker are shared
(``_split.py`` and ``_worker.py``) and driven by ``EntityDef``.  The bottom of
this file imports all entity modules to trigger registration — adding a new
entity only requires creating the module and adding an import here.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from discogskit.entities._split import find_split_points

if TYPE_CHECKING:
    from lxml import etree

# Column accumulators: table name -> column name -> values, one list per column. The value type
# follows each column's Arrow type, so it differs per column: hence Any.
Cols = dict[str, dict[str, list[Any]]]


@dataclass
class ChunkArgs:
    """Arguments passed to each parse worker for one XML chunk.

    Carries the entity name rather than the ``EntityDef`` so it stays cheap to pickle; the worker looks the entity up
    in the registry.
    """

    entity: str
    file_path: str
    start: int
    end: int
    strict: bool = False


@dataclass
class EntityDef:
    """Definition for one Discogs entity type.

    ``name`` is also the XML container element (``<artists>``) and ``root_tag`` the record element (``<artist>``).
    The first table in ``schemas`` is the root table, and its primary key column is always ``id``. ``record_id``
    returns a record element's id as written in the XML, or ``None`` when it has none.
    """

    name: str
    root_tag: str
    schemas: dict[str, pa.Schema]
    table_weights: dict[str, float]
    append_record: Callable[[Cols, etree._Element, set[str] | None], None]
    record_id: Callable[[etree._Element], str | None]
    fk_column: str | None = None

    @cached_property
    def table_order(self) -> list[str]:
        return list(self.schemas)

    def find_split_points(
        self, file_path: str, target_chunk_bytes: int
    ) -> list[tuple[int, int]]:
        return find_split_points(
            file_path, target_chunk_bytes, self.root_tag, self.name
        )


ENTITIES: dict[str, EntityDef] = {}


def register(entity: EntityDef) -> None:
    ENTITIES[entity.name] = entity


def get(name: str) -> EntityDef:
    return ENTITIES[name]


def detect_entity(filename: str) -> str:
    """Infer entity name from Discogs dump filename.

    Expected: discogs_YYYYMMDD_ENTITY.xml.gz
    """
    stem = filename
    for suffix in (".gz", ".xml"):
        stem = stem.removesuffix(suffix)
    entity = stem.rsplit("_", 1)[-1]
    if entity not in ENTITIES:
        raise ValueError(
            f"Cannot detect entity from '{filename}'. "
            f"Expected format: discogs_YYYYMMDD_ENTITY.xml.gz "
            f"(known entities: {', '.join(sorted(ENTITIES))})"
        )
    return entity


# Only these literals are booleans; any other value fails instead of being read as False (#95).
_BOOLEANS = {"false": False, "true": True}


def parse_bool(name: str, value: str | None) -> bool | None:
    """Parse the boolean attribute ``name``: ``None`` when it's missing, ``True`` or ``False`` for ``"true"`` or
    ``"false"``. Any other value, including an empty one, raises ``ValueError``."""
    if value is None:
        return None
    try:
        return _BOOLEANS[value]
    except KeyError:
        raise ValueError(
            f"invalid {name} {value!r}, expected 'true' or 'false'"
        ) from None


# Import entity modules to trigger registration.
from discogskit.entities import (
    artists as _artists,  # noqa: F401 — imported after ENTITIES dict for registration side-effects
)
from discogskit.entities import (
    labels as _labels,  # noqa: F401 — imported after ENTITIES dict for registration side-effects
)
from discogskit.entities import (
    masters as _masters,  # noqa: F401 — imported after ENTITIES dict for registration side-effects
)
from discogskit.entities import (
    releases as _releases,  # noqa: F401 — imported after ENTITIES dict for registration side-effects
)
