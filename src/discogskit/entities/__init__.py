"""Entity registry for Discogs dump entity types.

Each entity module (artists, labels, masters, and releases) defines its own
Arrow schemas, XML parsing logic, and worker function, then calls
``register()`` at import time.  The bottom of this file imports all entity
modules to trigger registration — adding a new entity only requires
creating the module and adding an import here.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import pyarrow as pa


@dataclass
class ChunkArgs:
    """Arguments passed to each parse worker for one XML chunk."""

    file_path: str
    start: int
    end: int
    strict: bool = False


@dataclass
class EntityDef:
    """Definition for one Discogs entity type."""

    name: str
    root_tag: str
    table_order: list[str]
    schemas: dict[str, pa.Schema]
    table_weights: dict[str, float]
    extract_chunk_to_ipc: Callable
    find_split_points: Callable
    pk_column: str = "id"
    fk_column: str | None = None


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
        if stem.endswith(suffix):
            stem = stem[: -len(suffix)]
    entity = stem.rsplit("_", 1)[-1]
    if entity not in ENTITIES:
        raise ValueError(
            f"Cannot detect entity from '{filename}'. "
            f"Expected format: discogs_YYYYMMDD_ENTITY.xml.gz "
            f"(known entities: {', '.join(sorted(ENTITIES))})"
        )
    return entity


# Import entity modules to trigger registration.
from discogskit.entities import artists as _artists  # noqa: E402, F401 — imported after ENTITIES dict for registration side-effects
from discogskit.entities import labels as _labels  # noqa: E402, F401 — imported after ENTITIES dict for registration side-effects
from discogskit.entities import masters as _masters  # noqa: E402, F401 — imported after ENTITIES dict for registration side-effects
from discogskit.entities import releases as _releases  # noqa: E402, F401 — imported after ENTITIES dict for registration side-effects
