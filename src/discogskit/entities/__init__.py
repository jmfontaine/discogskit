"""Discogs dump entity types.

Each entity module (artists, labels, masters, and releases) defines its Arrow schemas, table weights and an
``append_record`` parser, and exposes them as ``ENTITY``, an ``EntityDef``. Splitting and the chunk worker are shared
(``_split.py`` and ``_worker.py``) and driven by ``EntityDef``. ``ENTITIES`` lists every entity explicitly: adding one
means creating its module and adding its ``ENTITY`` to ``_ALL`` below.
"""

from __future__ import annotations

from discogskit.entities import artists, labels, masters, releases
from discogskit.entities._def import EntityDef

__all__ = ["ENTITIES", "EntityDef", "detect_entity"]

_ALL: tuple[EntityDef, ...] = (
    artists.ENTITY,
    labels.ENTITY,
    masters.ENTITY,
    releases.ENTITY,
)


def _by_name(entities: tuple[EntityDef, ...]) -> dict[str, EntityDef]:
    """Index ``entities`` by name, failing on a duplicate name instead of letting one entity replace another."""
    by_name: dict[str, EntityDef] = {}
    for entity in entities:
        if entity.name in by_name:
            raise ValueError(f"Duplicate entity name {entity.name!r}")
        by_name[entity.name] = entity
    return by_name


ENTITIES: dict[str, EntityDef] = _by_name(_ALL)


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
