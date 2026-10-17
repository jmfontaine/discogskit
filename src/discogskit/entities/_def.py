"""Definitions shared by the entity modules, the chunk worker and the writers.

The entity modules import from here rather than from the ``discogskit.entities`` package, which imports them to build
``ENTITIES``, so there is no import cycle.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
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


@dataclass(kw_only=True)
class ChunkArgs:
    """Arguments passed to each parse worker for one XML chunk.

    Carries the entity name rather than the ``EntityDef`` so it stays cheap to pickle; the worker looks the entity up
    in ``ENTITIES``.
    """

    end: int
    entity: str
    file_path: str
    start: int
    strict: bool = False


class ElementValueError(ValueError):
    """A record value error about one element inside the record. The chunk worker adds that element's chunk line.

    It never leaves the worker, which re-raises it as a plain ``ValueError``, so it needn't pickle.
    """

    def __init__(self, message: str, element: etree._Element) -> None:
        super().__init__(message)
        self.element = element


def second_copy(elem: etree._Element) -> ElementValueError:
    """The error for ``elem``, a second copy of an element the parser reads one of: keeping either copy would drop the
    other's data without a word (#205). The chunk worker adds its line."""
    parent = elem.getparent()
    where = "" if parent is None else f" in <{parent.tag}>"
    return ElementValueError(
        f"second <{elem.tag}>{where}; discogskit reads only one", elem
    )


@dataclass(kw_only=True)
class EntityDef:
    """Definition for one Discogs entity type.

    ``name`` is also the XML container element (``<artists>``) and ``root_tag`` the record element (``<artist>``).
    The first table in ``schemas`` is the root table, and its primary key column is always ``id``. ``fk_column`` is
    the column every other table uses to reference the root table's ``id``; the database writers index it. ``record_id``
    returns a record element's id as written in the XML, or ``None`` when it has none.
    """

    append_record: Callable[[Cols, etree._Element, set[str] | None], None]
    fk_column: str
    name: str
    record_id: Callable[[etree._Element], str | None]
    root_tag: str
    schemas: dict[str, pa.Schema]
    table_weights: dict[str, float]

    @cached_property
    def table_order(self) -> list[str]:
        return list(self.schemas)

    def find_split_points(
        self, file_path: str, target_chunk_bytes: int
    ) -> list[tuple[int, int]]:
        return find_split_points(
            file_path, target_chunk_bytes, self.root_tag, self.name
        )


# With ``--strict``, the parsers add to ``unknown`` the path of everything they don't read, at every level they walk,
# through the helpers below. A path is relative to the record (``videos/video/foo``); ``parent_path`` is the path of
# ``elem``'s parent ending in ``/``, or ``""`` when that parent is the record itself. A text-only element's children
# are unhandled too: reading only its text drops them. So are the attributes a parser doesn't read (``urls/url/@foo``,
# or ``@foo`` on the record) and the text of an element the parser reads only the children or attributes of
# (``urls/text()``, or ``text()`` directly in the record), whether before its first child or after any child. Loops
# over repeated items read text-only children inline and call the checks only with ``--strict``, so a run without it
# makes no extra call per element.

# The attributes of an element whose only attribute is a Discogs id, such as an alias's <name id="...">.
ID_ATTR = frozenset({"id"})
# The attributes of a master's or release's <video>.
VIDEO_ATTRS = frozenset({"duration", "embed", "src"})
# XML's whitespace characters.
XML_SPACE = " \t\n\r"


def check_attrs(
    elem: etree._Element,
    path: str,
    unknown: set[str],
    handled: frozenset[str] = frozenset(),
) -> None:
    """Add the path of each attribute of ``elem`` not in ``handled``; ``path`` is ``elem``'s own path ending in ``/``,
    or ``""`` for the record."""
    for name in elem.attrib:
        if name not in handled:
            unknown.add(f"{path}@{name}")


def check_leaf(
    elem: etree._Element,
    parent_path: str,
    unknown: set[str],
    attrs: frozenset[str] = frozenset(),
) -> None:
    """Add the path of each child of ``elem``, an element the parser reads only the text and ``attrs`` of, and of each
    of its other attributes."""
    path = f"{parent_path}{elem.tag}/"
    for child in elem:
        unknown.add(f"{path}{child.tag}")
    if elem.attrib:
        check_attrs(elem, path, unknown, attrs)


def check_text(elem: etree._Element, path: str, unknown: set[str]) -> None:
    """Add ``elem``'s text path when it holds text other than whitespace, before its first child or after any; for an
    element the parser reads only the children or attributes of. ``path`` is as for ``check_attrs``."""
    if has_text(elem.text) or any(has_text(child.tail) for child in elem):
        unknown.add(f"{path}text()")


def child_elements(
    elem: etree._Element,
    tag: str,
    parent_path: str,
    unknown: set[str] | None,
    attrs: frozenset[str] = frozenset(),
) -> Iterator[etree._Element]:
    """Yield ``elem``'s ``<tag>`` children and add the path of each other child, of each attribute not in ``attrs``
    and of its text; iterate to the end to check them all."""
    if unknown is not None:
        path = f"{parent_path}{elem.tag}/"
        check_attrs(elem, path, unknown, attrs)
        check_text(elem, path, unknown)
    for child in elem:
        if child.tag == tag:
            yield child
        elif unknown is not None:
            unknown.add(f"{parent_path}{elem.tag}/{child.tag}")


def has_text(value: str | None) -> bool:
    """Whether ``value``, an element's text or tail, holds more than XML's whitespace. Other spaces, such as U+00A0,
    are text."""
    return value is not None and bool(value.strip(XML_SPACE))


def leaf_text(elem: etree._Element, parent_path: str, unknown: set[str] | None) -> str:
    """Return the text of ``elem``, a text-only element, or ``""`` when it has none, and add the path of each of its
    children and attributes."""
    if unknown is not None:
        check_leaf(elem, parent_path, unknown)
    return elem.text or ""


def leaf_texts(
    elem: etree._Element, tag: str, parent_path: str, unknown: set[str] | None
) -> list[str]:
    """Return the texts of ``elem``'s ``<tag>`` children, as ``leaf_text`` does, and add the path of each other child
    and of ``elem``'s attributes and text."""
    if unknown is None:
        return [child.text or "" for child in elem.findall(tag)]
    path = f"{parent_path}{elem.tag}/"
    return [
        leaf_text(child, path, unknown)
        for child in child_elements(elem, tag, parent_path, unknown)
    ]


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
