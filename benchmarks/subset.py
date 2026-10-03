#!/usr/bin/env python3
"""Build a valid subset of a Discogs dump from its first N records.

The download is streamed and decompressed on the fly, and stops once the subset is complete, so only the start of the
file is downloaded. expat decides where each record ends, so the cut doesn't depend on how the dump is formatted.
"""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
from http.client import HTTPResponse
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Protocol
from urllib.parse import parse_qs, quote, urlsplit
from xml.parsers import expat

import typer

from discogskit.entities import ENTITIES

if TYPE_CHECKING:
    from _typeshed import WriteableBuffer

BASE_URL = "https://data.discogs.com/"
METADATA_FILE = "subset.json"
USER_AGENT = "discogskit-benchmark (+https://github.com/jmfontaine/discogskit)"

# Year listings link each file as ?download=data%2F<year>%2Fdiscogs_<date>_<entity>.xml.gz (confirmed against
# tests/fixtures/data_discogs_com_2026.html). A dump is listed in the year of its date, even when it is published
# later: discogs_20260101_* appeared on 2026-01-15.
_KEY_PREFIX = "data/{year}/discogs_"


class SubsetError(Exception):
    """The dump can't be turned into a valid subset."""


class _Reader(Protocol):
    def read(self, size: int = -1, /) -> bytes: ...


class _Writer(Protocol):
    def write(self, data: bytes | bytearray, /) -> int: ...


class _Done(Exception):
    """Raised from an expat handler at the first event after the last record, to stop parsing there."""

    def __init__(self, offset: int) -> None:
        super().__init__(offset)
        self.offset = offset


class _LinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "a":
            self.hrefs.extend(
                value for name, value in attrs if name == "href" and value
            )


def parse_listing(html: str, year: int) -> dict[str, dict[str, str]]:
    """Map each dump date in a data.discogs.com year listing to its entities' download keys."""
    parser = _LinkParser()
    parser.feed(html)
    parser.close()
    prefix = _KEY_PREFIX.format(year=year)
    dumps: dict[str, dict[str, str]] = {}
    for href in parser.hrefs:
        for key in parse_qs(urlsplit(href).query).get("download", []):
            if not (key.startswith(prefix) and key.endswith(".xml.gz")):
                continue
            date, _, entity = key[len(prefix) : -len(".xml.gz")].partition("_")
            if entity in ENTITIES and len(date) == 8 and date.isdigit():
                dumps.setdefault(date, {})[entity] = key
    return dumps


def newest_complete(
    dumps: dict[str, dict[str, str]], entities: list[str]
) -> str | None:
    """Return the newest dump date listing every one of ``entities``, or None if there's none."""
    complete = [
        date for date, files in dumps.items() if all(e in files for e in entities)
    ]
    return max(complete, default=None)


def _open(url: str) -> HTTPResponse:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    return urllib.request.urlopen(request, timeout=60)


def _listing(year: int) -> dict[str, dict[str, str]]:
    url = f"{BASE_URL}?prefix={quote(f'data/{year}/', safe='')}"
    try:
        with _open(url) as response:
            html = response.read().decode()
    except OSError as exc:
        raise SubsetError(f"listing {url}: {exc}") from exc
    return parse_listing(html, year)


def resolve(date: str, entities: list[str]) -> tuple[str, dict[str, str]]:
    """Return the dump date and each entity's download key; ``latest`` is the newest dump listing every entity."""
    if date == "latest":
        year = datetime.now(timezone.utc).year
        # Early in January, the newest dump is still the previous year's December one.
        for candidate in (year, year - 1):
            dumps = _listing(candidate)
            newest = newest_complete(dumps, entities)
            if newest is not None:
                return newest, {e: dumps[newest][e] for e in entities}
        raise SubsetError(
            f"no dump lists all of {', '.join(entities)} in {year} or {year - 1}"
        )
    try:
        # The length check rejects unpadded fields such as 2026101, which strptime accepts.
        if len(date) != 8:
            raise ValueError(date)
        year = datetime.strptime(date, "%Y%m%d").replace(tzinfo=timezone.utc).year
    except ValueError:
        raise SubsetError(
            f"invalid dump date {date!r}: expected YYYYMMDD or 'latest'"
        ) from None
    files = _listing(year).get(date, {})
    missing = [e for e in entities if e not in files]
    if missing:
        raise SubsetError(f"dump {date} doesn't list {', '.join(missing)}")
    return date, {e: files[e] for e in entities}


@dataclass(frozen=True)
class Cut:
    """The subset ``cut`` wrote: the XML's size and SHA-256, before compression."""

    xml_bytes: int
    xml_sha256: str


def cut(
    source: _Reader,
    out: _Writer,
    entity: str,
    records: int,
    chunk_size: int = 1 << 20,
) -> Cut:
    """Copy a decompressed dump's first ``records`` records from ``source`` to ``out`` and close the root element.

    Everything up to the end of the last record is copied byte for byte, so the subset starts exactly like the dump.
    Raises SubsetError if the dump isn't UTF-8, its elements aren't the entity's, or it has fewer records.
    """
    if records < 1:
        raise SubsetError(f"record limit must be at least 1, got {records}")
    definition = ENTITIES[entity]
    # Forcing UTF-8 makes expat reject any other encoding, so the ASCII closing tag appended below stays valid.
    parser = expat.ParserCreate(encoding="UTF-8")
    depth = 0
    count = 0
    # Offset of the latest element event. The cut point comes after it, so everything before it can be written.
    mark = 0

    def stop(*_: object) -> None:
        # The first event after the last record starts right where that record's end tag finished.
        raise _Done(parser.CurrentByteIndex)

    def xml_decl(_version: str, encoding: str | None, _standalone: int) -> None:
        if encoding is not None and encoding.lower() not in ("utf-8", "utf8"):
            raise SubsetError(
                f"the dump declares encoding {encoding!r}; only UTF-8 is supported"
            )

    def start(name: str, _attrs: list[str]) -> None:
        nonlocal depth, mark
        if count == records:
            stop()
        mark = parser.CurrentByteIndex
        if depth == 0 and name != definition.name:
            raise SubsetError(
                f"the root element is <{name}>, expected <{definition.name}>"
            )
        if depth == 1 and name != definition.root_tag:
            raise SubsetError(
                f"<{definition.name}> contains <{name}>, expected <{definition.root_tag}>"
            )
        depth += 1

    def end(_name: str) -> None:
        nonlocal count, depth, mark
        if count == records:
            stop()
        mark = parser.CurrentByteIndex
        depth -= 1
        if depth == 1:
            count += 1
            if count == records:
                # Whatever follows the last record (whitespace, a comment, the root end tag) reaches the default
                # handler or one of the element handlers, and either one stops the parse.
                parser.DefaultHandler = stop
        elif depth == 0:
            raise SubsetError(
                f"the dump has only {count} records, fewer than {records}"
            )

    parser.ordered_attributes = True
    parser.XmlDeclHandler = xml_decl
    parser.StartElementHandler = start
    parser.EndElementHandler = end

    digest = hashlib.sha256()
    written = 0

    def emit(data: bytes | bytearray) -> None:
        nonlocal written
        out.write(data)
        digest.update(data)
        written += len(data)

    # expat may report the event after the last record only once it has read the whole next token, chunks later, so
    # the bytes after the latest event stay pending until the parse gets past them. A bytearray grows in place, so a
    # text node spanning many chunks doesn't get copied once per chunk.
    pending = bytearray()
    pending_start = 0  # Offset of pending in the decompressed dump.
    while True:
        chunk = source.read(chunk_size)
        pending += chunk
        try:
            parser.Parse(chunk, not chunk)
        except _Done as done:
            emit(pending[: done.offset - pending_start])
            emit(f"\n</{definition.name}>\n".encode())
            return Cut(xml_bytes=written, xml_sha256=digest.hexdigest())
        except expat.ExpatError as exc:
            raise SubsetError(f"invalid XML after {count} records: {exc}") from exc
        # An empty chunk is final: expat either raised above or the root closed early, which `end` reported.
        emit(pending[: mark - pending_start])
        del pending[: mark - pending_start]
        pending_start = mark


class _CountingReader(io.RawIOBase):
    """Count the compressed bytes actually downloaded, to show the download stopped early."""

    def __init__(self, raw: HTTPResponse) -> None:
        super().__init__()
        self.bytes_read = 0
        self._raw = raw

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: WriteableBuffer, /) -> int:
        count = self._raw.readinto(buffer)
        self.bytes_read += count
        return count


def fetch(key: str, entity: str, records: int, path: Path) -> dict[str, object]:
    """Stream ``key``'s first ``records`` records into a gzipped subset at ``path``; return its metadata."""
    url = f"{BASE_URL}?download={quote(key, safe='')}"
    partial = path.with_name(path.name + ".partial")
    try:
        with _open(url) as response:
            source_bytes = response.headers.get("Content-Length")
            counter = _CountingReader(response)
            with (
                gzip.GzipFile(fileobj=counter, mode="rb") as xml,
                partial.open("wb") as raw,
                # mtime=0 and no file name keep the .gz byte-identical across runs.
                gzip.GzipFile(
                    compresslevel=6, filename="", fileobj=raw, mode="wb", mtime=0
                ) as compressed,
            ):
                result = cut(xml, compressed, entity, records)
        partial.replace(path)
    except BaseException as exc:
        partial.unlink(missing_ok=True)
        # Network, gzip (BadGzipFile is an OSError; a truncated stream raises EOFError) and disk errors.
        if isinstance(exc, (EOFError, OSError)):
            raise SubsetError(f"fetching {entity} from {url}: {exc}") from exc
        raise
    return {
        "downloaded_bytes": counter.bytes_read,
        "entity": entity,
        "file": path.name,
        "gz_bytes": path.stat().st_size,
        "records": records,
        "source_bytes": int(source_bytes) if source_bytes else None,
        "source_url": url,
        "xml_bytes": result.xml_bytes,
        "xml_sha256": result.xml_sha256,
    }


def _entities(value: str) -> list[str]:
    names = [name.strip() for name in value.split(",") if name.strip()]
    unknown = [name for name in names if name not in ENTITIES]
    if unknown or not names:
        raise typer.BadParameter(
            f"expected a comma-separated list of {', '.join(ENTITIES)}, got {value!r}"
        )
    return names


def _run(action: Callable[[], None]) -> None:
    try:
        action()
    except SubsetError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1) from None


app = typer.Typer(
    help="Build a valid subset of a Discogs dump from its first N records."
)

Entities = Annotated[
    str, typer.Option(help="Comma-separated entities, e.g. releases,artists.")
]


@app.command("fetch")
def fetch_command(
    date: Annotated[str, typer.Option(help="Dump date (YYYYMMDD).")],
    entities: Entities,
    output: Annotated[
        Path, typer.Option(help="Directory for the subsets and their metadata.")
    ],
    records: Annotated[int, typer.Option(help="Records to keep per entity.", min=1)],
) -> None:
    """Download each entity's first N records into OUTPUT, with their metadata in subset.json."""

    def action() -> None:
        names = _entities(entities)
        dump_date, keys = resolve(date, names)
        output.mkdir(parents=True, exist_ok=True)
        files = []
        for entity in names:
            typer.echo(f"Fetching the first {records:,} {entity}...")
            files.append(
                fetch(
                    keys[entity],
                    entity,
                    records,
                    output / f"discogs_{dump_date}_{entity}.xml.gz",
                )
            )
            typer.echo(json.dumps(files[-1], indent=2))
        metadata = {"dump_date": dump_date, "files": files, "records": records}
        (output / METADATA_FILE).write_text(json.dumps(metadata, indent=2) + "\n")

    _run(action)


@app.command("resolve")
def resolve_command(
    date: Annotated[str, typer.Option(help="Dump date (YYYYMMDD), or 'latest'.")],
    entities: Entities,
) -> None:
    """Print the exact dump date, checking that it lists every entity."""
    _run(lambda: typer.echo(resolve(date, _entities(entities))[0]))


if __name__ == "__main__":
    app()
