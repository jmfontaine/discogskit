"""Shared fixtures for discogskit tests."""

from __future__ import annotations

import errno
import gzip
import os
import re
import sys
from pathlib import Path
from typing import NoReturn, cast

import pyarrow as pa
import pytest
from rich.text import Text

from discogskit._ipc import deserialize_batch
from discogskit.entities import ENTITIES
from discogskit.entities._def import ChunkArgs
from discogskit.entities._worker import extract_chunk_to_ipc
from discogskit.writers import _staging

# ------------------------------------------------------------------------------------------------------------------------
# Helper: IPC dict → dict[str, pa.Table] / dict[str, pa.RecordBatch]
# ------------------------------------------------------------------------------------------------------------------------


def ipc_to_tables(
    ipc_dict: dict[str, pa.Buffer], schemas: dict[str, pa.Schema]
) -> dict[str, pa.Table]:
    """Deserialize an IPC dict into Arrow Tables keyed by table name."""
    return {
        name: pa.Table.from_batches(
            [deserialize_batch(ipc_bytes)], schema=schemas[name]
        )
        for name, ipc_bytes in ipc_dict.items()
    }


def ipc_to_record_batches(ipc_dict: dict[str, pa.Buffer]) -> dict[str, pa.RecordBatch]:
    """Deserialize an IPC dict into the ``dict[str, pa.RecordBatch]`` ``Writer.write_chunk`` expects."""
    return {name: deserialize_batch(ipc_bytes) for name, ipc_bytes in ipc_dict.items()}


def empty_ipc(entity: str) -> dict[str, pa.Buffer]:
    """IPC dict for a chunk with no records, from an empty byte range of any file."""
    return extract_chunk_to_ipc(
        ChunkArgs(end=0, entity=entity, file_path=__file__, start=0)
    ).tables


def parse_file(path: Path, entity: str) -> dict[str, pa.Buffer]:
    """Parse the whole file at ``path`` as one ``entity`` chunk into an IPC dict."""
    return extract_chunk_to_ipc(
        ChunkArgs(
            end=os.path.getsize(path), entity=entity, file_path=str(path), start=0
        )
    ).tables


def parse_records(tmp_path: Path, entity: str, xml: str) -> dict[str, pa.Table]:
    """Parse raw ``entity`` records (no envelope) into Arrow Tables keyed by table name."""
    f = tmp_path / f"{entity}.xml"
    f.write_text(xml)
    return ipc_to_tables(parse_file(f, entity), ENTITIES[entity].schemas)


def write_dump(
    path: Path, entity: str, records: str | bytes, *, compresslevel: int = 9
) -> Path:
    """Write a gzipped ``entity`` dump to ``path``: the XML declaration, ``<entity>``, ``records``, ``</entity>``."""
    with gzip.open(path, "wb", compresslevel=compresslevel) as f:
        f.write(f"<?xml version='1.0' encoding='UTF-8'?>\n<{entity}>\n".encode())
        f.write(records.encode() if isinstance(records, str) else records)
        f.write(f"</{entity}>".encode())
    return path


def unstyle(output: str) -> str:
    """Strip ANSI styling from CLI output (Typer colors its error panels when it detects CI).

    Decodes line by line because ``Text.from_ansi`` drops a final newline before rich 15.
    """
    return "\n".join(Text.from_ansi(line).plain for line in output.split("\n"))


def assert_second_copy_fails(
    tmp_path: Path,
    entity: str,
    record: tuple[str, str],
    context: tuple[str, str, str],
    element: str,
) -> None:
    """Check that a record holding ``element`` twice fails, naming the record's id ``7`` and the second copy's line.

    ``record`` is the opening and closing XML of the record; ``context`` the opening and closing XML of the elements
    around the copies, and the tag of the one holding them. A parser that reads one copy would drop the other (#205).
    """
    opening, closing, parent = context
    xml = f"{record[0]}\n{opening}\n{element}\n{element}\n{closing}{record[1]}\n"
    tag = cast("re.Match[str]", re.match(r"<(\w+)", element))[1]
    f = tmp_path / "repeat.xml"
    f.write_text(xml)
    with pytest.raises(
        ValueError,
        match=rf"^Invalid <{ENTITIES[entity].root_tag}> record in .* at chunk line 1 \(chunk bytes \d+-\d+, id='7'\): "
        + re.escape(f"second <{tag}> in <{parent}>; discogskit reads only one")
        + " at chunk line 4$",
    ):
        parse_file(f, entity)


# Root ignores directory permissions, and Windows doesn't apply POSIX modes to directories.
needs_permissions = pytest.mark.skipif(
    sys.platform == "win32" or os.geteuid() == 0,
    reason="needs directory permissions that apply to the test process",
)


def require_or_skip(reason: str, env: str = "DISCOGSKIT_REQUIRE_DOCKER") -> NoReturn:
    """Skip the test, or fail it when ``env`` is ``1``, as CI sets it where the test must run."""
    if os.environ.get(env) == "1":
        pytest.fail(f"{reason}, but {env}=1 requires it")
    pytest.skip(reason)


def fail_after_first_move(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make the staging commit move its first file, then fail, as a move across filesystems would."""
    replace = os.replace
    moved: list[object] = []

    def failing_replace(src, dst):
        if moved:
            raise OSError(errno.EXDEV, os.strerror(errno.EXDEV), str(dst))
        replace(src, dst)
        moved.append(dst)

    monkeypatch.setattr(_staging.os, "replace", failing_replace)


# ------------------------------------------------------------------------------------------------------------------------
# XML fixture strings
# ------------------------------------------------------------------------------------------------------------------------

ARTISTS_XML = """\
<artist>
  <id>1</id>
  <name>DJ Test</name>
  <realname>Test Person</realname>
  <profile>A test artist</profile>
  <data_quality>Correct</data_quality>
  <namevariations>
    <name>DJ T</name>
    <name>Test</name>
  </namevariations>
  <urls>
    <url>https://example.com</url>
  </urls>
  <aliases>
    <name id="10">Alias One</name>
  </aliases>
  <groups>
    <name id="20">Group One</name>
  </groups>
  <members>
    <name id="30">Member One</name>
    <name id="31">Member Two</name>
  </members>
</artist>
<artist>
  <id>2</id>
  <name>Minimal Artist</name>
  <data_quality>Needs Vote</data_quality>
</artist>
"""

LABELS_XML = """\
<label>
  <id>1</id>
  <name>Test Label</name>
  <contactinfo>test@example.com</contactinfo>
  <data_quality>Correct</data_quality>
  <profile>A test label</profile>
  <parentLabel id="100">Parent Label</parentLabel>
  <urls>
    <url>https://label.example.com</url>
  </urls>
  <sublabels>
    <label id="10">Sub Label One</label>
    <label id="11">Sub Label Two</label>
  </sublabels>
</label>
<label>
  <id>2</id>
  <name>Minimal Label</name>
  <data_quality>Needs Vote</data_quality>
</label>
"""

MASTERS_XML = """\
<master id="1">
  <main_release>100</main_release>
  <title>Test Master</title>
  <year>2020</year>
  <notes>Some notes</notes>
  <data_quality>Correct</data_quality>
  <artists>
    <artist>
      <id>50</id>
      <name>Master Artist</name>
      <anv>M.A.</anv>
      <join>,</join>
    </artist>
  </artists>
  <genres>
    <genre>Electronic</genre>
    <genre>Rock</genre>
  </genres>
  <styles>
    <style>Techno</style>
  </styles>
  <videos>
    <video src="https://example.com/video" duration="300" embed="true">
      <title>Video Title</title>
      <description>Video Desc</description>
    </video>
  </videos>
</master>
<master id="2">
  <title>Minimal Master</title>
  <data_quality>Needs Vote</data_quality>
</master>
"""

RELEASES_XML = """\
<release id="1" status="Accepted">
  <title>Test Release</title>
  <country>US</country>
  <released>2020-01-01</released>
  <notes>Release notes</notes>
  <data_quality>Correct</data_quality>
  <master_id is_main_release="true">100</master_id>
  <genres>
    <genre>Electronic</genre>
  </genres>
  <styles>
    <style>Techno</style>
  </styles>
  <artists>
    <artist>
      <id>50</id>
      <name>Release Artist</name>
      <anv></anv>
      <join>,</join>
    </artist>
  </artists>
  <extraartists>
    <artist>
      <id>51</id>
      <name>Extra Artist</name>
      <anv></anv>
      <role>Producer</role>
      <tracks>A1</tracks>
    </artist>
  </extraartists>
  <labels>
    <label id="10" name="Test Label" catno="TL001"/>
  </labels>
  <series>
    <series id="20" name="Test Series" catno="TS001"/>
  </series>
  <formats>
    <format name="Vinyl" qty="1" text="">
      <descriptions>
        <description>12"</description>
        <description>33 RPM</description>
      </descriptions>
    </format>
  </formats>
  <identifiers>
    <identifier type="Barcode" description="" value="123456"/>
  </identifiers>
  <videos>
    <video src="https://example.com/v" duration="180" embed="true">
      <title>Vid Title</title>
      <description>Vid Desc</description>
    </video>
  </videos>
  <companies>
    <company>
      <id>60</id>
      <name>Test Company</name>
      <catno>TC001</catno>
      <entity_type>1</entity_type>
      <entity_type_name>Pressed By</entity_type_name>
    </company>
  </companies>
  <tracklist>
    <track>
      <position>A1</position>
      <title>Track One</title>
      <duration>5:00</duration>
      <artists>
        <artist>
          <id>50</id>
          <name>Track Artist</name>
          <anv></anv>
          <join></join>
        </artist>
      </artists>
      <extraartists>
        <artist>
          <id>52</id>
          <name>Track Extra</name>
          <anv></anv>
          <role>Remix</role>
        </artist>
      </extraartists>
      <sub_tracks>
        <track>
          <position>A1.1</position>
          <title>Sub Track</title>
          <duration>2:30</duration>
        </track>
      </sub_tracks>
    </track>
  </tracklist>
</release>
<release id="2" status="Accepted">
  <title>Minimal Release</title>
  <data_quality>Needs Vote</data_quality>
</release>
"""

# Non-ASCII text in every form the XML can carry it: raw UTF-8 (including a 4-byte emoji), predefined entities,
# decimal and hex character references, and CDATA holding markup characters.
UNICODE_ARTISTS_XML = """\
<artist>
  <id>1</id>
  <name>Björk &amp; 坂本龍一 🎵</name>
  <realname>Bj&#246;rk Gu&#xF0;mundsd&#243;ttir</realname>
  <profile><![CDATA[Sings <i>"Jóga"</i> & more]]> &#x2014; &lt;©&gt;</profile>
  <data_quality>Correct</data_quality>
  <namevariations>
    <name>ビョーク</name>
    <name><![CDATA[<Björk>]]></name>
  </namevariations>
  <urls>
    <url>https://example.com/bj%C3%B6rk?a=1&amp;b=Ω</url>
  </urls>
  <aliases>
    <name id="10">Sugarcubes &#x1F3B5;</name>
  </aliases>
</artist>
"""

# The rows UNICODE_ARTISTS_XML must produce, by table; the other artists tables are empty.
UNICODE_ARTISTS_ROWS: dict[str, list[dict]] = {
    "artist_aliases": [{"alias_id": 10, "artist_id": 1, "name": "Sugarcubes 🎵"}],
    "artists": [
        {
            "data_quality": "Correct",
            "id": 1,
            "name": "Björk & 坂本龍一 🎵",
            "namevariations": ["ビョーク", "<Björk>"],
            "profile": 'Sings <i>"Jóga"</i> & more — <©>',
            "realname": "Björk Guðmundsdóttir",
            "urls": ["https://example.com/bj%C3%B6rk?a=1&b=Ω"],
        }
    ],
}


# ------------------------------------------------------------------------------------------------------------------------
# XML file fixtures (write to tmp_path with proper envelope)
# ------------------------------------------------------------------------------------------------------------------------


@pytest.fixture()
def artists_xml_file(tmp_path):
    """Write raw artist elements (no envelope) — extract_chunk_to_ipc adds its own."""
    p = tmp_path / "artists.xml"
    p.write_text(ARTISTS_XML)
    return p


@pytest.fixture()
def labels_xml_file(tmp_path):
    p = tmp_path / "labels.xml"
    p.write_text(LABELS_XML)
    return p


@pytest.fixture()
def masters_xml_file(tmp_path):
    p = tmp_path / "masters.xml"
    p.write_text(MASTERS_XML)
    return p


@pytest.fixture()
def releases_xml_file(tmp_path):
    p = tmp_path / "releases.xml"
    p.write_text(RELEASES_XML)
    return p


@pytest.fixture()
def unicode_artists_xml_file(tmp_path):
    p = tmp_path / "unicode_artists.xml"
    p.write_text(UNICODE_ARTISTS_XML, encoding="utf-8")
    return p


# ------------------------------------------------------------------------------------------------------------------------
# Writer test fixtures: the artists entity and an IPC chunk of ARTISTS_XML
# ------------------------------------------------------------------------------------------------------------------------


@pytest.fixture()
def entity():
    return ENTITIES["artists"]


@pytest.fixture()
def ipc_dict(artists_xml_file):
    return parse_file(artists_xml_file, "artists")


# ------------------------------------------------------------------------------------------------------------------------
# PostgreSQL testcontainer fixtures
# ------------------------------------------------------------------------------------------------------------------------


@pytest.fixture(scope="session")
def pg_container():
    import importlib.util
    import subprocess

    if importlib.util.find_spec("testcontainers") is None:
        require_or_skip("testcontainers not installed")

    # Check Docker is available
    try:
        subprocess.run(
            ["docker", "info"],
            capture_output=True,
            check=True,
            timeout=10,
        )
    except (
        FileNotFoundError,
        subprocess.CalledProcessError,
        subprocess.TimeoutExpired,
    ):
        require_or_skip("Docker not available")

    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16") as pg:
        yield pg


@pytest.fixture(scope="session")
def pg_dsn(pg_container):
    return pg_container.get_connection_url(driver=None)
