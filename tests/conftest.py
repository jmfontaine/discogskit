"""Shared fixtures for discogskit tests."""

from __future__ import annotations

import pyarrow as pa
import pytest

from discogskit.writers._ipc import deserialize_batches

# ------------------------------------------------------------------------------------------------------------------------
# Helper: IPC dict → dict[str, pa.Table]
# ------------------------------------------------------------------------------------------------------------------------


def ipc_to_tables(
    ipc_dict: dict[str, bytes], schemas: dict[str, pa.Schema]
) -> dict[str, pa.Table]:
    """Deserialize an IPC dict into Arrow Tables keyed by table name."""
    result = {}
    for name, ipc_bytes in ipc_dict.items():
        batches = deserialize_batches(ipc_bytes)
        if batches:
            result[name] = pa.Table.from_batches(batches, schema=schemas[name])
        else:
            result[name] = pa.table({}, schema=schemas[name])
    return result


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


# ------------------------------------------------------------------------------------------------------------------------
# PostgreSQL testcontainer fixtures
# ------------------------------------------------------------------------------------------------------------------------


@pytest.fixture(scope="session")
def pg_container():
    pytest.importorskip("testcontainers")
    import subprocess

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
        pytest.skip("Docker not available")

    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16") as pg:
        yield pg


@pytest.fixture(scope="session")
def pg_dsn(pg_container):
    return pg_container.get_connection_url(driver=None)
