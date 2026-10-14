import os

import pytest

from discogskit.entities import ChunkArgs, get
from discogskit.entities._worker import extract_chunk_to_ipc
from discogskit.entities.artists import SCHEMAS
from tests.conftest import (
    ARTISTS_XML,
    LABELS_XML,
    MASTERS_XML,
    RELEASES_XML,
    ipc_to_tables,
)

PLAIN_RECORDS = {
    "artists": ARTISTS_XML,
    "labels": LABELS_XML,
    "masters": MASTERS_XML,
    "releases": RELEASES_XML,
}


@pytest.mark.parametrize("entity_name", sorted(PLAIN_RECORDS))
@pytest.mark.parametrize(
    "layout",
    [
        # Checked when the next record ends.
        "{ns}{plain}",
        # An element between the namespaced record and the next record must not shield it.
        "{ns}<other/>\n{plain}",
        # Checked once the chunk has been parsed.
        "{plain}{ns}",
        # libxml2 can't give exact element lines past 65535.
        "{plain}" + "\n" * 70_000 + "{ns}",
    ],
    ids=["first", "before-other", "last", "past-line-65535"],
)
def test_namespaced_record_fails(tmp_path, entity_name, layout):
    """A namespaced record element must fail, naming the file, the element and the line where its start tag begins,
    instead of being dropped."""
    tag = get(entity_name).root_tag
    # A start tag over several lines: lxml's own line would be its last one.
    namespaced = f'<{tag}\n  xmlns="urn:x"\n  id="9">\n  <id>9</id>\n</{tag}>\n'
    plain = PLAIN_RECORDS[entity_name]
    xml = layout.format(ns=namespaced, plain=plain)
    line = xml[: xml.index(namespaced)].count("\n") + 1
    f = tmp_path / f"{entity_name}.xml"
    f.write_text(xml)

    with pytest.raises(ValueError) as info:
        extract_chunk_to_ipc(ChunkArgs(entity_name, str(f), 0, os.path.getsize(f)))

    message = str(info.value)
    assert str(f) in message
    assert f"namespaced record element <{{urn:x}}{tag}> on line {line};" in message


def test_namespaced_line_past_65535_is_approximate_when_expat_fails(tmp_path):
    """Expat rejects some names libxml2 accepts (here U+0370); the line then can't be exact and mustn't claim to be."""
    xml = '<artist \u0370="x"><id>1</id></artist>' + "\n" * 70_000
    xml += '<artist xmlns="urn:x">\n  <id>9</id>\n</artist>\n'
    f = tmp_path / "artists.xml"
    f.write_text(xml, encoding="utf-8")

    with pytest.raises(ValueError, match=r"<\{urn:x\}artist> near line \d+;"):
        extract_chunk_to_ipc(ChunkArgs("artists", str(f), 0, os.path.getsize(f)))


def test_namespaced_element_in_nested_container_is_not_a_record(tmp_path):
    """Only children of the chunk's container are records; one nested in a same-named element is record content."""
    xml = (
        "<artist><id>1</id><name>A</name>"
        '<artists><thing xmlns="urn:inner"/><artist><id>2</id></artist></artists>'
        "</artist>\n"
    )
    f = tmp_path / "artists.xml"
    f.write_text(xml)

    ipc_dict = extract_chunk_to_ipc(ChunkArgs("artists", str(f), 0, os.path.getsize(f)))

    assert 1 in ipc_to_tables(ipc_dict, SCHEMAS)["artists"].column("id").to_pylist()
