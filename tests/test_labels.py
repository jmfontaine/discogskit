"""Tests for labels entity XML parsing → IPC → tables."""

from __future__ import annotations

import pytest

from discogskit.entities.labels import SCHEMAS
from tests.conftest import (
    assert_second_copy_fails,
    ipc_to_tables,
    parse_file,
    parse_records,
)


class TestLabelsParsing:
    def test_extract_chunk(self, labels_xml_file):
        ipc_dict = parse_file(labels_xml_file, "labels")
        tables = ipc_to_tables(ipc_dict, SCHEMAS)

        labels = tables["labels"]
        assert labels.num_rows == 2
        assert labels.column("id").to_pylist() == [1, 2]
        assert labels.column("name").to_pylist() == ["Test Label", "Minimal Label"]
        assert labels.column("contactinfo").to_pylist() == ["test@example.com", None]
        assert labels.column("profile").to_pylist() == ["A test label", None]
        assert labels.column("parent_label_id").to_pylist() == [100, None]
        assert labels.column("parent_label_name").to_pylist() == ["Parent Label", None]
        assert labels.column("urls").to_pylist() == [
            ["https://label.example.com"],
            None,
        ]

    def test_sublabels(self, labels_xml_file):
        ipc_dict = parse_file(labels_xml_file, "labels")
        tables = ipc_to_tables(ipc_dict, SCHEMAS)

        subs = tables["label_sublabels"]
        assert subs.num_rows == 2
        assert subs.column("label_id").to_pylist() == [1, 1]
        assert subs.column("sublabel_id").to_pylist() == [10, 11]
        assert subs.column("name").to_pylist() == ["Sub Label One", "Sub Label Two"]

    def test_schema_matches(self, labels_xml_file):
        ipc_dict = parse_file(labels_xml_file, "labels")
        tables = ipc_to_tables(ipc_dict, SCHEMAS)

        for name, table in tables.items():
            assert table.schema == SCHEMAS[name]

    @pytest.mark.parametrize("id_xml", ["", "<id></id>", "<id/>"])
    def test_missing_id_fails(self, tmp_path, id_xml):
        """A label without an id fails with its line instead of vanishing (#94)."""
        xml = (
            "<label><id>1</id><name>A</name></label>\n"
            f"<label>{id_xml}<name>No ID</name></label>\n"
        )
        f = tmp_path / "no_id.xml"
        f.write_text(xml)

        with pytest.raises(
            ValueError,
            match=r"^Invalid <label> record in .* at chunk line 2 .*: missing or empty id$",
        ):
            parse_file(f, "labels")


_LABEL = ("<label><id>7</id>", "</label>")
_RECORD = ("", "", "label")


class TestRepeatedElements:
    """An element a label holds one of fails when repeated; a container of repeated items appends (#205)."""

    def test_appending_container_repeated(self, tmp_path):
        element = '<sublabels><label id="1">S</label></sublabels>'
        tables = parse_records(
            tmp_path, "labels", f"<label><id>7</id>{element}{element}</label>\n"
        )
        assert tables["label_sublabels"].num_rows == 2

    @pytest.mark.parametrize(
        ("record", "element"),
        [
            pytest.param(("<label>", "</label>"), "<id>7</id>", id="id"),
            pytest.param(_LABEL, "<name>N</name>", id="name"),
            pytest.param(_LABEL, "<contactinfo>C</contactinfo>", id="contactinfo"),
            pytest.param(_LABEL, "<profile>P</profile>", id="profile"),
            pytest.param(
                _LABEL, "<data_quality>Correct</data_quality>", id="data_quality"
            ),
            pytest.param(
                _LABEL, '<parentLabel id="1">P</parentLabel>', id="parentLabel"
            ),
            pytest.param(_LABEL, "<parentLabel/>", id="parentLabel-empty"),
            pytest.param(
                _LABEL, "<urls><url>https://example.com</url></urls>", id="urls"
            ),
        ],
    )
    def test_second_copy_fails(self, tmp_path, record, element):
        assert_second_copy_fails(tmp_path, "labels", record, _RECORD, element)
