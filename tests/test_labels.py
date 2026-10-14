"""Tests for labels entity XML parsing → IPC → tables."""

from __future__ import annotations

import os

import pytest

from discogskit.entities._def import ChunkArgs
from discogskit.entities._worker import extract_chunk_to_ipc
from discogskit.entities.labels import SCHEMAS
from tests.conftest import ipc_to_tables


class TestLabelsParsing:
    def test_extract_chunk(self, labels_xml_file):
        size = os.path.getsize(labels_xml_file)
        ipc_dict = extract_chunk_to_ipc(
            ChunkArgs(
                end=size, entity="labels", file_path=str(labels_xml_file), start=0
            )
        ).tables
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
        size = os.path.getsize(labels_xml_file)
        ipc_dict = extract_chunk_to_ipc(
            ChunkArgs(
                end=size, entity="labels", file_path=str(labels_xml_file), start=0
            )
        ).tables
        tables = ipc_to_tables(ipc_dict, SCHEMAS)

        subs = tables["label_sublabels"]
        assert subs.num_rows == 2
        assert subs.column("label_id").to_pylist() == [1, 1]
        assert subs.column("sublabel_id").to_pylist() == [10, 11]
        assert subs.column("name").to_pylist() == ["Sub Label One", "Sub Label Two"]

    def test_schema_matches(self, labels_xml_file):
        size = os.path.getsize(labels_xml_file)
        ipc_dict = extract_chunk_to_ipc(
            ChunkArgs(
                end=size, entity="labels", file_path=str(labels_xml_file), start=0
            )
        ).tables
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
            extract_chunk_to_ipc(
                ChunkArgs(
                    end=os.path.getsize(f), entity="labels", file_path=str(f), start=0
                )
            )
