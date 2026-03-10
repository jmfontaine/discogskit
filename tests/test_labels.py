"""Tests for labels entity XML parsing → IPC → tables."""

from __future__ import annotations

import os

from discogskit.entities import ChunkArgs
from discogskit.entities.labels import SCHEMAS, extract_chunk_to_ipc
from tests.conftest import ipc_to_tables


class TestLabelsParsing:
    def test_extract_chunk(self, labels_xml_file):
        size = os.path.getsize(labels_xml_file)
        ipc_dict = extract_chunk_to_ipc(ChunkArgs(str(labels_xml_file), 0, size))
        tables = ipc_to_tables(ipc_dict, SCHEMAS)

        labels = tables["labels"]
        assert labels.num_rows == 2
        assert labels.column("id").to_pylist() == [1, 2]
        assert labels.column("name").to_pylist() == ["Test Label", "Minimal Label"]
        assert labels.column("contactinfo").to_pylist() == ["test@example.com", ""]
        assert labels.column("profile").to_pylist() == ["A test label", ""]
        assert labels.column("parent_label_id").to_pylist() == [100, None]
        assert labels.column("parent_label_name").to_pylist() == ["Parent Label", ""]
        assert labels.column("urls").to_pylist() == [
            ["https://label.example.com"],
            [],
        ]

    def test_sublabels(self, labels_xml_file):
        size = os.path.getsize(labels_xml_file)
        ipc_dict = extract_chunk_to_ipc(ChunkArgs(str(labels_xml_file), 0, size))
        tables = ipc_to_tables(ipc_dict, SCHEMAS)

        subs = tables["label_sublabels"]
        assert subs.num_rows == 2
        assert subs.column("label_id").to_pylist() == [1, 1]
        assert subs.column("sublabel_id").to_pylist() == [10, 11]
        assert subs.column("name").to_pylist() == ["Sub Label One", "Sub Label Two"]

    def test_schema_matches(self, labels_xml_file):
        size = os.path.getsize(labels_xml_file)
        ipc_dict = extract_chunk_to_ipc(ChunkArgs(str(labels_xml_file), 0, size))
        tables = ipc_to_tables(ipc_dict, SCHEMAS)

        for name, table in tables.items():
            assert table.schema == SCHEMAS[name]

    def test_missing_id_skipped(self, tmp_path):
        """Label element without <id> is skipped with a warning."""
        xml = "<label><name>No ID</name><data_quality>Correct</data_quality></label>\n"
        f = tmp_path / "no_id.xml"
        f.write_text(xml)

        import warnings

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            ipc_dict = extract_chunk_to_ipc(ChunkArgs(str(f), 0, os.path.getsize(f)))
            tables = ipc_to_tables(ipc_dict, SCHEMAS)

        assert tables["labels"].num_rows == 0
        assert any("missing id" in str(warning.message) for warning in w)
