"""Tests for PostgreSQL DDL generation."""

from __future__ import annotations

import pyarrow as pa
import pytest

from discogskit.writers.postgresql import generate_ddl


class TestGenerateDDL:
    def test_basic_table(self):
        schema = pa.schema(
            [
                pa.field("name", pa.utf8(), nullable=False),
                pa.field("value", pa.int32(), nullable=False),
            ]
        )
        ddl = generate_ddl("test_table", schema).as_string(None)
        assert "CREATE TABLE" in ddl
        assert '"test_table"' in ddl
        assert '"name" TEXT' in ddl
        assert '"value" INTEGER' in ddl

    def test_pg_types(self):
        schema = pa.schema(
            [
                pa.field("flag", pa.bool_(), nullable=False),
                pa.field("big", pa.int64(), nullable=False),
            ]
        )
        ddl = generate_ddl("t", schema).as_string(None)
        assert "BOOLEAN" in ddl
        assert "BIGINT" in ddl

    def test_list_type_becomes_array(self):
        schema = pa.schema(
            [
                pa.field("tags", pa.list_(pa.utf8()), nullable=False),
            ]
        )
        ddl = generate_ddl("t", schema).as_string(None)
        assert "TEXT[]" in ddl
        assert "DEFAULT '{}'" in ddl

    def test_not_null_with_default(self):
        schema = pa.schema(
            [
                pa.field("name", pa.utf8(), nullable=False),
                pa.field("count", pa.int32(), nullable=False),
            ]
        )
        ddl = generate_ddl("t", schema).as_string(None)
        assert "NOT NULL" in ddl
        assert "DEFAULT ''" in ddl
        assert "DEFAULT 0" in ddl

    def test_id_column_no_default(self):
        schema = pa.schema(
            [
                pa.field("id", pa.int32(), nullable=False),
            ]
        )
        ddl = generate_ddl("t", schema).as_string(None)
        assert "NOT NULL" in ddl
        assert "DEFAULT" not in ddl

    def test_nullable_no_constraint(self):
        schema = pa.schema(
            [
                pa.field("maybe", pa.int32(), nullable=True),
            ]
        )
        ddl = generate_ddl("t", schema).as_string(None)
        assert "NOT NULL" not in ddl

    def test_unlogged_table(self):
        schema = pa.schema([pa.field("x", pa.int32(), nullable=True)])
        ddl = generate_ddl("t", schema, unlogged=True).as_string(None)
        assert "CREATE UNLOGGED TABLE" in ddl

    def test_logged_table_default(self):
        schema = pa.schema([pa.field("x", pa.int32(), nullable=True)])
        ddl = generate_ddl("t", schema).as_string(None)
        assert "UNLOGGED" not in ddl

    def test_unsupported_type_raises(self):
        schema = pa.schema(
            [
                pa.field("ts", pa.timestamp("us"), nullable=False),
            ]
        )
        with pytest.raises(ValueError, match="Unsupported Arrow type"):
            generate_ddl("t", schema)

    def test_unsupported_list_value_type_raises(self):
        schema = pa.schema(
            [
                pa.field("bad_list", pa.list_(pa.timestamp("us")), nullable=False),
            ]
        )
        with pytest.raises(ValueError, match="Unsupported list value type"):
            generate_ddl("t", schema)
