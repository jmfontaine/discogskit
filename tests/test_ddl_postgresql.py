"""Tests for PostgreSQL DDL generation."""

from __future__ import annotations

import pyarrow as pa
import pytest

from discogskit.entities import ENTITIES, get
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

    def test_not_null_without_default(self):
        """A DEFAULT would hide a missing value, e.g. DEFAULT 0 on a foreign key (#21)."""
        schema = pa.schema(
            [
                pa.field("id", pa.int32(), nullable=False),
                pa.field("name", pa.utf8(), nullable=False),
                pa.field("parent_id", pa.int32(), nullable=False),
                pa.field("tags", pa.list_(pa.utf8()), nullable=False),
            ]
        )
        ddl = generate_ddl("t", schema).as_string(None)
        assert ddl.count("NOT NULL") == 4
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

    @pytest.mark.parametrize("entity_name", sorted(ENTITIES))
    def test_real_entity_schemas(self, entity_name):
        """Every table of every entity: each column quoted (``join`` is a keyword), typed, and NOT NULL
        exactly where the Arrow field is non-nullable."""
        pg_types = {pa.bool_(): "BOOLEAN", pa.int32(): "INTEGER", pa.utf8(): "TEXT"}
        for table_name, schema in get(entity_name).schemas.items():
            columns = []
            for field in schema:
                if isinstance(field.type, pa.ListType):
                    pg_type = pg_types[field.type.value_type] + "[]"
                else:
                    pg_type = pg_types[field.type]
                not_null = "" if field.nullable else " NOT NULL"
                columns.append(f'"{field.name}" {pg_type}{not_null}')

            ddl = generate_ddl(table_name, schema).as_string(None)

            assert ddl == (
                f'CREATE TABLE "{table_name}" (\n    ' + ",\n    ".join(columns) + "\n)"
            )
