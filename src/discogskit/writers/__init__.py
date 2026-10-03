"""Writer protocol and registry for multi-target output."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    import pyarrow as pa

    from discogskit.entities import EntityDef

# Table name -> seconds spent flushing it. "_commit" holds any shared commit cost.
TableTimings = dict[str, float]


class OutputExistsError(Exception):
    """Raised when output already exists and overwrite is not enabled."""


class Writer(Protocol):
    def setup(self, entity: EntityDef) -> None:
        """Prepare destination (create tables / create output dir)."""
        ...

    def write_chunk(self, tables: dict[str, pa.RecordBatch]) -> TableTimings:
        """Write one already-deserialized chunk, one RecordBatch per table.

        ``tables`` iterates in ``entity.table_order`` (root table first); writers that enforce foreign keys
        while loading (e.g. SQLite with ``fk=True``) depend on the parent table being written before its
        child tables.

        Returns per-table flush time in seconds for ``--profile``.
        """
        ...

    def finalize(self, entity: EntityDef) -> None:
        """Post-load work (build indexes for DB, close files, etc.)."""
        ...

    def close(self) -> None:
        """Release resources. Must be safe to call even after errors."""
        ...


# Writer kwargs that only PostgreSQLWriter understands, mapped to the CLI flag that sets them and the
# default get_writer shares with the CLI's own Typer defaults. A SQLite DSN rejects these, but only when
# the caller passed a value other than that default — so CLI defaults the user never touched never
# trigger the error.
_PG_ONLY_DEFAULTS: dict[str, tuple[str, object]] = {
    "create_schema": ("--pg-create-schema", False),
    "index_workers": ("--pg-index-workers", 2),
    "schema": ("--pg-schema", None),
    "unlogged": ("--pg-unlogged", False),
    "write_workers": ("--pg-write-workers", 1),
}


def get_writer(
    dsn: str,
    *,
    create_schema: bool = False,
    fk: bool = False,
    index_workers: int = 2,
    overwrite: bool = False,
    schema: str | None = None,
    unlogged: bool = False,
    write_workers: int = 1,
) -> Writer:
    """Auto-detect and construct a writer from a DSN string.

    Only database targets (PostgreSQL, SQLite) are supported via this factory.
    File-format writers (Parquet, JSONL) should be constructed directly.
    """
    if dsn.startswith(("postgresql://", "postgres://")):
        from discogskit.writers.postgresql import PostgreSQLWriter

        return PostgreSQLWriter(
            dsn,
            create_schema=create_schema,
            fk=fk,
            index_workers=index_workers,
            overwrite=overwrite,
            schema=schema,
            unlogged=unlogged,
            write_workers=write_workers,
        )

    if dsn.startswith("sqlite:///") or dsn.endswith((".db", ".sqlite", ".sqlite3")):
        passed = {
            "create_schema": create_schema,
            "index_workers": index_workers,
            "schema": schema,
            "unlogged": unlogged,
            "write_workers": write_workers,
        }
        rejected = [
            flag
            for name, (flag, default) in _PG_ONLY_DEFAULTS.items()
            if passed[name] != default
        ]
        if rejected:
            raise ValueError(
                f"{', '.join(rejected)} can only be used with PostgreSQL, not a SQLite DSN."
            )

        from discogskit.writers.sqlite import SQLiteWriter

        path = dsn.removeprefix("sqlite:///") if dsn.startswith("sqlite:///") else dsn
        return SQLiteWriter(path, fk=fk, overwrite=overwrite)

    raise ValueError(
        f"Unsupported database DSN: {dsn!r}. "
        f"Expected postgresql://, postgres:// or a SQLite path (.db/.sqlite/.sqlite3)."
    )
