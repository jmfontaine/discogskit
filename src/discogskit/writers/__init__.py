"""Writer protocol and the factories for database and file writers."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Protocol
from urllib.parse import unquote

if TYPE_CHECKING:
    import os

    import pyarrow as pa

    from discogskit.entities import EntityDef

__all__ = ["OutputExistsError", "Writer", "get_file_writer", "get_writer"]

# Table name -> seconds spent flushing it. "_commit" holds any shared commit cost.
TableTimings = dict[str, float]


class OutputExistsError(Exception):
    """Raised when output already exists and overwrite is not enabled."""


class Writer(Protocol):
    def cancel(self) -> None:
        """Cancel an in-flight ``write_chunk()`` that may block indefinitely (e.g. a COPY on a stalled server).

        Called from another thread than the one writing, on Ctrl+C. Best effort: the cancelled write fails, and
        ``close()`` still follows. Writers whose writes can't stall do nothing.
        """
        ...

    def check(self, entity: EntityDef) -> None:
        """Raise ``OutputExistsError`` if ``entity``'s output already exists and overwrite is off.

        Changes nothing, so a caller can check every entity before writing any. ``setup()`` checks again.
        """
        ...

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


def get_file_writer(
    output_dir: str | os.PathLike[str],
    output_format: str,
    *,
    compression: str | None = None,
    compression_level: int | None = None,
    overwrite: bool = False,
) -> Writer:
    """Construct a file writer for ``output_format`` (``"jsonl"`` or ``"parquet"``) that writes under ``output_dir``.

    ``compression`` defaults to the format's own: ``"zstd"`` for Parquet (also ``"brotli"``, ``"gzip"``, ``"snappy"``,
    ``"none"``), ``"none"`` for JSONL (also ``"bzip2"``, ``"gzip"``). ``compression_level`` defaults to the codec's
    library default, except JSONL gzip, which uses 6. Raises ``ValueError``, before any file exists, for an unknown
    format, a codec the format can't use, or a level that isn't an ``int`` or that the codec doesn't accept.
    """
    if output_format == "jsonl":
        from discogskit.writers.jsonl import JSONLWriter

        return JSONLWriter(
            output_dir,
            compression=compression,
            compression_level=compression_level,
            overwrite=overwrite,
        )
    if output_format == "parquet":
        from discogskit.writers.parquet import ParquetWriter

        return ParquetWriter(
            output_dir,
            compression=compression,
            compression_level=compression_level,
            overwrite=overwrite,
        )
    raise ValueError(
        f"unsupported output format {output_format!r}. Valid: jsonl, parquet."
    )


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
    """Auto-detect and construct a database writer (PostgreSQL or SQLite) from a DSN string.

    For Parquet or JSONL output, use ``get_file_writer``.
    """
    if dsn.startswith(("postgresql://", "postgres://")):
        if not _parses_as_conninfo(dsn):
            # Raised outside any except block, so libpq's error, which quotes the password, isn't chained either.
            raise ValueError(
                f"Invalid PostgreSQL DSN: {_redact_dsn(dsn)!r}. libpq can't parse it: check the host and port, and "
                "percent-encode special characters in the user name, password and database name ('%' as %25)."
            )

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

    sqlite_path = _sqlite_path(dsn)
    if sqlite_path is not None:
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

        return SQLiteWriter(sqlite_path, fk=fk, overwrite=overwrite)

    raise ValueError(
        f"Unsupported database DSN: {_redact_dsn(dsn)!r}. "
        f"Expected postgresql://, postgres:// or a SQLite path ({'/'.join(_SQLITE_SUFFIXES)})."
    )


def _parses_as_conninfo(dsn: str) -> bool:
    """Whether libpq can parse ``dsn``. Its parse errors quote the token they reject, or the whole URI, password
    included, so ``get_writer`` checks first and reports the DSN redacted instead.
    """
    from psycopg import ProgrammingError
    from psycopg.conninfo import conninfo_to_dict

    try:
        conninfo_to_dict(dsn)
    except ProgrammingError:
        return False
    return True


_SQLITE_PREFIX = "sqlite:///"
_SQLITE_SUFFIXES = (".db", ".sqlite", ".sqlite3")


def _sqlite_path(dsn: str) -> str | None:
    """The file a SQLite DSN names, or None if ``dsn`` isn't one. Raises ValueError, before any file exists, if it's
    malformed: SQLite would otherwise create a file with an unintended name, or fail opaquely. Or if it names an
    in-memory database, which would take the whole load and discard it when the run ends.
    """
    if dsn[: len("sqlite:")].lower() == "sqlite:":
        if dsn[: len(_SQLITE_PREFIX)].lower() != _SQLITE_PREFIX:
            raise ValueError(
                f"Invalid SQLite DSN: {_redact_dsn(dsn)!r}. Use sqlite:///<path>: sqlite:///discogs.db for a "
                "relative path, sqlite:////data/discogs.db for an absolute one."
            )
        path = dsn[len(_SQLITE_PREFIX) :]
    elif re.match(_URL_SCHEME, dsn) or not dsn.endswith(_SQLITE_SUFFIXES):
        return None
    else:
        path = dsn
    if not path:
        raise ValueError(f"Invalid SQLite DSN: {dsn!r} has no path.")
    if "?" in path:
        raise ValueError(
            f"Invalid SQLite DSN: {_redact_dsn(dsn)!r}. The path contains '?': query parameters aren't supported."
        )
    if _is_in_memory(path):
        raise ValueError(
            f"Invalid SQLite DSN: {_redact_dsn(dsn)!r} names an in-memory database, which would be discarded when "
            "the run ends. Use a file path, such as sqlite:///discogs.db."
        )
    return path


def _is_in_memory(path: str) -> bool:
    """Whether ``path`` is ``:memory:``, or a ``file:`` URI whose path is, in any case.

    SQLite matches ``:memory:`` exactly, and reads ``file:`` URIs only when built to (``SQLITE_USE_URI``); anything
    close is rejected regardless, since a file with such a name would be just as unintended. Like SQLite, the URI's
    path ends at its fragment (``#``) or a decoded NUL (``%00``). A URI's query string (``?mode=memory``) never gets
    here: ``_sqlite_path`` rejects any ``?`` first.
    """
    if path[: len("file:")].lower() == "file:":
        path = unquote(path[len("file:") :].partition("#")[0]).partition("\0")[0]
    return path.lower() == ":memory:"


# A URL's scheme, at the start: "://" inside a keyword/value DSN (e.g. in a password) doesn't make it a URL.
_URL_SCHEME = r"\s*[A-Za-z][A-Za-z0-9+.-]*://"
# A URL's userinfo password: from the colon after the user name to the last "@", so an unencoded "@", "/" or "?" in
# the password is still covered (at worst, more than the password is hidden). The user name may hold "?" or "#", as
# libpq reads it: only ":", "@" and "/" end it.
_URL_PASSWORD = re.compile(rf"^({_URL_SCHEME}[^:@/]*:).*@", re.DOTALL)
# A URL's query string, hidden whole: a password parameter can be spelled many ways (e.g. ``%70assword``).
_URL_QUERY = re.compile(r"\?.*", re.DOTALL)
# One libpq keyword/value pair, lexed as libpq does: the keyword, "=", then a single-quoted value or an unquoted one up
# to the first unescaped whitespace, with backslash escapes in both.
_KEYWORD_PAIR = re.compile(
    r"(\s*([^=\s]+)\s*=\s*)('(?:[^'\\]|\\.)*'|(?!')(?:[^\s\\]|\\.)*)", re.DOTALL
)


def _redact_dsn(dsn: str) -> str:
    """``dsn`` with its password, and a URL's whole query string, replaced by ``***``, for error messages."""
    if re.match(_URL_SCHEME, dsn):
        return _URL_QUERY.sub(
            "?***", _URL_PASSWORD.sub(r"\1***@", dsn, count=1), count=1
        )
    if "=" not in dsn:
        # A path: it can't hold a keyword/value password.
        return dsn
    parts = []
    pos = 0
    while dsn[pos:].strip():
        pair = _KEYWORD_PAIR.match(dsn, pos)
        if pair is None:
            # Not valid keyword/value syntax, so there's no telling which part is a password.
            return "***"
        password = pair.group(2).lower() == "password"
        parts.append(pair.group(1) + ("***" if password else pair.group(3)))
        pos = pair.end()
    return "".join(parts) + dsn[pos:]
