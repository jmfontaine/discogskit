"""CLI application."""

from __future__ import annotations

import os
from enum import Enum
from importlib.metadata import version
from pathlib import Path
from typing import Annotated, cast

import typer
from rich.table import Table

from discogskit import pipeline
from discogskit._console import console, status
from discogskit.decompress import DecompressError
from discogskit.entities import detect_entity
from discogskit.entities import get as get_entity
from discogskit.writers import OutputExistsError, Writer, get_writer


class OutputFormat(str, Enum):
    """Output file format for `convert`."""

    JSONL = "jsonl"
    PARQUET = "parquet"


class Compression(str, Enum):
    """Compression codec for `convert` output. Valid codecs depend on `--format`."""

    BZIP2 = "bzip2"
    GZIP = "gzip"
    NONE = "none"
    SNAPPY = "snappy"
    ZSTD = "zstd"


_COMPRESSIONS_BY_FORMAT: dict[OutputFormat, frozenset[Compression]] = {
    OutputFormat.JSONL: frozenset(
        {Compression.BZIP2, Compression.GZIP, Compression.NONE}
    ),
    OutputFormat.PARQUET: frozenset(
        {Compression.GZIP, Compression.NONE, Compression.SNAPPY, Compression.ZSTD}
    ),
}

# (min, max) --compression-level Python's own gzip/bz2 modules accept for JSONL output.
_JSONL_LEVEL_RANGES: dict[Compression, tuple[int, int]] = {
    Compression.BZIP2: (1, 9),
    Compression.GZIP: (0, 9),
}


def _parquet_level_range(compression: Compression) -> tuple[int, int] | None:
    """(min, max) --compression-level pyarrow's Codec accepts for this codec, or None if it has none.

    Asks pyarrow directly rather than hard-coding ranges, since they're codec- (and pyarrow-version-)
    specific: gzip and bz2 differ from Python's stdlib modules, and zstd's range is nothing like 0-9.
    """
    if compression is Compression.NONE:
        return None
    import pyarrow as pa

    codec = compression.value
    if not pa.Codec.supports_compression_level(codec):
        return None
    return (
        pa.Codec.minimum_compression_level(codec),
        pa.Codec.maximum_compression_level(codec),
    )


def _validate_compression_level(
    output_format: OutputFormat, compression: Compression, level: int | None
) -> None:
    """Reject a --compression-level the codec can't use, before any conversion work starts."""
    if level is None:
        return
    level_range = (
        _JSONL_LEVEL_RANGES.get(compression)
        if output_format is OutputFormat.JSONL
        else _parquet_level_range(compression)
    )
    if level_range is None:
        console.print(
            f"[red]Error:[/] {compression.value} does not support --compression-level."
        )
        raise typer.Exit(1)
    lo, hi = level_range
    if not lo <= level <= hi:
        console.print(
            f"[red]Error:[/] --compression-level {level} is out of range for "
            f"{compression.value} ({lo}-{hi})."
        )
        raise typer.Exit(1)


CPUS = os.cpu_count() or 1

app: typer.Typer = typer.Typer(no_args_is_help=True)


def _version_callback(value: bool) -> None:
    if value:
        console.print(f"discogskit {version('discogskit')}")
        raise typer.Exit()


@app.callback()
def _callback(
    version: Annotated[
        bool | None,
        typer.Option(
            "--version",
            callback=_version_callback,
            help="Show version and exit.",
            is_eager=True,
        ),
    ] = None,
) -> None:
    """discogskit: Discogs Data Dumps Toolkit"""


def _resolve_jobs(paths: list[Path]) -> list[tuple[Path, str]]:
    """Resolve paths to a list of (gz_path, entity_name) pairs."""
    jobs: list[tuple[Path, str]] = []
    for p in paths:
        try:
            if p.is_dir():
                gz_files = sorted(p.glob("*.xml.gz"))
                if not gz_files:
                    console.print(f"[red]Error:[/] no .xml.gz files found in {p}")
                    raise typer.Exit(1) from None
                for gz in gz_files:
                    jobs.append((gz, detect_entity(gz.name)))
            else:
                if not p.exists():
                    console.print(f"[red]Error:[/] file not found: {p}")
                    raise typer.Exit(1) from None
                if p.name.endswith(".xml"):
                    console.print(
                        f"[red]Error:[/] {p}: uncompressed .xml input is not supported;"
                        " pass the original .xml.gz dump"
                    )
                    raise typer.Exit(1) from None
                if not p.name.endswith(".xml.gz"):
                    console.print(f"[red]Error:[/] {p}: not a .xml.gz dump")
                    raise typer.Exit(1) from None
                jobs.append((p, detect_entity(p.name)))
        except ValueError as exc:
            console.print(f"[red]Error:[/] {exc}")
            raise typer.Exit(1) from None
    return jobs


def _print_result(
    result: pipeline.PipelineResult,
    entity_name: str,
    entity_def,
    *,
    verb: str,
    verbose: bool = False,
) -> None:
    """Print the summary and optional profile for a completed entity."""
    rate = (
        f"{result.total_records / result.t_parse_load:,.0f} rec/s"
        if result.t_parse_load > 0
        else ""
    )
    console.print(
        f"  [green]✓[/] {result.total_records:,} {entity_name} {verb} "
        f"[dim][{result.t_total:.2f}s][/]"
    )

    if verbose:
        console.print()
        status("Decompress", f"{result.t_decompress:.2f}s")
        status(
            "Load",
            f"{result.t_parse_load:.2f}s  {rate}"
            if result.t_parse_load > 0
            else "0.00s",
        )
        status("Finalize", f"{result.t_indexes:.2f}s")
        status("Total", f"[bold]{result.t_total:.2f}s[/]")

    if result.profile_data:
        pd = result.profile_data
        console.print()
        console.rule("[bold]Profile[/]", style="dim")
        status("Put blocked", f"{pd['put_blocked']:.2f}s")
        status("Get wait", f"{pd['get_wait']:.2f}s")
        console.print()

        table_timings = cast(dict[str, float], pd.get("table_timings", {}))
        flush_total = sum(table_timings.values())

        tbl = Table(
            show_edge=False,
            title=f"Per-table flush ({flush_total:.2f}s)",
            title_style="bold",
        )
        tbl.add_column("Table", style="cyan")
        tbl.add_column("Time", justify="right")
        tbl.add_column("%", justify="right", style="dim")
        for key in entity_def.table_order + ["_commit"]:
            t = table_timings.get(key, 0.0)
            pct = t / flush_total * 100 if flush_total else 0
            label = "commit" if key == "_commit" else key
            tbl.add_row(label, f"{t:.2f}s", f"{pct:.1f}%")
        console.print(tbl)


def _run_jobs(
    jobs: list[tuple[Path, str]],
    writer: Writer,
    *,
    chunk_mb: int,
    keep_xml: bool,
    parse_workers: int,
    profile: bool,
    progress: bool,
    strict: bool,
    verb: str,
    write_queue: int,
) -> None:
    """Run the pipeline for each job, printing progress and handling errors.

    Builds a ``PipelineConfig`` per job (entity/gz_path vary, the rest is
    shared across the whole run), calls ``pipeline.run``, and prints the
    result. Closes ``writer`` once the whole run is done, whether it
    succeeded, failed, or was interrupted.
    """
    verbose = not progress or profile
    try:
        for gz_path, entity_name in jobs:
            console.print()
            console.print(
                f"[bold green]{entity_name.capitalize()}[/]  [dim]{gz_path.name}[/]"
            )

            config = pipeline.PipelineConfig(
                chunk_mb=chunk_mb,
                entity=entity_name,
                gz_path=gz_path,
                keep_xml=keep_xml,
                parse_workers=parse_workers,
                profile=profile,
                progress=progress,
                strict=strict,
                write_queue=write_queue,
            )
            result = pipeline.run(config, writer)

            entity_def = get_entity(entity_name)
            _print_result(
                result,
                entity_name,
                entity_def,
                verb=verb,
                verbose=verbose,
            )
    except (DecompressError, OutputExistsError) as exc:
        console.print(f"[red]Error:[/] {exc}")
        raise typer.Exit(1) from None
    except KeyboardInterrupt:
        console.print("\n  [yellow]Interrupted — cleaning up …[/]")
        raise typer.Exit(130) from None
    # Top-level error boundary: any failure becomes a one-line error and exit 1.
    except Exception as exc:  # noqa: BLE001
        console.print(f"[red]Error:[/] {exc}")
        raise typer.Exit(1) from None
    finally:
        writer.close()


@app.command()
def convert(
    paths: Annotated[
        list[Path],
        typer.Argument(help="One or more .xml.gz files or directories containing them"),
    ],
    # Output
    output_format: Annotated[
        OutputFormat,
        typer.Option("-f", "--format", case_sensitive=False, help="Output format"),
    ] = OutputFormat.PARQUET,
    output: Annotated[
        Path,
        typer.Option(help="Output directory"),
    ] = Path("."),
    compression: Annotated[
        Compression | None,
        typer.Option(
            help="Compression codec. Parquet: gzip, snappy, zstd (default), none. JSONL: bzip2, gzip, none (default)."
        ),
    ] = None,
    compression_level: Annotated[
        int | None,
        typer.Option(
            help=(
                "Compression level for codecs that support one: JSONL gzip/bzip2, Parquet gzip/zstd. "
                "Rejected for snappy and none. Default: JSONL gzip uses level 6 (faster, slightly "
                "larger output than 9; measured in #32); every other codec uses its own library default."
            )
        ),
    ] = None,
    # Performance tuning
    parse_workers: Annotated[
        int,
        typer.Option(help="Number of parallel parse workers", min=1),
    ] = max(1, CPUS // 2),
    chunk_mb: Annotated[
        int,
        typer.Option(help="Split XML into chunks of roughly this size (MB)", min=1),
    ] = 256,
    write_queue: Annotated[
        int,
        typer.Option(
            help="Max chunks buffered in memory before writes must catch up", min=1
        ),
    ] = 2,
    # Behavior
    keep_xml: Annotated[
        bool,
        typer.Option(help="Keep decompressed XML file after converting"),
    ] = False,
    overwrite: Annotated[
        bool,
        typer.Option(help="Overwrite existing output files"),
    ] = False,
    profile: Annotated[
        bool,
        typer.Option(help="Print detailed per-table timing breakdown after convert"),
    ] = False,
    progress: Annotated[
        bool,
        typer.Option(help="Show a progress bar instead of per-chunk output"),
    ] = True,
    strict: Annotated[
        bool,
        typer.Option(help="Warn about unhandled XML elements during parsing"),
    ] = False,
) -> None:
    """Convert Discogs XML dumps into flat files (Parquet or JSONL)."""
    from discogskit.writers.jsonl import JSONLWriter
    from discogskit.writers.parquet import ParquetWriter

    jobs = _resolve_jobs(paths)

    if compression is None:
        compression = (
            Compression.ZSTD
            if output_format is OutputFormat.PARQUET
            else Compression.NONE
        )

    valid_compressions = _COMPRESSIONS_BY_FORMAT[output_format]
    if compression not in valid_compressions:
        valid = ", ".join(
            sorted(c.value for c in valid_compressions if c is not Compression.NONE)
            + [Compression.NONE.value]
        )
        console.print(
            f"[red]Error:[/] unsupported compression '{compression.value}' for "
            f"{output_format.value}. Valid: {valid}."
        )
        raise typer.Exit(1)

    _validate_compression_level(output_format, compression, compression_level)

    if output_format is OutputFormat.PARQUET:
        writer = ParquetWriter(
            str(output),
            compression=compression.value,
            compression_level=compression_level,
            overwrite=overwrite,
        )
    else:
        writer = JSONLWriter(
            str(output),
            compression=compression.value,
            compression_level=compression_level,
            overwrite=overwrite,
        )

    _run_jobs(
        jobs,
        writer,
        chunk_mb=chunk_mb,
        keep_xml=keep_xml,
        parse_workers=parse_workers,
        profile=profile,
        progress=progress,
        strict=strict,
        verb="converted",
        write_queue=write_queue,
    )


@app.command()
def load(
    paths: Annotated[
        list[Path],
        typer.Argument(help="One or more .xml.gz files or directories containing them"),
    ],
    # Connection
    dsn: Annotated[
        str,
        typer.Option(
            envvar="DATABASE_URL",
            help="Database DSN (e.g., postgresql://localhost/postgres) or path to SQLite file",
        ),
    ] = "postgresql://localhost/discogskit",
    # Performance tuning
    parse_workers: Annotated[
        int,
        typer.Option(help="Number of parallel parse workers", min=1),
    ] = max(1, CPUS // 2),
    chunk_mb: Annotated[
        int,
        typer.Option(help="Split XML into chunks of roughly this size (MB)", min=1),
    ] = 256,
    write_queue: Annotated[
        int,
        typer.Option(
            help="Max chunks buffered in memory before writes must catch up", min=1
        ),
    ] = 2,
    # Behavior
    fk: Annotated[
        bool,
        typer.Option(help="Enforce foreign key constraints (SQLite and PostgreSQL)"),
    ] = False,
    keep_xml: Annotated[
        bool,
        typer.Option(help="Keep decompressed XML file after loading"),
    ] = False,
    overwrite: Annotated[
        bool,
        typer.Option(help="Overwrite existing tables in the database"),
    ] = False,
    profile: Annotated[
        bool,
        typer.Option(help="Print detailed per-table timing breakdown after load"),
    ] = False,
    progress: Annotated[
        bool,
        typer.Option(help="Show a progress bar instead of per-chunk output"),
    ] = True,
    strict: Annotated[
        bool,
        typer.Option(help="Warn about unhandled XML elements during parsing"),
    ] = False,
    # PostgreSQL options
    pg_unlogged: Annotated[
        bool,
        typer.Option(
            help="Skip WAL for faster writes (tables stay unlogged; data lost on crash)",
            rich_help_panel="PostgreSQL",
        ),
    ] = False,
    pg_create_schema: Annotated[
        bool,
        typer.Option(
            help="Create --pg-schema if it doesn't exist",
            rich_help_panel="PostgreSQL",
        ),
    ] = False,
    pg_schema: Annotated[
        str | None,
        typer.Option(
            help="Schema to create tables in (must exist unless --pg-create-schema)",
            rich_help_panel="PostgreSQL",
        ),
    ] = None,
    pg_write_workers: Annotated[
        int,
        typer.Option(
            help="Number of parallel database write workers",
            min=1,
            rich_help_panel="PostgreSQL",
        ),
    ] = 1,
    pg_index_workers: Annotated[
        int,
        typer.Option(
            help="Number of parallel index creation workers",
            min=1,
            rich_help_panel="PostgreSQL",
        ),
    ] = 2,
) -> None:
    """Load Discogs XML dumps into a database."""
    jobs = _resolve_jobs(paths)

    if pg_create_schema and not pg_schema:
        console.print("[red]Error:[/] --pg-create-schema requires --pg-schema.")
        raise typer.Exit(1)

    try:
        writer = get_writer(
            dsn,
            create_schema=pg_create_schema,
            fk=fk,
            index_workers=pg_index_workers,
            overwrite=overwrite,
            schema=pg_schema,
            unlogged=pg_unlogged,
            write_workers=pg_write_workers,
        )
    # Writer construction can fail many ways (bad DSN, driver, connection).
    except Exception as exc:  # noqa: BLE001
        console.print(f"[red]Error:[/] {exc}")
        raise typer.Exit(1) from None
    _run_jobs(
        jobs,
        writer,
        chunk_mb=chunk_mb,
        keep_xml=keep_xml,
        parse_workers=parse_workers,
        profile=profile,
        progress=progress,
        strict=strict,
        verb="loaded",
        write_queue=write_queue,
    )
