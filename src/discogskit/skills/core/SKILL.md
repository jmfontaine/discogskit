---
name: core
description: Core discogskit usage guide. Read this before running any discogskit command. Covers convert vs load, choosing entities and output formats, decompression and the XML lock, the table layout, common workflows, and troubleshooting.
allowed-tools: Bash(discogskit:*)
---

# discogskit

Converts the monthly [Discogs data dumps](https://www.discogs.com/data/) (`discogs_YYYYMMDD_<entity>.xml.gz`) into
Parquet or JSONL files, or loads them into SQLite or PostgreSQL. It reads local dumps only: download them first.

## Choose a command

| Goal | Command |
|---|---|
| Analyze with DuckDB, Polars, pandas or Spark; ship files | `discogskit convert` (Parquet, the default) |
| Stream records to another tool, line by line | `discogskit convert --format jsonl` |
| Query with SQL from a single file, no server | `discogskit load --dsn discogs.db` (SQLite) |
| Query with SQL on a server, or join with other data there | `discogskit load --dsn postgresql://...` |

Both commands take one or more `.xml.gz` dumps or directories. A directory means every `*.xml.gz` in it.

## Choose entities

The entity comes from the file name (`discogs_20260301_releases.xml.gz` → `releases`), so keep Discogs' names. The
four entities are `artists`, `labels`, `masters` and `releases`. Pass only the dumps the task needs: `releases` is
by far the largest, with 12 tables, and takes most of the time of a full run (around half an hour on a laptop
into PostgreSQL).

A run takes at most one dump per entity: two months of one entity in a directory, a directory plus one of its
files, or the same file twice stop the run before any work (`two inputs for artists: <a> and <b>`).

The entities don't reference each other through constraints: `releases.master_id`, `release_artists.artist_id` or
`release_labels.label_id` hold Discogs ids, which match rows only when that entity was loaded too. Load all four
when a query crosses entities.

## Output

- `convert` writes one file per table: `<output>/<entity>/<table>.parquet`, or `.jsonl` (`.jsonl.gz`,
  `.jsonl.bz2` with `--compression`). Parquet defaults to `zstd`, JSONL to no compression. Files appear only once
  every table of the entity is complete.
- `load --dsn postgresql://user@host/db` (or the `DATABASE_URL` environment variable; the default is
  `postgresql://localhost/discogskit`) creates the tables in the current schema, or in `--pg-schema`. PostgreSQL
  14+.
- `load --dsn discogs.db` (a path ending in `.db`, `.sqlite` or `.sqlite3`, or `sqlite:///<path>`) writes a SQLite
  file.

Existing output stops the run before anything is written, naming it: pass `--overwrite` to replace it.

Never put a database password on the command line, where `ps` and the shell history show it: use `~/.pgpass` or
`DATABASE_URL`.

## Tables

Each entity has a root table (`artists`, `labels`, `masters`, `releases`) keyed by `id`, and child tables linked to
it by `<entity>_id` (`artist_id`, `label_id`, `master_id`, `release_id`). `load` indexes those link columns. Every
table and column, with its types per format: [references/schemas.md](references/schemas.md).

- **Values are as the dump has them.** A value missing from the XML is `NULL` (`null` in JSONL); a text value
  present but empty is `""`. A list column is `NULL` without its element (no `<genres>`), an empty list for an
  empty one (`<genres/>`), and keeps an empty item as `""` (`<genre/>`). Typed values are never coerced: a
  non-numeric id, or a video `duration` or `embed` that isn't an integer or `true`/`false` (an empty one
  included), stops the run.
- **Placeholders are kept, not cleaned.** Seen in real dumps, with likely meanings Discogs doesn't document: `0` in
  `releases.master_id` (no master), `0` in `masters.year` (unknown year), `0` as an artist or company id in a link
  table (no linked entity). Filter them where they matter, e.g. `WHERE master_id <> 0`.
- **Dates are text.** `releases.released` is the dump's string, such as `1994-03-08`, `1999-03-00` (likely an
  unknown day) or `2000`; don't assume it is a full date.
- **List columns** (`genres`, `styles`, `namevariations`, `urls`, `descriptions`) are Arrow lists in Parquet,
  arrays in JSONL, `TEXT[]` in PostgreSQL and compact JSON text in SQLite (query with `json_each()`).
- **Tracks** are flat: `release_tracks.track_idx` numbers a release's tracks from 0 in the dump's order, and a
  sub-track's `parent_idx` is its parent's `track_idx`. Track artists and track credits join on
  `(release_id, track_idx)`.
- **Credits** are in `release_extraartists` (whole release, or the positions in `tracks`) and
  `release_track_extraartists` (one track); `role` is free text, sometimes several roles in one value
  (`Producer, Recorded By`): match it with `LIKE '%Producer%'`, not `=`.

Example queries for SQLite, PostgreSQL, Parquet and JSONL: [references/queries.md](references/queries.md).

## Decompression and the XML lock

Both commands decompress each `.xml.gz` to a `.xml` next to it, so that directory needs free space for the XML,
which is much larger than the `.gz`. The `.xml` is deleted after a successful run unless `--keep-xml` is set.

- A run that fails or is interrupted after decompression keeps the `.xml`, and the next run reuses it.
- A truncated, corrupt or non-gzip `.xml.gz` stops the run with
  `failed to decompress <name> (truncated, corrupt, or not a gzip file)`; when the truncation makes the decompressor
  hang, that comes after 60 seconds without output. Check the `.xml.gz` against the matching month's
  `discogs_YYYYMMDD_CHECKSUM.txt` first.
- Runs on the same dump can overlap (a `convert` and a `load`): one decompresses while the others wait, then all
  read the same `.xml`. They coordinate through `<name>.xml.lock`, which stays next to the dump; delete it only
  while no run uses that dump.
- discogskit reuses or deletes only a `.xml` it wrote itself, recorded in a `<name>.xml.discogskit` marker. A
  `.xml` without a marker (from `gunzip -k`, or an older discogskit) stops the run with
  `<name>.xml exists but wasn't written by discogskit; move or delete it`. Ask the user before deleting it.

## Workflows

```bash
# Every dump in a directory to Parquet, under ./parquet/<entity>/
discogskit convert --output parquet ~/discogs/

# Releases only, to gzipped JSONL
discogskit convert --format jsonl --compression gzip --output jsonl discogs_20260301_releases.xml.gz

# Everything into SQLite
discogskit load --dsn discogs.db ~/discogs/

# Into PostgreSQL, faster: unlogged tables (lost on a server crash), 4 write and 4 index workers
discogskit load --dsn postgresql://user@localhost/discogs --pg-unlogged \
  --pg-write-workers 4 --pg-index-workers 4 ~/discogs/

# Into a PostgreSQL schema of its own, replacing an earlier load
discogskit load --pg-schema discogs_202603 --pg-create-schema --overwrite ~/discogs/

# Refresh to a newer month: same command, newer dumps, --overwrite
discogskit load --dsn discogs.db --overwrite ~/discogs/2026-04/
```

A run on a full dump takes minutes to tens of minutes: run it in the background or with a long timeout. When its
output isn't a terminal, it prints one line per chunk as it's written, then a summary per entity.

Tuning: `--parse-workers` (default half the CPU count) parses chunks in parallel; `--chunk-mb` sets their size;
`--write-queue` bounds how far parsing runs ahead of writing. Peak memory follows `--chunk-mb` and `--parse-workers`:
loading releases with the defaults peaked at about 9.6 GiB across discogskit's processes, and `--chunk-mb 64` at about
6.3 GiB; `--write-queue` barely changed it. `--profile` prints a per-table timing
breakdown. `--strict` lists the XML elements no parser handled, which discogskit would otherwise drop, then fails the
run: use it to check a new dump after Discogs changes the format.

## Troubleshooting

Errors print `Error: <message>` and exit 1; Ctrl+C exits 130.

| Message | Cause and fix |
|---|---|
| `Cannot detect entity from '<file>'` | The file name isn't `discogs_YYYYMMDD_<entity>.xml.gz`; rename it |
| `two inputs for <entity>` / `same file given twice` | One dump per entity per run; split into several runs |
| `Output files already exist in ...` / `Tables already exist in ...` | Pass `--overwrite`, or another `--output`, database or `--pg-schema` |
| `Can't overwrite tables in schema ...: other objects depend on them` | Your views or foreign keys use the tables; the message lists them. Drop them, or load into another schema |
| `<name>.xml exists but wasn't written by discogskit` | A foreign `.xml` next to the dump; move it away (ask first) |
| `unsupported compression '<codec>' for <format>` | Parquet takes `zstd`, `snappy`, `gzip`, `none`; JSONL `gzip`, `bzip2`, `none` |
| `--pg-... can only be used with PostgreSQL, not a SQLite DSN` | Drop the `--pg-*` flags for SQLite |
| `Duplicate <entity> id: <n>` or another value error naming a chunk and line | The dump has a record discogskit can't store as is; nothing is coerced. Report it with the message |
| `--strict: <n> unhandled XML element paths in <record>` | `--strict` found elements discogskit doesn't parse, listed on the `Strict` lines before it. Rerun without `--strict` to load anyway, dropping them, and report the paths |

After a failed or interrupted `load`, the tables are incomplete: run it again with `--overwrite`. With
`--pg-write-workers` above 1, each table group commits separately, so a failed load can leave part of a chunk in some
tables: re-run the whole load with `--overwrite`.

`convert` writes into an `<entity>.partial-*` directory and moves the files into place only once all are complete,
so a run that fails before that leaves earlier output untouched. If the move itself fails, files already moved
sit next to the older ones, and the error names the `.partial-*` directory that holds the rest: move them by hand,
or run again with `--overwrite`. A `.partial-*` directory left by a killed run, with no run still writing to it, is
safe to delete.

## Reference docs

| Document | Content |
|---|---|
| [references/commands.md](references/commands.md) | Every command and option, with defaults |
| [references/schemas.md](references/schemas.md) | Every table and column, with its Parquet, PostgreSQL and SQLite type |
| [references/ddl.md](references/ddl.md) | The `CREATE TABLE`, key and index statements `load` runs |
| [references/queries.md](references/queries.md) | Example queries over each output |
