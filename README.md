# discogskit

A fast tool for converting and loading [Discogs data dumps](https://www.discogs.com/data/) into Parquet, JSONL, SQLite,
and PostgreSQL.

## Why discogskit?

- **Fast.** Parallel parsing and writing squeeze maximum performance out of your machine. See [Benchmarks](#benchmarks)
    for numbers.
- **Easy to use.** A single command does the job. No multi-step workflows, no manual schema setup.
- **Flexible outputs.** Convert to Parquet or JSONL for quick analysis without standing up a database, or load directly
    into SQLite or PostgreSQL.
- **Reliable.** Comprehensive unit and integration tests run against every release.

## Installation

Requires Python 3.11+. Tested on Linux and macOS; Windows support is untested.

```bash
pipx install discogskit
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
# Install locally
uv tool install discogskit

# Run without installing
uvx discogskit
```

### AI coding agent skill

Install the skill so your AI coding agent can find and drive discogskit:

```bash
npx skills add jmfontaine/discogskit
```

The installed skill is a thin pointer. The usage guide ships inside the package, so it always matches the installed
version: `discogskit skills get core` prints it, `discogskit skills get core --full` adds every command and option,
the table schemas, the DDL `load` runs and example queries, and `discogskit skills path` prints where the files are.

## Usage

```text

 Usage: discogskit [OPTIONS] COMMAND [ARGS]...

 discogskit: Discogs Data Dumps Toolkit

╭─ Options ────────────────────────────────────────────────────────────────────────────────────╮
│ --version                     Show version and exit.                                         │
│ --install-completion          Install completion for the current shell.                      │
│ --show-completion             Show completion for the current shell, to copy it or customize │
│                               the installation.                                              │
│ --help                        Show this message and exit.                                    │
╰──────────────────────────────────────────────────────────────────────────────────────────────╯
╭─ Commands ───────────────────────────────────────────────────────────────────────────────────╮
│ convert  Convert Discogs XML dumps into flat files (Parquet or JSONL).                       │
│ load     Load Discogs XML dumps into a database.                                             │
│ skills   List or print the bundled agent skills: usage guides matching this version.         │
╰──────────────────────────────────────────────────────────────────────────────────────────────╯

```

Each run takes at most one dump per entity. A directory holding two months of one entity, a directory plus one of its
files, or the same file given twice stops the run before any work, e.g. with `two inputs for artists: <a> and <b>`.
Before the first entity starts, the run also checks every entity's output: without `--overwrite`, existing output for
any of them stops the run before anything is written.

The progress bar shows only on a terminal. When output goes to a pipe, a file or CI, a run prints one line per chunk as
it's written instead, as with `--no-progress`. With `--strict`, once an entity's records are parsed, the run prints a
`Strict` line for each path no parser handled: an element, such as `unhandled XML element <videos/video/foo> in
<master>`, an attribute, such as `unhandled XML attribute <videos/video/@foo> in <master>`, or text other than
whitespace that no parser reads, such as `unhandled XML text <urls/text()> in <artist>`: any in an element whose text
no parser reads, before its first child or after any, and any after a child in one whose text is read up to that child.
It then fails with `--strict: 1 unhandled XML path in <master>; see the Strict lines above` and exits 1. Like any
failed run, it stops before finishing that entity: `convert` doesn't move its files into the output directory, and
`load` builds no indexes, though the rows it wrote stay in the tables. Entities earlier in
the run are already done; later ones don't run. Without `--strict`, those elements, attributes and text are dropped
silently.

Both commands decompress each `.xml.gz` to a `.xml` next to it, and delete the `.xml` after a successful run unless
`--keep-xml` is set. A run that fails or is interrupted (including Ctrl+C) after decompression finished keeps the
`.xml`, and the next run reuses it instead of decompressing again. One stopped during decompression leaves no `.xml`.
A truncated, corrupt or non-gzip `.xml.gz` stops the run with `failed to decompress <name> (truncated, corrupt, or
not a gzip file)`, followed by what the decompressor printed about it, if anything. Some truncated files make the
decompressor hang instead of failing: a decompression that produces no output for 60 seconds is reported the same way.
Runs on the same dump can overlap, e.g. a `convert` and a `load`: one decompresses while the others wait, then they all
read the same `.xml`. A run that finishes while another still reads it leaves it in place for that run. They coordinate
through a `<name>.xml.lock` file, which stays next to the dump; delete it only while no run is using that dump.

discogskit only reuses or deletes a `.xml` it decompressed itself. Next to each one it writes a `<name>.xml.discogskit`
marker recording the `.xml.gz` it came from (path, size and modification time), and removes the marker along with the
`.xml`. A `.xml` is reused only when its marker matches the `.xml.gz` named on the command line; otherwise, such as a
`--keep-xml` copy from an older dump of the same name, or an empty `.xml`, it is decompressed again. Size and
modification time are a freshness check, not a content check: a different `.xml.gz` with the same path, size and
modification time would not be noticed. A `.xml` without a marker, such as one made with `gunzip -k`, is never read,
replaced or deleted: the run stops with `<name>.xml exists but wasn't written by discogskit; move or delete it`. The
same applies to a `--keep-xml` copy left by an older discogskit version, which wrote no marker.

In every output format, a value missing from the XML is `NULL` (`null` in JSONL), while a text value that is present but
empty is an empty string: `<country/>` gives `""`, no `<country>` element gives `NULL`. List columns follow the same
rule: no `<genres>` element gives `NULL`, and an empty `<genre/>` stays in the list as `""`. Only each record's own
`id` and the keys linking child tables to their parent (`release_id`, `track_idx`, ...) are never `NULL`; ids that
reference other entities, such as `artist_id` or `label_id`, are `NULL` when the dump doesn't give one.

discogskit stores values exactly as the dump has them, including values that look like placeholders rather than real
data. Seen in the 2026-10-01 dump (other dumps may differ); the meanings are interpretations, not documented by
Discogs:

| Value | Where | Likely meaning |
|---|---|---|
| `0` | `releases.master_id` | the release has no master (no master has id 0) |
| `0` | `masters.year` | the year is unknown |
| `0` | `artist_id` in `release_extraartists`, `release_track_artists` and `release_track_extraartists` | no linked Discogs entity |
| `65535` | `duration` in `master_videos` and `release_videos` | possibly a value capped at the 16-bit maximum upstream rather than a real duration |

`release_artists.artist_id` and `release_companies.company_id` hold no `0` in that dump: a release artist or company
without an id has `NULL` there.

Filter them in your queries where they matter, e.g. `WHERE master_id <> 0`.

No output guarantees a row order: use `ORDER BY` wherever order matters, and always before `LIMIT`. discogskit
writes each chunk of records as soon as a parse worker finishes it, so chunks land in the order the workers finish
them. In two conversions of the 2026-10-01 releases dump to Parquet, each file's 234 row groups came out in a
different order (10 positions differed), and the first run's weren't in id order. The artists, labels and masters
outputs weren't checked for id order.

A record with a missing or empty id stops the run, and so do a release sub-track with `<sub_tracks>` of its own
(discogskit reads one level of sub-tracks), a non-empty `<tracks>` in a track's or sub-track's extra artist (only
release-level extra artists have a `tracks` column) and a second copy of an element discogskit reads one of, such as
two `<title>` in a record, two `<artists>` in a track or two `<name>` in an artist credit (keeping one would drop the
other); the 2026-10-01 dumps repeat none of them. A repeated `<labels>`, `<series>`, `<formats>`, `<identifiers>`,
`<videos>` or `<companies>` in a release, `<aliases>`, `<groups>` or `<members>` in an artist, `<sublabels>` in a label,
or `<artists>` or `<videos>` in a master is read in full instead, its items added to the others'. Typed values are never
coerced: every id, a master's `main_release` and `year`, a release's `master_id` and a video's `duration` must be an
integer from -2147483648 to 2147483647 written in ASCII digits with an optional leading `-`, so a `+`, an underscore,
whitespace or another script's digits fail; `embed` and `is_main_release` must be `true` or `false`. A missing value
is `NULL`, except a record's id, and any other value, including an empty one (`<year/>` or `id=""`), stops the run.
The error names the `.xml` file, the byte range of the chunk being parsed, the record's line, counted from the
chunk's first byte, its id when it has one, and the value's field, as in `invalid alias id '+7', expected an integer
from -2147483648 to 2147483647`. When the record fails for one of the elements above (a repeated one, a nested
`<sub_tracks>` or a track-level `<tracks>` value), the error adds that element's line, or `unknown` when it can't be
found.

Only records and whitespace may appear directly in a dump's container (`<artists>`, `<labels>`, `<masters>` or
`<releases>`), plus comments and processing instructions after the first record, which are skipped as they are inside
records. Anything else between two records, such as an element, text or a namespaced record like `<artist
xmlns="urn:x">`, stops the run, since discogskit would drop it. The error names the `.xml` file, the byte range of the
chunk being parsed and the content's line, counted from the chunk's first byte, e.g. `text 'STRAY' on line 120; only
<artist> records may appear in <artists>`. Content before the first record, comments included, and such content after
the last stop the run too, with its byte offset.

### Memory

A run's memory goes mostly to its parse workers, each holding the chunk it parses as Python objects and then as
Arrow tables, and to the main process, which holds the parsed chunks waiting to be written. `--chunk-mb` and
`--parse-workers` set the peak; a larger `--write-queue` lets more parsed chunks wait in the main process. Loading a
6.4 GB stretch of the 2026-10-01 releases dump (2,019,143 releases) into a local PostgreSQL 18 on an Apple M3 (8
cores, 24 GB RAM, macOS), once per setting, discogskit's processes together peaked at:

| Options | All processes | Main process | Largest parse worker |
|---|---:|---:|---:|
| defaults (two runs) | 3.9 GiB, 4.4 GiB | 1.5 GiB, 1.4 GiB | 1.1 GiB, 1.2 GiB |
| `--chunk-mb 64` | 1.7 GiB | 0.6 GiB | 0.3 GiB |
| `--chunk-mb 128` | 2.4 GiB | 0.8 GiB | 0.5 GiB |
| `--chunk-mb 512` | 7.6 GiB | 2.4 GiB | 1.8 GiB |
| `--parse-workers 2` | 3.2 GiB | 1.2 GiB | 1.2 GiB |
| `--parse-workers 8` | 6.4 GiB | 1.8 GiB | 1.1 GiB |
| `--write-queue 1` | 4.3 GiB | 1.3 GiB | 1.1 GiB |
| `--write-queue 4` | 4.4 GiB | 1.7 GiB | 1.2 GiB |
| `--write-queue 8` | 5.2 GiB | 1.6 GiB | 1.1 GiB |
| `--pg-write-workers 3` | 4.6 GiB | 1.4 GiB | 1.1 GiB |

The defaults are 4 parse workers, 256 MiB chunks, a write queue of 2 and 1 write worker. These are peak footprints
(macOS `phys_footprint`, which Activity Monitor shows as Memory), summed over the main process and its parse workers
every 0.25 s; the two default runs differ by 0.5 GiB, so smaller differences are noise. On macOS, a worker keeps
memory it has freed for reuse, so its footprint grows over its first few chunks: at 256 MiB, from about 0.9 GiB during
its first chunk to about 1.1 GiB by its tenth. On Linux (Python 3.13 in Docker on the same Mac), the same worker's
peak resident set stayed at about 0.9 GiB from its first chunk to its tenth; the table above wasn't measured there.
Sending each table's Arrow buffer to the main process as it is, instead of a pickled copy, cut that worker's peak from
2.0 GiB to 1.1 GiB on macOS but left it unchanged on Linux
([#31](https://github.com/jmfontaine/discogskit/issues/31)).
`/usr/bin/time -l` reports something else: the largest single process's peak resident set, which leaves out memory
the system has compressed. Converting the whole releases dump (62.7 GB of XML) to Parquet with the defaults peaked at
5.7 GiB, 1.2 GiB of it in the main process.

### discogskit convert

Convert Discogs XML dumps into flat files.

Each entity gets its own directory, e.g. `<output>/artists/artists.parquet`. Files are written to a temporary
directory and moved into place, one at a time, only after all of them are complete. If a run fails before that
point, no files appear under their final names and any previous output stays unchanged, including with
`--overwrite`. Don't run two conversions of the same entity into the same output directory at once: their files can
end up mixed. A process killed mid-run can leave an `<entity>.partial-*` directory behind; it's safe to delete. A
failed run removes the output directory (and its parents) if it created them and they're still empty; directories
that existed before stay.

For JSONL, existing output means a `.jsonl`, `.jsonl.gz` or `.jsonl.bz2` file in `<output>/<entity>/`, whichever
`--compression` the run uses, so a reader globbing `*.jsonl*` doesn't read two runs' files. For the same reason,
once the new files are in place, `--overwrite` also deletes the entity's table files of the other codecs, e.g.
`artists.jsonl` after a `--compression gzip` run. It deletes only files named after one of the entity's tables with
one of those three extensions; anything else in the directory stays.

Before it decompresses anything, a run checks that it can create files in the output directory (or, if that doesn't
exist yet, in its nearest existing parent) and move them into an existing `<output>/<entity>/`, and fails early if it
can't (e.g. a directory isn't writable, or a parent is a file). If moving the finished files fails anyway, the run keeps
them: the error names the `<entity>.partial-*` directory that holds the files not moved yet, which can then be
moved by hand. Files moved before the failure are already in `<output>/<entity>/`, next to any older files.

| Option | Values |
|-----------------|-------------------------------|
| Output formats | `parquet`, `jsonl` |
| Compression (Parquet) | `zstd` (default), `snappy`, `gzip`, `brotli`, `none` |
| Compression (JSONL) | `gzip`, `bzip2`, `none` (default) |

`--compression-level` overrides the codec's default level. Valid ranges come from each library: JSONL gzip 0-9
(defaults to **6**: on the 2026-10-01 dump, gzip output was 4.15% larger at level 6 than 9 across all four
entities, for roughly 56-68% less compression CPU — see [#32](https://github.com/jmfontaine/discogskit/issues/32)),
JSONL bzip2 1-9, Parquet brotli 0-11, Parquet gzip 1-9, Parquet zstd -131072 to 22. `snappy` and `none` don't take a
level and reject one, as does JSONL without `--compression`, which is uncompressed. Codec names are lowercase: any
other name, such as `GZIP` or `lz4`, fails with `unsupported compression`.

Converting the 2026-10-01 releases dump (62.7 GB of XML) to Parquet with each codec at its default level, on an Apple
M3 (8 cores, 24 GB RAM, macOS) and otherwise default options:

| Codec (default level) | Output | Run time | Parquet writes |
|---|---:|---:|---:|
| `zstd` (1) | 9,434 MiB | 840 s | 192 s |
| `gzip` (9) | 8,326 MiB (−12%) | 1,886 s | 1,677 s |
| `brotli` (8) | 7,625 MiB (−19%) | 1,126 s | 934 s |

The Parquet writes run on one thread, which compresses every row group, so once a codec makes them slower than
parsing, the whole run slows with them: `gzip` took more than twice as long as `zstd`. `brotli` makes the smallest
files; `gzip` makes larger files than `brotli`, and more slowly.

Parquet `zstd` defaults to pyarrow's level 1, not zstd's own default of 3, and a Parquet file records its codec but
not its level. In another series of single runs on the same machine, `--compression-level 3` wrote 8,572 MiB (−9%)
and `--compression-level 9` 8,087 MiB (−14%), against 9,434 MiB at level 1. Leaving decompression out, those two
runs took 807 s and 855 s, and three runs at level 1 took 689 s, 840 s (the table above) and 859 s (with `--strict`):
with one run per level and that much spread between runs, what the higher levels cost in time isn't measured.

<details>
<summary>Full command help</summary>

```text

 Usage: discogskit convert [OPTIONS] {paths}...

 Convert Discogs XML dumps into flat files (Parquet or JSONL).

╭─ Arguments ──────────────────────────────────────────────────────────────────────────────────╮
│ *    paths      <path>  One or more .xml.gz files or directories containing them [required]  │
╰──────────────────────────────────────────────────────────────────────────────────────────────╯
╭─ Options ────────────────────────────────────────────────────────────────────────────────────╮
│ --compression                              <codec>             Compression codec. Parquet:   │
│                                                                brotli, gzip, snappy, zstd    │
│                                                                (default), none. JSONL:       │
│                                                                bzip2, gzip, none (default).  │
│ --compression-level                        <int>               Compression level for codecs  │
│                                                                that support one: JSONL       │
│                                                                gzip/bzip2, Parquet           │
│                                                                brotli/gzip/zstd. Rejected    │
│                                                                for snappy and none. Default: │
│                                                                JSONL gzip uses level 6       │
│                                                                (faster, slightly larger      │
│                                                                output than 9); every other   │
│                                                                codec uses its own library    │
│                                                                default.                      │
│ --format             -f                    <jsonl|parquet>     Output format                 │
│                                                                [default: parquet]            │
│ --output                                   <path>              Output directory [default: .] │
│ --chunk-mb                                 <int range> [x>=1]  Split XML into chunks of      │
│                                                                roughly this size (MiB)       │
│                                                                [default: 256]                │
│ --parse-workers                            <int range> [x>=1]  Number of parallel parse      │
│                                                                workers                       │
│                                                                [default: (half the CPU       │
│                                                                count, at least 1)]           │
│ --write-queue                              <int range> [x>=1]  Chunks parsing may run ahead  │
│                                                                of writing: at most           │
│                                                                --parse-workers plus this     │
│                                                                many chunks are parsing or    │
│                                                                waiting to be written         │
│                                                                [default: 2]                  │
│ --keep-xml               --no-keep-xml                         Keep decompressed XML file    │
│                                                                after converting              │
│                                                                [default: no-keep-xml]        │
│ --overwrite              --no-overwrite                        Overwrite existing output     │
│                                                                files                         │
│                                                                [default: no-overwrite]       │
│ --profile                --no-profile                          Print detailed per-table      │
│                                                                timing breakdown after        │
│                                                                convert                       │
│                                                                [default: no-profile]         │
│ --progress               --no-progress                         Show a progress bar instead   │
│                                                                of per-chunk output           │
│                                                                [default: progress]           │
│ --strict                 --no-strict                           Fail on unhandled XML         │
│                                                                elements, attributes and      │
│                                                                text, after listing them all  │
│                                                                [default: no-strict]          │
│ --help                                                         Show this message and exit.   │
╰──────────────────────────────────────────────────────────────────────────────────────────────╯

```

</details>

#### JSONL output

Each JSONL file holds one record per line: a JSON object with the table's columns as keys, in the table's column
order, and list columns as arrays. Every line ends in `\n`, on every platform. Separators are compact (`,` and `:`,
no spaces). Text is UTF-8, with non-ASCII characters written as they are, except U+2028, U+2029 and U+0085: JSON
allows them raw, but Python's `str.splitlines()` and some editors and log tools treat them as line breaks, so they're
escaped as `\u2028`, `\u2029` and `\u0085`, which decode to the same text. gzip output records no creation time or
file name in its header, so the same rows in the same order give the same bytes. Rows are written in the order the
parse workers finish their chunks.

DuckDB and Polars read uncompressed and gzipped JSONL, but not bzip2: on a `.jsonl.bz2` file, DuckDB fails with
`Malformed JSON` and Polars with `stream did not contain valid UTF-8`. `jq` reads one through `bzip2 -dc`; for DuckDB
and Polars, decompress it first, or convert with `--compression gzip`. Both infer column types from the data: Polars
from the first 100 lines, which fails on sparse columns such as `release_tracks.parent_idx`, and both mistype a column
with no value in the whole file, such as `releases.status` in the 2026-10-01 dump (Polars as `Null`, DuckDB as
`JSON`). Pass the types instead: the agent skill's
[example queries](src/discogskit/skills/core/references/queries.md#jsonl) show how.

JSONL is much slower to write than Parquet. One writer thread serializes, and compresses, every row, so the parse
workers wait on it: with `gzip` at level 6 they were blocked for 2,622 of the run's 2,986 s. Converting the
2026-10-01 releases dump (19,492,392 records) on a quiet Apple M3, before the compact separators described above
(which made the uncompressed labels file of the same dump 6.5% smaller):

| Codec (level) | Run time | Output |
|---|---:|---:|
| `none` | 2,142–2,248 s | 71,879 MiB |
| `gzip` (1) | 2,477 s | 13,136 MiB |
| `gzip` (6, default) | 2,986 s | 11,141 MiB |
| `gzip` (9) | 4,828 s | 10,675 MiB |
| `bzip2` (9, default) | 9,085 s | 7,507 MiB |

Peak memory was about 4 GiB summed over the processes, 5.7 GiB with `--strict`. Parquet with `zstd` took 840 s on
the same dump (see above), so prefer Parquet for large dumps. For JSONL, prefer `gzip` at level 1 to 6 over `bzip2`
unless size matters most: `bzip2` takes three times as long as `gzip` at level 6, and DuckDB and Polars can't read it.

#### Examples

```shell
# Convert releases to Parquet (default)
discogskit convert --format parquet discogs_20260301_releases.xml.gz

# Convert to JSONL with gzip compression
discogskit convert --format jsonl --compression gzip discogs_20260301_artists.xml.gz

# Convert to JSONL with gzip at a specific level instead of the default (6)
discogskit convert --format jsonl --compression gzip --compression-level 9 discogs_20260301_artists.xml.gz

# Convert all dump files in the current directory
discogskit convert --format parquet .

# Keep decompressed XML after converting
discogskit convert --format parquet --keep-xml discogs_20260301_releases.xml.gz
```

### discogskit load

Load Discogs XML dumps into a database.

In PostgreSQL, tables are created in the current schema — the first existing schema on `search_path` (usually `public`),
or the schema given with `--pg-schema` (which must already exist, unless `--pg-create-schema` is also passed).
`--pg-schema` adds `search_path` to the libpq `options` of the write and index connections, after the options the DSN
sets or, if it sets none, those in `PGOPTIONS`, so settings such as `-c maintenance_work_mem=4GB` apply to every
connection. Without `--overwrite`, the load stops if any of its tables already exist there. With `--overwrite`, the
existing tables are dropped and recreated, all in one transaction; if your own views or foreign keys depend on them, the
load stops and lists those objects instead of dropping them. The write connections open before anything is dropped, so a
load that can't open them (for example, when the server reaches `max_connections`) leaves the existing tables as they
were.

`--pg-write-workers` splits each entity's tables into groups, one per worker, and writes each group on its own
connection. An entity with fewer tables than workers gets one worker per table. With more than one worker, each group
commits separately, so a failed load can leave part of a chunk in some tables: those of the groups that committed, but
not the others. When one group fails, the load waits for the other groups to finish before it reports the error.
After any failure, re-run the whole load with `--overwrite`.

In SQLite, the tables are likewise dropped and recreated in one transaction: if a drop fails (e.g. with `--fk`, a
table of your own references `artists` and has rows), the load stops and every table stays as it was. During the
load, the file is in WAL journal mode with `synchronous=OFF`, for speed, so an OS crash or power loss mid-load can
corrupt it. When the load finishes, discogskit sets `synchronous=FULL`, checkpoints the WAL into the database file,
and switches the file back to the journal mode it had before (rollback-journal `delete` for a new file); a failed
load tries the same before it exits. The finished database is then durable. A new file, or one that wasn't in WAL
mode before, then opens without `-wal`/`-shm` files, even from a read-only directory; a file that was already in WAL
mode stays in WAL mode, with WAL's usual side files. List columns (e.g. `namevariations`, `genres`) are stored as
compact JSON text with non-ASCII characters as-is, e.g. `["Björk","ビョーク"]`, so `LIKE` matches them;
`json_each()` reads them as arrays.

At the end of a SQLite load, SQLite builds the indexes by sorting in temporary files rather than in memory, so Ctrl+C
stops a build at once instead of after it. These files need free disk space in SQLite's temporary directory: up to
2.1 GiB for the 2026-10-01 releases dump, while its largest index (`release_tracks`) builds. SQLite deletes each file
as soon as it opens it, so they don't show up in the directory, and their space comes back once the index is built.
On Linux and macOS, the directory is the first of `$SQLITE_TMPDIR`, `$TMPDIR`, `/var/tmp`, `/usr/tmp`, `/tmp` and the
current directory that exists and is writable: a variable naming a missing directory is skipped. To put the files on
another disk, set `SQLITE_TMPDIR` to a directory there, e.g.
`SQLITE_TMPDIR=/mnt/scratch discogskit load --dsn discogs.db discogs_20261001_releases.xml.gz`.

After a failed or interrupted `load` (crash, Ctrl+C, lost connection), re-run it with `--overwrite`: the tables
aren't consistent until a load finishes. Chunks commit as they go, so earlier chunks stay in the tables after a
failure. With `--pg-write-workers 1` (the default) and with SQLite, a chunk commits in a single transaction on one
connection, so it's never partially written, but earlier, already-committed chunks are still not rolled back. With
`--pg-write-workers` above 1, the chunk that was in progress can itself end up partially written, as described above.

On Ctrl+C, a PostgreSQL load cancels the write in progress on the server instead of waiting for it, so it stops
promptly even when that write waits on a lock held by another session. The cancelled transaction commits nothing,
though with `--pg-write-workers` above 1, other table groups may already have committed their part of that chunk. A
server that doesn't answer the cancel request at all can still hold the load until its write ends.

| Database | Versions |
|----------|----------|
| SQLite | 3.x |
| PostgreSQL | 14+ |

`--dsn` accepts `postgresql://` or `postgres://` (both work identically) for PostgreSQL. A DSN libpq can't parse, such
as one with an unencoded `%` in the password (write it `%25`), or one with a value that isn't valid UTF-8, such as
`%ff`, is rejected before connecting, with an error that shows the DSN with its password and query string hidden. For
SQLite, it accepts `sqlite:///<path>` (`sqlite:///discogs.db` is relative to the current directory,
`sqlite:////data/discogs.db` is absolute) or a plain path ending in `.db`, `.sqlite`, or `.sqlite3`. Other `sqlite:`
forms, paths containing `?` (SQLite URI parameters such as `?mode=ro` aren't supported) and in-memory databases
(`sqlite:///:memory:`, `file::memory:`), which would discard the load when the run ends, are rejected before any file is
created. A database file that doesn't exist yet is created only once the input has been decompressed and checked, so a
run that fails before that, such as on a truncated `.xml.gz` or Ctrl+C during decompression, leaves none; the run still
fails before any work if the file's directory can't hold it.

<details>
<summary>Full command help</summary>

```text

 Usage: discogskit load [OPTIONS] {paths}...

 Load Discogs XML dumps into a database.

╭─ Arguments ──────────────────────────────────────────────────────────────────────────────────╮
│ *    paths      <path>  One or more .xml.gz files or directories containing them [required]  │
╰──────────────────────────────────────────────────────────────────────────────────────────────╯
╭─ Options ────────────────────────────────────────────────────────────────────────────────────╮
│ --dsn                                <str>               Database DSN (e.g.,                 │
│                                                          postgresql://localhost/postgres) or │
│                                                          path to SQLite file                 │
│                                                          [env var: DATABASE_URL]             │
│                                                          [default:                           │
│                                                          postgresql://localhost/discogskit]  │
│ --chunk-mb                           <int range> [x>=1]  Split XML into chunks of roughly    │
│                                                          this size (MiB)                     │
│                                                          [default: 256]                      │
│ --parse-workers                      <int range> [x>=1]  Number of parallel parse workers    │
│                                                          [default: (half the CPU count, at   │
│                                                          least 1)]                           │
│ --write-queue                        <int range> [x>=1]  Chunks parsing may run ahead of     │
│                                                          writing: at most --parse-workers    │
│                                                          plus this many chunks are parsing   │
│                                                          or waiting to be written            │
│                                                          [default: 2]                        │
│ --fk               --no-fk                               Enforce foreign key constraints     │
│                                                          (SQLite and PostgreSQL)             │
│                                                          [default: no-fk]                    │
│ --keep-xml         --no-keep-xml                         Keep decompressed XML file after    │
│                                                          loading                             │
│                                                          [default: no-keep-xml]              │
│ --overwrite        --no-overwrite                        Overwrite existing tables in the    │
│                                                          database                            │
│                                                          [default: no-overwrite]             │
│ --profile          --no-profile                          Print detailed per-table timing     │
│                                                          breakdown after load                │
│                                                          [default: no-profile]               │
│ --progress         --no-progress                         Show a progress bar instead of      │
│                                                          per-chunk output                    │
│                                                          [default: progress]                 │
│ --strict           --no-strict                           Fail on unhandled XML elements,     │
│                                                          attributes and text, after listing  │
│                                                          them all                            │
│                                                          [default: no-strict]                │
│ --help                                                   Show this message and exit.         │
╰──────────────────────────────────────────────────────────────────────────────────────────────╯
╭─ PostgreSQL ─────────────────────────────────────────────────────────────────────────────────╮
│ --pg-create-schema    --no-pg-create-schema                        Create --pg-schema if it  │
│                                                                    doesn't exist             │
│                                                                    [default:                 │
│                                                                    no-pg-create-schema]      │
│ --pg-index-workers                             <int range> [x>=1]  Number of parallel index  │
│                                                                    creation workers          │
│                                                                    [default: 2]              │
│ --pg-schema                                    <str>               Schema to create tables   │
│                                                                    in (must exist unless     │
│                                                                    --pg-create-schema)       │
│ --pg-unlogged         --no-pg-unlogged                             Skip WAL for faster       │
│                                                                    writes (tables stay       │
│                                                                    unlogged; data lost on    │
│                                                                    crash)                    │
│                                                                    [default: no-pg-unlogged] │
│ --pg-write-workers                             <int range> [x>=1]  Number of parallel        │
│                                                                    database write workers.   │
│                                                                    Above 1, each table group │
│                                                                    commits separately, so a  │
│                                                                    failed load can leave     │
│                                                                    part of a chunk in some   │
│                                                                    tables: re-run the whole  │
│                                                                    load with --overwrite     │
│                                                                    [default: 1]              │
╰──────────────────────────────────────────────────────────────────────────────────────────────╯

```

</details>

#### Examples

```shell
# Load releases into PostgreSQL (default DSN: postgresql://localhost/discogskit)
discogskit load discogs_20260301_releases.xml.gz

# Load into a specific PostgreSQL database. Keep the password off the command line
# (it would show up in `ps` and shell history): put it in ~/.pgpass (or $PGPASSFILE)
# for libpq to read, and use a passwordless DSN.
discogskit load --dsn "postgresql://user@localhost/discogs" discogs_20260301_releases.xml.gz

# Or set DATABASE_URL instead of --dsn, loaded from a secrets file rather than typed,
# so the password never hits shell history either.
export DATABASE_URL="$(cat ~/.discogskit-dsn)"
discogskit load discogs_20260301_releases.xml.gz

# Load into SQLite
discogskit load --dsn discogs.db discogs_20260301_releases.xml.gz

# Load all dump files from a directory
discogskit load --dsn discogs.db .

# Use UNLOGGED tables for faster PostgreSQL writes (~2x speedup)
discogskit load --pg-unlogged discogs_20260301_releases.xml.gz

# Enforce foreign key constraints (SQLite and PostgreSQL)
discogskit load --fk discogs_20260301_releases.xml.gz

# Use multiple write workers for parallel PostgreSQL inserts
discogskit load --pg-write-workers 4 discogs_20260301_releases.xml.gz

# Load into a specific schema, creating it if it doesn't exist
discogskit load --pg-schema discogs --pg-create-schema discogs_20260301_releases.xml.gz
```

#### PostgreSQL tuning

`discogskit load` doesn't tune the server: `--pg-tune` used to run `ALTER SYSTEM SET max_wal_size = '16GB'` before
the load and `ALTER SYSTEM RESET max_wal_size` on close, but `RESET` deletes the setting instead of restoring
whatever value the user had before, it needed superuser, changed a server-wide setting from a data-loading tool,
and a killed process (`SIGKILL`, OOM, or a timed out `close()`) left the tuned value on the server permanently
([#26](https://github.com/jmfontaine/discogskit/issues/26)).

Raising `max_wal_size` is optional and can reduce checkpoint I/O on the server during a load; it did **not** make the
load faster in our measurement. Loading the 2026-10-01 dump (all four entities) into a local PostgreSQL 18 on a 24 GB
Apple Silicon Mac, one run per setting (1GB first), fresh database each time:

| `max_wal_size` | load time | checkpoints requested / timed / completed | buffers written by checkpoints |
|---|---:|---:|---:|
| 1GB | 43.9 min | 182 / 0 / 181 | 423,976 |
| 16GB | 53.0 min | 7 / 6 / 13 | 3,518 |

Both runs wrote about 117 GB of WAL. With a single run each, in a fixed order, neither difference is proven to be
caused by the setting; the time difference may be run-to-run variation, and the drop in checkpoints is consistent
with the larger WAL limit. So consider it if checkpoint I/O matters on your server (e.g. other workloads share it),
not as a speed-up. Apply it yourself before the load, either in `postgresql.conf` or with `ALTER SYSTEM`
(requires superuser; reload the server to apply):

```sql
ALTER SYSTEM SET max_wal_size = '16GB';
SELECT pg_reload_conf();
```

Afterwards, undo exactly what you changed. If you had already set `max_wal_size` with `ALTER SYSTEM` before (check
`postgresql.auto.conf`), set it back to that value: `ALTER SYSTEM SET max_wal_size = '<previous value>';`.
Otherwise remove the override with `ALTER SYSTEM RESET max_wal_size;`; the value from `postgresql.conf` (or the
built-in default) applies again. Either way, follow it with `SELECT pg_reload_conf();`, then check `SHOW max_wal_size;`.

## Python API

discogskit can also run from Python. The supported API is what the `discogskit` package exports; every other module
and name may change in any release.

| Name | What it is |
|---|---|
| `run(config, writer, *, console=None)` | Runs one dump through the pipeline and returns a `PipelineResult` |
| `PipelineConfig` | `run()`'s settings: the dump, its entity, and the options `convert` and `load` take |
| `PipelineResult` | Records loaded and seconds spent per stage |
| `ProfileData` | The timings in `PipelineResult.profile_data` when `PipelineConfig.profile` is set: seconds the writer waited for parsed chunks (`get_wait`), seconds parsing waited for the writer (`put_blocked`) and seconds spent writing each table (`table_timings`, with any shared commit time under `_commit`) |
| `get_writer(dsn, ...)` | A SQLite or PostgreSQL `Writer` for a DSN, with `load`'s options as keyword arguments |
| `get_file_writer(output_dir, output_format, ...)` | A Parquet (`"parquet"`) or JSONL (`"jsonl"`) `Writer` that writes under `output_dir`, with `convert`'s `compression`, `compression_level` and `overwrite` options as keyword arguments |
| `Writer` | The protocol every writer implements |
| `ENTITIES` | Every entity (`artists`, `labels`, `masters`, `releases`) by name, as an `EntityDef` |
| `EntityDef` | One entity's tables and Arrow schemas |
| `detect_entity(filename)` | The entity a Discogs dump file name holds, e.g. `artists` |
| `OutputExistsError` | Raised when the output exists and the writer doesn't overwrite it |
| `DecompressError` | Raised when a `.xml.gz` is truncated, corrupt or not gzip |
| `ForeignXmlError` | Raised when the `.xml` next to the dump wasn't written by discogskit; move or delete it |
| `UnhandledElementsError` | Raised by a `strict` run whose input has XML elements, attributes or text no parser handled, before the writer's `finalize()`; its `entity` and `paths` attributes name them |

`get_writer()` and `get_file_writer()` raise `ValueError` for an option they can't use, such as a codec the format
doesn't support, a compression level that isn't an `int` or is out of the codec's range, a `write_workers` below 1 or
`create_schema` without `schema`, before creating any file or connecting to PostgreSQL. `run()` raises `ValueError` for
a `PipelineConfig` with a `chunk_mb` or `parse_workers` below 1 or a negative `write_queue`, before it decompresses the
dump or touches the output.
Other failures raise other exceptions, e.g. `ValueError` for a value discogskit can't store.

```python
from pathlib import Path

from rich.console import Console

import discogskit

if __name__ == "__main__":
    gz_path = Path("discogs_20260301_artists.xml.gz")
    config = discogskit.PipelineConfig(
        chunk_mb=256,
        entity=discogskit.detect_entity(gz_path.name),
        gz_path=gz_path,
        keep_xml=False,
        parse_workers=4,
        profile=False,
        progress=False,
        strict=False,
        write_queue=2,
    )
    # Writes output/artists/*.parquet. Pass "jsonl" for JSONL files instead, or use
    # discogskit.get_writer("discogs.db") to load a SQLite database.
    writer = discogskit.get_file_writer("output", "parquet", overwrite=True)
    try:
        result = discogskit.run(config, writer, console=Console(quiet=True))
    finally:
        writer.close()
    print(f"{result.total_records:,} artists in {result.t_total:.1f}s")
```

Decompression and the parse workers run in separate processes started with `spawn`, which imports the calling script
again in each one: keep the code that calls `run()` under `if __name__ == "__main__":`. Without it, the first of those
processes calls `run()` again, which raises a `RuntimeError` there, so the run fails at once, whether or not the XML is
already decompressed, with an error that names this requirement: a `RuntimeError` when decompression runs, or a
`BrokenProcessPool` when the XML is already decompressed and a parse worker starts first.

`run()` prints the status and progress lines the CLI shows during a run, to stdout by default. Pass a
[Rich](https://rich.readthedocs.io/) `Console` as `console` to change that: `Console(quiet=True)` prints nothing, and
`Console(file=...)` writes the output to a file or a buffer of your own. The progress bar shows only when that console
is a terminal, `config.progress` is set and `config.profile` isn't. The console covers everything the run prints,
including the messages of the writer's `setup()` and `finalize()`. It doesn't cover the writer's `close()`, which
prints only when a PostgreSQL connection doesn't close in time.

## Benchmarks

Full load of the `20260301` data dump (artists, labels, masters, releases) into PostgreSQL 18
on a 24 GB Apple MacBook Air M3.

| | discogs-xml2db Python | discogs-xml2db .NET | discogskit | discogskit `--pg-unlogged` |
|---|---:|---:|---:|---:|
| Parse + load | 0:59:11 | 1:01:08 | 18:55 | 9:25 |
| Indexes | 0:43:24 | 0:43:24 | 14:53 | 1:56 |
| **Total** | **1:42:35** | **1:44:32** | **33:49** | **11:22** |
| **Speedup** | **baseline** | **0.98x** | **3.0x** | **9.0x** |

<details>
<summary>Commands and detailed output</summary>

**discogs-xml2db Python**

```shell
python3 run.py --apicounts --export artist --export label --export master --export release --output ./csv-dir [path]
python3 postgresql/psql.py < postgresql/sql/CreateTables.sql
python3 postgresql/importcsv.py ./csv-dir/*
python3 postgresql/psql.py < postgresql/sql/CreatePrimaryKeys.sql
python3 postgresql/psql.py < postgresql/sql/CreateFKConstraints.sql
python3 postgresql/psql.py < postgresql/sql/CreateIndexes.sql
```

| Step | Time |
|---|---:|
| Export to CSV | 0:41:14 |
| Table creation | 0:00:01 |
| Data import | 0:17:56 |
| Primary keys | 0:19:21 |
| Foreign keys | 0:02:12 |
| Indexes | 0:21:51 |
| **Total** | **1:42:35** |

**discogs-xml2db .NET**

```shell
discogs [paths]
python3 postgresql/psql.py < postgresql/sql/CreateTables.sql
python3 postgresql/importcsv.py ./csv-dir/*
python3 postgresql/psql.py < postgresql/sql/CreatePrimaryKeys.sql
python3 postgresql/psql.py < postgresql/sql/CreateFKConstraints.sql
python3 postgresql/psql.py < postgresql/sql/CreateIndexes.sql
```

| Step | Time |
|---|---:|
| Export to CSV | 0:43:11 |
| Table creation | 0:00:01 |
| Data import | 0:17:56 |
| Primary keys | 0:19:21 |
| Foreign keys | 0:02:12 |
| Indexes | 0:21:51 |
| **Total** | **1:44:32** |

**discogskit**

```shell
discogskit load --dsn postgresql://localhost:5432/discogskit --chunk-mb 256 \
  --parse-workers 6 --pg-write-workers 3 --pg-index-workers 6 [path]
```

| Entity | Records | Parse + load | Indexes | Total |
|---|---:|---:|---:|---:|
| Artists | 9,957,079 | 40.40s | 14.77s | 55.15s |
| Labels | 2,349,729 | 9.27s | 0.67s | 9.95s |
| Masters | 2,530,697 | 34.45s | 19.94s | 54.36s |
| Releases | 18,952,204 | 1,051.37s | 857.89s | 1,909.24s |
| **Total** | **33,789,709** | **1,135.49s** | **893.27s** | **2,028.70s** |

**discogskit `--pg-unlogged`**

```shell
discogskit load --dsn postgresql://localhost:5432/discogskit --chunk-mb 256 --pg-unlogged \
  --parse-workers 6 --pg-write-workers 3 --pg-index-workers 6 [path]
```

| Entity | Records | Parse + load | Indexes | Total |
|---|---:|---:|---:|---:|
| Artists | 9,957,079 | 27.73s | 2.52s | 30.22s |
| Labels | 2,349,729 | 8.84s | 0.51s | 9.34s |
| Masters | 2,530,697 | 23.38s | 1.93s | 25.29s |
| Releases | 18,952,204 | 505.45s | 111.46s | 616.86s |
| **Total** | **33,789,709** | **565.40s** | **116.42s** | **681.71s** |

</details>

### On GitHub Actions

The [Benchmark workflow](.github/workflows/benchmark.yml) runs discogskit on a standard `ubuntu-24.04` runner (4 vCPU,
16 GB RAM), which anyone can reproduce. Start it from the Actions tab, or with `gh workflow run benchmark.yml`. It
benchmarks the first million releases of the latest dump by default, into JSONL, Parquet, SQLite and PostgreSQL 18,
3 times each. Inputs select the dump date, entities, record count, targets and repeats.

The full dump doesn't fit on the runner's disk, so `benchmarks/subset.py` streams the start of each dump and stops
after N records, so only that part is downloaded. It closes the root element after the last record so the subset
is a valid dump. The workflow caches the subset by dump date and record count, and records its SHA-256: Discogs'
checksum covers the whole `.gz`, so it can't verify a partial download.

`benchmarks/ci.py` times each run and reads its peak memory from a dedicated cgroup's `memory.peak`, which covers
every process discogskit starts, plus the page cache they fill; the PostgreSQL server runs in its own container
and isn't included. Every run starts with the page cache dropped, so repeats are equally cold. Downloading, cutting
and resetting the target stay outside the timed sections. The run page shows the medians with the fastest and slowest
runs, and the `benchmark-results` artifact holds every run's numbers and logs, with the runner image, CPU model, dump
date, subset SHA-256, commit and tool versions.

Compare numbers only within one run: shared runners vary from run to run, and so can their CPU model. A subset is the
start of the dump, not a random sample, so its throughput may not match the full dump's.


## Development

[just](https://github.com/casey/just) runs the development tasks: `just --list` shows them all.

### Releasing

Between releases, `main` carries the next version as a [PEP 440](https://peps.python.org/pep-0440/) development
release, such as `0.2.0.dev0`, which sorts below `0.2.0`, so installers never prefer a build from `main` over a
release. `main` only changes through pull requests, so a release takes three steps, each run from a clean `main` equal
to `origin/main`:

1. `just release-prepare` opens a pull request that sets the release version (`0.2.0.dev0` → `0.2.0`) and changes
   nothing else. Merging it is the go/no-go decision.
2. After it merges, update `main` and run `just release`. It refuses a development or release-candidate version and an
   existing tag, creates the signed tag `v0.2.0` on the release commit, and pushes only the tag. The tag starts the
   [release workflow](.github/workflows/release.yml), which checks that the tag matches the version, publishes to
   PyPI and creates the GitHub release, and `just release` waits for it.
3. `just release-next` opens a pull request that starts the next minor version's development (`0.2.0` →
   `0.3.0.dev0`). Pass `patch` or `major` for `0.2.1.dev0` or `1.0.0.dev0` instead.

Each recipe takes `--dry-run`, which runs its checks and prints what it would do without creating a branch, a tag or a
pull request.

## License

discogskit is licensed under the [Apache License 2.0](LICENSE.txt).
