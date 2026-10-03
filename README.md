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

Requires Python 3.10+. Tested on Linux and macOS; Windows support is untested.

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
╰──────────────────────────────────────────────────────────────────────────────────────────────╯

```

Both commands decompress each `.xml.gz` to a `.xml` next to it, and delete the `.xml` after a successful run unless
`--keep-xml` is set. A run that fails or is interrupted (including Ctrl+C) after decompression finished keeps the
`.xml`, and the next run reuses it instead of decompressing again. One stopped during decompression leaves no `.xml`.
Runs on the same dump can overlap, e.g. a `convert` and a `load`: one decompresses while the others wait, then they all
read the same `.xml`. A run that finishes while another still reads it leaves it in place for that run. They coordinate
through a `<name>.xml.lock` file, which stays next to the dump; delete it only while no run is using that dump.

In every output format, a value missing from the XML is `NULL` (`null` in JSONL), while a text value that is present but
empty is an empty string: `<country/>` gives `""`, no `<country>` element gives `NULL`. List columns follow the same
rule: no `<genres>` element gives `NULL`, and an empty `<genre/>` stays in the list as `""`. Only IDs and the keys
linking child tables to their parent are never `NULL`.

### discogskit convert

Convert Discogs XML dumps into flat files.

Each entity gets its own directory, e.g. `<output>/artists/artists.parquet`. Files are written to a temporary
directory and moved into place, one at a time, only after all of them are complete. If a run fails before that
point, no files appear under their final names and any previous output stays unchanged, including with
`--overwrite`. Don't run two conversions of the same entity into the same output directory at once: their files can
end up mixed. A process killed mid-run can leave an `<entity>.partial-*` directory behind; it's safe to delete.

| Option | Values |
|-----------------|-------------------------------|
| Output formats | `parquet`, `jsonl` |
| Compression (Parquet) | `zstd` (default), `snappy`, `gzip`, `none` |
| Compression (JSONL) | `gzip`, `bzip2`, `none` (default) |

<details>
<summary>Full command help</summary>

```text

 Usage: discogskit convert [OPTIONS] {paths}...

 Convert Discogs XML dumps into flat files (Parquet or JSONL).

╭─ Arguments ──────────────────────────────────────────────────────────────────────────────────╮
│ *    paths      <path>  One or more .xml.gz files or directories containing them [required]  │
╰──────────────────────────────────────────────────────────────────────────────────────────────╯
╭─ Options ────────────────────────────────────────────────────────────────────────────────────╮
│ --format         -f                    <jsonl|parquet>            Output format              │
│                                                                   [default: parquet]         │
│ --output                               <path>                     Output directory           │
│                                                                   [default: .]               │
│ --compression                          <bzip2|gzip|none|snappy|z  Compression codec.         │
│                                        std>                       Parquet: gzip, snappy,     │
│                                                                   zstd (default), none.      │
│                                                                   JSONL: bzip2, gzip, none   │
│                                                                   (default).                 │
│ --parse-workers                        <int range> [x>=1]         Number of parallel parse   │
│                                                                   workers                    │
│                                                                   [default: 4]               │
│ --chunk-mb                             <int range> [x>=1]         Split XML into chunks of   │
│                                                                   roughly this size (MB)     │
│                                                                   [default: 256]             │
│ --write-queue                          <int range> [x>=1]         Max chunks buffered in     │
│                                                                   memory before writes must  │
│                                                                   catch up                   │
│                                                                   [default: 2]               │
│ --keep-xml           --no-keep-xml                                Keep decompressed XML file │
│                                                                   after converting           │
│                                                                   [default: no-keep-xml]     │
│ --overwrite          --no-overwrite                               Overwrite existing output  │
│                                                                   files                      │
│                                                                   [default: no-overwrite]    │
│ --profile            --no-profile                                 Print detailed per-table   │
│                                                                   timing breakdown after     │
│                                                                   convert                    │
│                                                                   [default: no-profile]      │
│ --progress           --no-progress                                Show a progress bar        │
│                                                                   instead of per-chunk       │
│                                                                   output                     │
│                                                                   [default: progress]        │
│ --strict             --no-strict                                  Warn about unhandled XML   │
│                                                                   elements during parsing    │
│                                                                   [default: no-strict]       │
│ --help                                                            Show this message and      │
│                                                                   exit.                      │
╰──────────────────────────────────────────────────────────────────────────────────────────────╯

```

</details>

#### Examples

```shell
# Convert releases to Parquet (default)
discogskit convert --format parquet discogs_20260301_releases.xml.gz

# Convert to JSONL with gzip compression
discogskit convert --format jsonl --compression gzip discogs_20260301_artists.xml.gz

# Convert all dump files in the current directory
discogskit convert --format parquet .

# Keep decompressed XML after converting
discogskit convert --format parquet --keep-xml discogs_20260301_releases.xml.gz
```

### discogskit load

Load Discogs XML dumps into a database.

In PostgreSQL, tables are created in the current schema — the first existing schema on `search_path` (usually
`public`), or the schema given with `--pg-schema` (which must already exist, unless `--pg-create-schema` is also
passed). Without `--overwrite`, the load stops if any of its tables already exist there. With `--overwrite`, the
existing tables are dropped and recreated, all in one transaction; if your own views or foreign keys depend on them,
the load stops and lists those objects instead of dropping them.

After a failed or interrupted `load` (crash, Ctrl+C, lost connection), re-run it with `--overwrite`: the tables
aren't consistent until a load finishes. Chunks commit as they go, so earlier chunks stay in the tables after a
failure. With PostgreSQL and `--write-workers` above its default of 1, each table group commits on its own
connection, so the chunk that was in progress when the failure happened can itself end up partially written — some
of its tables committed, others not. With `--write-workers 1` (the default) and with SQLite, a chunk commits in a
single transaction on one connection, so it's never partially written, but earlier, already-committed chunks are
still not rolled back.

| Database | Versions |
|----------|----------|
| SQLite | 3.x |
| PostgreSQL | 14+ |

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
│ --parse-workers                      <int range> [x>=1]  Number of parallel parse workers    │
│                                                          [default: 4]                        │
│ --chunk-mb                           <int range> [x>=1]  Split XML into chunks of roughly    │
│                                                          this size (MB)                      │
│                                                          [default: 256]                      │
│ --write-queue                        <int range> [x>=1]  Max chunks buffered in memory       │
│                                                          before writes must catch up         │
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
│ --strict           --no-strict                           Warn about unhandled XML elements   │
│                                                          during parsing                      │
│                                                          [default: no-strict]                │
│ --help                                                   Show this message and exit.         │
╰──────────────────────────────────────────────────────────────────────────────────────────────╯
╭─ PostgreSQL ─────────────────────────────────────────────────────────────────────────────────╮
│ --pg-unlogged         --no-pg-unlogged                             Skip WAL for faster       │
│                                                                    writes (tables stay       │
│                                                                    unlogged; data lost on    │
│                                                                    crash)                    │
│                                                                    [default: no-pg-unlogged] │
│ --pg-create-schema    --no-pg-create-schema                        Create --pg-schema if it  │
│                                                                    doesn't exist             │
│                                                                    [default:                 │
│                                                                    no-pg-create-schema]      │
│ --pg-schema                                    <str>               Schema to create tables   │
│                                                                    in (must exist unless     │
│                                                                    --pg-create-schema)       │
│ --pg-write-workers                             <int range> [x>=1]  Number of parallel        │
│                                                                    database write workers    │
│                                                                    [default: 1]              │
│ --pg-index-workers                             <int range> [x>=1]  Number of parallel index  │
│                                                                    creation workers          │
│                                                                    [default: 2]              │
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

To get the same effect, apply the setting yourself before a large load, either in `postgresql.conf` or with
`ALTER SYSTEM` (requires superuser; reload or restart the server to apply):

```sql
ALTER SYSTEM SET max_wal_size = '16GB';
SELECT pg_reload_conf();
```

Afterwards, restore whatever value you had before the load — `ALTER SYSTEM SET max_wal_size = '<previous value>';`
— or, if you had never set it, remove the override with `ALTER SYSTEM RESET max_wal_size;`. Either way, follow it
with `SELECT pg_reload_conf();`.

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


## License

discogskit is licensed under the [Apache License 2.0](LICENSE.txt).
