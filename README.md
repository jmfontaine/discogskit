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

Requires Python 3.10+.

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

╭─ Options ────────────────────────────────────────────────────────────────────────────╮
│ --version                     Show version and exit.                                 │
│ --install-completion          Install completion for the current shell.              │
│ --show-completion             Show completion for the current shell, to copy it or   │
│                               customize the installation.                            │
│ --help                        Show this message and exit.                            │
╰──────────────────────────────────────────────────────────────────────────────────────╯
╭─ Commands ───────────────────────────────────────────────────────────────────────────╮
│ convert  Convert Discogs XML dumps into flat files (Parquet or JSONL).               │
│ load     Load Discogs XML dumps into a database.                                     │
╰──────────────────────────────────────────────────────────────────────────────────────╯

```

### discogskit convert

Convert Discogs XML dumps into flat files.

| Option | Values |
|-----------------|-------------------------------|
| Output formats | `parquet`, `jsonl` |
| Compression (Parquet) | `zstd` (default), `snappy`, `gzip`, `none` |
| Compression (JSONL) | `gzip`, `bzip2`, `none` (default) |

<details>
<summary>Full command help</summary>

```text

 Usage: discogskit convert [OPTIONS] PATHS...

 Convert Discogs XML dumps into flat files (Parquet or JSONL).

╭─ Arguments ──────────────────────────────────────────────────────────────────────────╮
│ *    paths      PATHS...  One or more .xml.gz files or directories containing them   │
│                           [required]                                                 │
╰──────────────────────────────────────────────────────────────────────────────────────╯
╭─ Options ────────────────────────────────────────────────────────────────────────────╮
│ --format         -f                    TEXT     Output format: parquet or jsonl      │
│                                                 [default: parquet]                   │
│ --output                               PATH     Output directory [default: .]        │
│ --compression                          TEXT     Compression codec. Parquet: gzip,    │
│                                                 snappy, zstd (default), none. JSONL: │
│                                                 bzip2, gzip, none (default).         │
│ --parse-workers                        INTEGER  Number of parallel parse workers     │
│                                                 [default: 4]                         │
│ --chunk-mb                             INTEGER  Split XML into chunks of roughly     │
│                                                 this size (MB)                       │
│                                                 [default: 256]                       │
│ --write-queue                          INTEGER  Max chunks buffered in memory before │
│                                                 writes must catch up                 │
│                                                 [default: 2]                         │
│ --keep-xml           --no-keep-xml              Keep decompressed XML file after     │
│                                                 converting                           │
│                                                 [default: no-keep-xml]               │
│ --overwrite          --no-overwrite             Overwrite existing output files      │
│                                                 [default: no-overwrite]              │
│ --profile            --no-profile               Print detailed per-table timing      │
│                                                 breakdown after convert              │
│                                                 [default: no-profile]                │
│ --progress           --no-progress              Show a progress bar instead of       │
│                                                 per-chunk output                     │
│                                                 [default: progress]                  │
│ --strict             --no-strict                Warn about unhandled XML elements    │
│                                                 during parsing                       │
│                                                 [default: no-strict]                 │
│ --help                                          Show this message and exit.          │
╰──────────────────────────────────────────────────────────────────────────────────────╯

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

| Database | Versions |
|----------|----------|
| SQLite | 3.x |
| PostgreSQL | 14+ |

<details>
<summary>Full command help</summary>

```text

 Usage: discogskit load [OPTIONS] PATHS...

 Load Discogs XML dumps into a database.

╭─ Arguments ──────────────────────────────────────────────────────────────────────────╮
│ *    paths      PATHS...  One or more .xml.gz files or directories containing them   │
│                           [required]                                                 │
╰──────────────────────────────────────────────────────────────────────────────────────╯
╭─ Options ────────────────────────────────────────────────────────────────────────────╮
│ --dsn                                TEXT     Database DSN (e.g.,                    │
│                                               postgresql://localhost/postgres) or    │
│                                               path to SQLite file                    │
│                                               [env var: DATABASE_URL]                │
│                                               [default:                              │
│                                               postgresql://localhost/discogskit]     │
│ --parse-workers                      INTEGER  Number of parallel parse workers       │
│                                               [default: 4]                           │
│ --write-workers                      INTEGER  Number of parallel database write      │
│                                               workers                                │
│                                               [default: 1]                           │
│ --index-workers                      INTEGER  Number of parallel index creation      │
│                                               workers                                │
│                                               [default: 2]                           │
│ --chunk-mb                           INTEGER  Split XML into chunks of roughly this  │
│                                               size (MB)                              │
│                                               [default: 256]                         │
│ --write-queue                        INTEGER  Max chunks buffered in memory before   │
│                                               writes must catch up                   │
│                                               [default: 2]                           │
│ --keep-xml         --no-keep-xml              Keep decompressed XML file after       │
│                                               loading                                │
│                                               [default: no-keep-xml]                 │
│ --overwrite        --no-overwrite             Overwrite existing tables in the       │
│                                               database                               │
│                                               [default: no-overwrite]                │
│ --profile          --no-profile               Print detailed per-table timing        │
│                                               breakdown after load                   │
│                                               [default: no-profile]                  │
│ --progress         --no-progress              Show a progress bar instead of         │
│                                               per-chunk output                       │
│                                               [default: progress]                    │
│ --strict           --no-strict                Warn about unhandled XML elements      │
│                                               during parsing                         │
│                                               [default: no-strict]                   │
│ --help                                        Show this message and exit.            │
╰──────────────────────────────────────────────────────────────────────────────────────╯
╭─ PostgreSQL ─────────────────────────────────────────────────────────────────────────╮
│ --pg-unlogged    --no-pg-unlogged      Skip WAL for faster writes (tables stay       │
│                                        unlogged; data lost on crash)                 │
│                                        [default: no-pg-unlogged]                     │
│ --pg-tune        --no-pg-tune          Temporarily apply settings optimized for bulk │
│                                        loading                                       │
│                                        [default: no-pg-tune]                         │
│ --pg-fk          --no-pg-fk            Add foreign key constraints after load        │
│                                        [default: no-pg-fk]                           │
╰──────────────────────────────────────────────────────────────────────────────────────╯

```

</details>

#### Examples

```shell
# Load releases into PostgreSQL (default DSN: postgresql://localhost/discogskit)
discogskit load discogs_20260301_releases.xml.gz

# Load into a specific PostgreSQL database
discogskit load --dsn "postgresql://user:pass@localhost/discogs" discogs_20260301_releases.xml.gz

# Load into SQLite
discogskit load --dsn discogs.db discogs_20260301_releases.xml.gz

# Load all dump files from a directory
discogskit load --dsn discogs.db .

# Use UNLOGGED tables for faster PostgreSQL writes (~2x speedup)
discogskit load --pg-unlogged discogs_20260301_releases.xml.gz

# Temporarily tune PostgreSQL for bulk loading
discogskit load --pg-tune discogs_20260301_releases.xml.gz

# Add foreign key constraints after load
discogskit load --pg-fk discogs_20260301_releases.xml.gz

# Use multiple write workers for parallel database inserts
discogskit load --write-workers 4 discogs_20260301_releases.xml.gz
```

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
  --parse-workers 6 --write-workers 3 --index-workers 6 [path]
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
  --parse-workers 6 --write-workers 3 --index-workers 6 [path]
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
