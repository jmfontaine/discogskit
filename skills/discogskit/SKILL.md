---
name: discogskit
description: >
  Discogs data dump toolkit for AI agents. Converts the monthly Discogs XML
  dumps (artists, labels, masters, releases) into Parquet or JSONL, or loads
  them into SQLite or PostgreSQL, then queries the result. Use when asked to
  "convert Discogs dumps to Parquet", "load Discogs data into PostgreSQL",
  "load Discogs data into SQLite", "export releases to JSONL", "import the
  Discogs XML dump", "build a local Discogs database", "query the Discogs
  data dump", "analyze Discogs releases offline", or any task involving the
  Discogs data dumps (discogs_YYYYMMDD_*.xml.gz) rather than the Discogs API.
allowed-tools: Bash(discogskit:*)
---

# discogskit

Fast converter and loader for the Discogs data dumps: Parquet, JSONL, SQLite and PostgreSQL.

Install: `uv tool install discogskit` (or `pipx install discogskit`).

## Start here

This file is a discovery stub, not the usage guide. Before running any `discogskit` command, load the guide from
the CLI itself so the instructions match the installed version:

```bash
discogskit skills get core          # convert vs load, formats, tables, workflows, troubleshooting
discogskit skills get core --full   # plus every command and option, the table schemas, DDL and example queries
```

`discogskit skills` lists everything the installed version ships.
