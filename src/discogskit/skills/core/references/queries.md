# Example queries

The ids below are examples: look ids up first (an artist by `name`, a release by `title`), never guess them. Table
and column details: [schemas.md](schemas.md).

Without `ORDER BY`, rows come back in no guaranteed order: discogskit writes each chunk of records as soon as a
worker has parsed it, so rows aren't in id order, and two conversions of the same releases dump to Parquet stored the
same rows in a different order. Add `ORDER BY` wherever order matters, and always before `LIMIT`.

`load` indexes each root table's `id` and each child table's link column (`release_id`, `artist_id`, ...) only.
A lookup by any other column, such as `release_labels.label_id` or `releases.master_id`, scans the table: for
repeated lookups, add an index first, e.g. `CREATE INDEX release_labels_label_id ON release_labels (label_id);`.

## SQLite

List columns are JSON text: expand them with `json_each()`.

Find an artist by name:

```sql
SELECT id, name, realname FROM artists WHERE name = 'Nine Inch Nails' ORDER BY id;
```

An artist's releases, with label and catalog number, undated releases last (`NULLS LAST`; SQLite and PostgreSQL
both take it):

```sql
SELECT r.id, r.title, r.released, r.country, rl.name AS label, rl.catno
FROM release_artists ra
JOIN releases r ON r.id = ra.release_id
LEFT JOIN release_labels rl ON rl.release_id = r.id
WHERE ra.artist_id = 3857
ORDER BY r.released NULLS LAST, r.id, label, rl.catno;
```

A release's tracklist; a sub-track's `parent_idx` is its parent's `track_idx`:

```sql
SELECT track_idx, parent_idx, position, title, duration
FROM release_tracks
WHERE release_id = 847868
ORDER BY track_idx;
```

Each track with its own artists, when the release credits artists per track:

```sql
SELECT t.position, t.title, ta.name AS artist
FROM release_tracks t
JOIN release_track_artists ta ON ta.release_id = t.release_id AND ta.track_idx = t.track_idx
WHERE t.release_id = 800013
ORDER BY t.track_idx, artist;
```

Every version of a master, the main release first (`master_id` isn't indexed; `is_main_release` can be `NULL`,
which PostgreSQL would sort first in a plain `DESC`):

```sql
SELECT id, title, country, released, is_main_release
FROM releases
WHERE master_id = 3719
ORDER BY CASE WHEN is_main_release THEN 1 ELSE 0 END DESC, released NULLS LAST, id;
```

Releases with a style (a list column):

```sql
SELECT r.id, r.title
FROM releases r, json_each(r.styles) s
WHERE s.value = 'Techno'
ORDER BY r.id
LIMIT 20;
```

Vinyl LPs: a format name plus one of its descriptions:

```sql
SELECT DISTINCT f.release_id
FROM release_formats f, json_each(f.descriptions) d
WHERE f.name = 'Vinyl' AND d.value = 'LP'
ORDER BY f.release_id
LIMIT 20;
```

Who produced a release (credits on the whole release; `tracks` lists positions when the credit is narrower):

```sql
SELECT name, artist_id, role, tracks
FROM release_extraartists
WHERE release_id = 847868 AND role LIKE '%Producer%'
ORDER BY name, artist_id, role, tracks;
```

Releases by barcode. Values are written as on the release, with spaces or dashes (`0 7464-63628-2 2`), so
compare them without:

```sql
SELECT r.id, r.title, i.value
FROM release_identifiers i
JOIN releases r ON r.id = i.release_id
WHERE i.type = 'Barcode' AND replace(replace(i.value, ' ', ''), '-', '') = '074646362822'
ORDER BY r.id, i.value;
```

A label's catalogue (`label_id` isn't indexed):

```sql
SELECT rl.catno, r.id, r.title, r.released
FROM release_labels rl
JOIN releases r ON r.id = rl.release_id
WHERE rl.label_id = 647
ORDER BY rl.catno, r.id;
```

A group's members:

```sql
SELECT m.member_id, m.name
FROM artists a
JOIN artist_members m ON m.artist_id = a.id
WHERE a.name = 'Nine Inch Nails'
ORDER BY m.member_id, m.name;
```

Releases per year from one country, undated ones last; `released` is text such as `1994-03-08`, so take its first
four characters:

```sql
SELECT substr(released, 1, 4) AS year, count(*) AS releases
FROM releases
WHERE country = 'US'
GROUP BY year
ORDER BY year NULLS LAST;
```

## PostgreSQL

Every SQLite query above runs unchanged on PostgreSQL, except those over list columns, which are `TEXT[]` arrays
here. PostgreSQL's `LIKE` is case-sensitive, unlike SQLite's for ASCII letters: use `ILIKE` to match `producer`
too.

```sql
-- Releases with a style
SELECT id, title FROM releases WHERE 'Techno' = ANY (styles) ORDER BY id LIMIT 20;

-- Vinyl LPs
SELECT DISTINCT release_id
FROM release_formats
WHERE name = 'Vinyl' AND 'LP' = ANY (descriptions)
ORDER BY release_id
LIMIT 20;

-- Most common styles
SELECT style, count(*) AS releases
FROM releases, unnest(styles) AS style
GROUP BY style
ORDER BY releases DESC, style
LIMIT 20;
```

With `--pg-schema`, qualify the tables (`discogs.releases`) or `SET search_path TO discogs;` first.

## Parquet

[DuckDB](https://duckdb.org/) queries the files in place; list columns are DuckDB lists:

```sql
SELECT r.id, r.title, t.position, t.title AS track
FROM 'parquet/releases/releases.parquet' r
JOIN 'parquet/releases/release_tracks.parquet' t ON t.release_id = r.id
WHERE r.id = 847868
ORDER BY t.track_idx;

SELECT id, title
FROM 'parquet/releases/releases.parquet'
WHERE list_contains(styles, 'Techno')
ORDER BY id
LIMIT 20;
```

Polars reads them too:
`pl.scan_parquet("parquet/releases/releases.parquet").filter(pl.col("country") == "US").sort("id").collect()`.

## JSONL

One JSON object per line, one line per row, with the table's columns as keys; list columns are arrays. `jq` reads the
lines in file order, which isn't id order: sort its output where order matters, which reads the whole file.

```bash
# The US releases with the lowest ids; @tsv escapes tabs and line breaks in titles
jq -r 'select(.country == "US") | [.id, .title] | @tsv' jsonl/releases/releases.jsonl | sort -n | head

# A gzipped file
gzip -dc jsonl/releases/releases.jsonl.gz \
  | jq -r 'select((.styles // []) | any(. == "Techno")) | [.id, .title] | @tsv' | sort -n | head
```

DuckDB reads JSONL, compressed or not: `SELECT count(*) FROM read_json('jsonl/releases/releases.jsonl.gz');`.
