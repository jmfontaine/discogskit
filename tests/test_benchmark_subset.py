"""Tests for benchmarks/subset.py: dump date resolution and cutting a dump after N records."""

import gzip
import hashlib
import io
from email.message import Message
from pathlib import Path
from urllib.response import addinfourl

import pytest

from benchmarks import subset
from benchmarks.subset import (
    Cut,
    SubsetError,
    cut,
    fetch,
    newest_complete,
    parse_listing,
    resolve,
)

FIXTURES = Path(__file__).parent / "fixtures"
PROLOG = b'<?xml version="1.0" encoding="UTF-8"?>\n<releases>\n'
# Bare and self-closing records, with ">" inside an attribute value and in text.
RECORDS = [
    b'<release id="1" status="Accepted"><title>a &gt; b</title></release>',
    b'<release id="2" status="Accepted" notes="x>y"/>',
    b'<release id="3" status="Accepted">\n  <tracklist><track><title>T</title></track></tracklist>\n</release>',
    b'<release id="4" status="Accepted" notes="&gt;"/>',
    b'<release id="5" status="Accepted"><title>Last</title></release>',
]


def _dump(separator: bytes = b"\n", records: list[bytes] = RECORDS) -> bytes:
    return PROLOG + separator.join(records) + separator + b"</releases>\n"


def _cut(
    data: bytes, records: int, chunk_size: int = 1 << 20, entity: str = "releases"
) -> tuple[bytes, Cut]:
    out = io.BytesIO()
    result = cut(io.BytesIO(data), out, entity, records, chunk_size=chunk_size)
    return out.getvalue(), result


class TestListing:
    def test_year_listing(self):
        dumps = parse_listing(
            (FIXTURES / "data_discogs_com_2026.html").read_text(), 2026
        )

        # CHECKSUM files and the parent directory link aren't dumps.
        assert sorted(dumps) == [f"2026{month:02d}01" for month in range(1, 11)]
        assert dumps["20261001"] == {
            entity: f"data/2026/discogs_20261001_{entity}.xml.gz"
            for entity in ("artists", "labels", "masters", "releases")
        }
        assert newest_complete(dumps, ["releases"]) == "20261001"

    def test_newest_complete_skips_dumps_missing_an_entity(self):
        dumps = {
            "20260901": {"artists": "a", "releases": "r"},
            "20261001": {"releases": "r"},
        }

        assert newest_complete(dumps, ["artists", "releases"]) == "20260901"
        assert newest_complete(dumps, ["labels"]) is None

    @pytest.mark.parametrize("date", ["20260231", "2026101", "2026-10-01", "recent"])
    def test_invalid_dump_date(self, date):
        with pytest.raises(SubsetError, match="invalid dump date"):
            resolve(date, ["releases"])


class TestCut:
    @pytest.mark.parametrize("chunk_size", [1, 7, 64, 1 << 20])
    @pytest.mark.parametrize("separator", [b"\n", b"", b"\n<!-- c -->\n"])
    @pytest.mark.parametrize("records", [1, 2, 3, 5])
    def test_keeps_the_first_records_verbatim_then_closes_the_root(
        self, chunk_size, separator, records
    ):
        subset, result = _cut(_dump(separator), records, chunk_size)

        expected = PROLOG + separator.join(RECORDS[:records]) + b"\n</releases>\n"
        assert subset == expected
        assert result == Cut(
            xml_bytes=len(expected), xml_sha256=hashlib.sha256(expected).hexdigest()
        )

    def test_fewer_records_than_the_limit(self):
        with pytest.raises(SubsetError, match="only 5 records, fewer than 6"):
            _cut(_dump(), 6)

    def test_truncated_dump(self):
        with pytest.raises(SubsetError, match="invalid XML after 2 records"):
            _cut(_dump()[: len(PROLOG) + 150], 4)

    def test_wrong_root_element(self):
        with pytest.raises(
            SubsetError, match="root element is <releases>, expected <artists>"
        ):
            _cut(_dump(), 1, entity="artists")

    def test_wrong_record_element(self):
        data = _dump(records=[RECORDS[0], b"<master id='1'/>"])
        with pytest.raises(
            SubsetError, match="<releases> contains <master>, expected <release>"
        ):
            _cut(data, 2)

    def test_declared_encoding_other_than_utf8(self):
        data = _dump().replace(b"UTF-8", b"ISO-8859-1", 1)
        with pytest.raises(SubsetError, match="declares encoding 'ISO-8859-1'"):
            _cut(data, 1)


class TestFetch:
    @pytest.mark.parametrize("failure", ["truncated download", "rename"])
    def test_failure_leaves_no_partial_file(self, tmp_path, monkeypatch, failure):
        data = gzip.compress(_dump())
        if failure == "truncated download":
            data = data[: len(data) // 2]
        else:

            def replace(self, target):
                raise PermissionError(13, "Permission denied", str(target))

            monkeypatch.setattr(Path, "replace", replace)
        # What urlopen returns for file: URLs: a file object with headers.
        monkeypatch.setattr(
            subset,
            "_open",
            lambda url: addinfourl(io.BytesIO(data), Message(), url),
        )

        with pytest.raises(SubsetError, match="fetching releases from https://"):
            fetch(
                "data/2026/discogs_20261001_releases.xml.gz",
                "releases",
                5,
                tmp_path / "discogs_20261001_releases.xml.gz",
            )
        assert list(tmp_path.iterdir()) == []
