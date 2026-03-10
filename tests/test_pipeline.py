"""Tests for pipeline utility functions."""

from __future__ import annotations

import gzip
import json

import pytest

from discogskit.pipeline import _fmt_time


# ------------------------------------------------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------------------------------------------------

_ARTISTS_GZ_XML = (
    b"<?xml version='1.0' encoding='UTF-8'?>\n"
    b"<artists>\n"
    b"<artist>\n"
    b"  <id>1</id>\n"
    b"  <name>Test</name>\n"
    b"  <data_quality>Correct</data_quality>\n"
    b"</artist>\n"
    b"<artist>\n"
    b"  <id>2</id>\n"
    b"  <name>Other</name>\n"
    b"  <data_quality>Correct</data_quality>\n"
    b"</artist>\n"
    b"</artists>"
)


@pytest.fixture()
def artists_gz(tmp_path):
    """Create a small artists .xml.gz file."""
    gz_path = tmp_path / "artists.xml.gz"
    with gzip.open(gz_path, "wb") as f:
        f.write(_ARTISTS_GZ_XML)
    return gz_path


# ------------------------------------------------------------------------------------------------------------------------
# Unit tests
# ------------------------------------------------------------------------------------------------------------------------


class TestFmtTime:
    @pytest.mark.parametrize(
        "seconds, expected",
        [
            (0, "0s"),
            (1, "1s"),
            (42, "42s"),
            (59, "59s"),
            (60, "1:00"),
            (61, "1:01"),
            (125, "2:05"),
            (3599, "59:59"),
            (3600, "60:00"),
        ],
    )
    def test_formatting(self, seconds, expected):
        assert _fmt_time(seconds) == expected

    def test_float_truncated(self):
        assert _fmt_time(42.9) == "42s"


# ------------------------------------------------------------------------------------------------------------------------
# Integration tests — full pipeline.run()
# ------------------------------------------------------------------------------------------------------------------------


@pytest.mark.integration
class TestPipelineRun:
    def test_end_to_end_no_progress(self, tmp_path, artists_gz):
        """Full pipeline with progress=False (verbose per-chunk output)."""
        from discogskit import pipeline
        from discogskit.writers.jsonl import JSONLWriter

        out_dir = tmp_path / "out"
        writer = JSONLWriter(str(out_dir))

        config = pipeline.PipelineConfig(
            chunk_mb=1,
            entity="artists",
            gz_path=artists_gz,
            keep_xml=False,
            parse_workers=1,
            profile=False,
            progress=False,
            strict=False,
            write_queue=2,
        )

        try:
            result = pipeline.run(config, writer)
        finally:
            writer.close()

        assert result.total_records == 2
        assert result.t_total > 0
        assert result.profile_data is None

        # Verify data actually reached the output
        artists_jsonl = out_dir / "artists" / "artists.jsonl"
        lines = artists_jsonl.read_text().strip().split("\n")
        assert len(lines) == 2
        assert json.loads(lines[0])["name"] == "Test"

        # XML should be cleaned up
        xml_path = artists_gz.with_suffix("")
        assert not xml_path.exists()

    def test_end_to_end_with_progress(self, tmp_path, artists_gz):
        """Full pipeline with progress=True (Rich progress bar)."""
        from discogskit import pipeline
        from discogskit.writers.jsonl import JSONLWriter

        out_dir = tmp_path / "out"
        writer = JSONLWriter(str(out_dir))

        config = pipeline.PipelineConfig(
            chunk_mb=1,
            entity="artists",
            gz_path=artists_gz,
            keep_xml=False,
            parse_workers=1,
            profile=False,
            progress=True,
            strict=False,
            write_queue=2,
        )

        try:
            result = pipeline.run(config, writer)
        finally:
            writer.close()

        assert result.total_records == 2

    def test_end_to_end_with_profile(self, tmp_path, artists_gz):
        """Full pipeline with profile=True collects timing data."""
        from discogskit import pipeline
        from discogskit.writers.jsonl import JSONLWriter

        out_dir = tmp_path / "out"
        writer = JSONLWriter(str(out_dir))

        config = pipeline.PipelineConfig(
            chunk_mb=1,
            entity="artists",
            gz_path=artists_gz,
            keep_xml=True,
            parse_workers=1,
            profile=True,
            progress=False,
            strict=False,
            write_queue=2,
        )

        try:
            result = pipeline.run(config, writer)
        finally:
            writer.close()

        assert result.total_records == 2
        assert result.profile_data is not None
        assert "put_blocked" in result.profile_data
        assert "get_wait" in result.profile_data
        assert "table_timings" in result.profile_data

        # keep_xml=True should preserve the XML
        xml_path = artists_gz.with_suffix("")
        assert xml_path.exists()

    def test_writer_error_propagated(self, tmp_path, artists_gz):
        """Writer thread errors are re-raised in the main thread."""
        from discogskit import pipeline

        class FailingWriter:
            def setup(self, entity):
                pass

            def write_chunk(self, ipc_dict, entity, table_timings=None):
                raise RuntimeError("intentional failure")

            def finalize(self, entity):
                pass

            def close(self):
                pass

        config = pipeline.PipelineConfig(
            chunk_mb=1,
            entity="artists",
            gz_path=artists_gz,
            keep_xml=False,
            parse_workers=1,
            profile=False,
            progress=False,
            strict=False,
            write_queue=2,
        )

        with pytest.raises(RuntimeError, match="intentional failure"):
            pipeline.run(config, FailingWriter())
