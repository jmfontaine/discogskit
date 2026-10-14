"""Tests for `discogskit skills`: the agent guide bundled with the package, and its generated references."""

from __future__ import annotations

import gzip
import re
import sqlite3
import subprocess
import zipfile
from importlib.metadata import version
from importlib.resources import files
from pathlib import Path

import pytest
from rich.text import Text
from typer.testing import CliRunner, Result

from discogskit.cli import app
from scripts import skill_references
from tests.conftest import ARTISTS_XML, LABELS_XML, MASTERS_XML, RELEASES_XML

runner = CliRunner()

SKILLS = Path(str(files("discogskit") / "skills"))
CORE = SKILLS / "core"


def _invoke(*args: str) -> Result:
    return runner.invoke(app, ["skills", *args])


def _unstyle(output: str) -> str:
    """Strip ANSI styling (Rich colors error messages when it detects CI)."""
    return Text.from_ansi(output).plain


def _references() -> list[Path]:
    return sorted((CORE / "references").glob("*.md"))


class TestList:
    def test_lists_each_skill_with_its_description_and_version(self) -> None:
        result = _invoke("list")

        assert result.exit_code == 0, result.output
        front_matter = (CORE / "SKILL.md").read_text(encoding="utf-8").split("---")[1]
        description = re.search(r"^description: (.+)$", front_matter, re.MULTILINE)
        assert description is not None
        assert result.output.splitlines() == [
            f"core     {description[1]} (discogskit {version('discogskit')})"
        ]

    def test_bare_command_lists(self) -> None:
        bare = _invoke()

        assert bare.exit_code == 0, bare.output
        assert bare.output == _invoke("list").output


class TestGet:
    def test_prints_skill_md_verbatim(self) -> None:
        result = _invoke("get", "core")

        assert result.exit_code == 0, result.output
        assert result.output == (CORE / "SKILL.md").read_text(encoding="utf-8")

    def test_full_appends_every_reference_in_name_order(self) -> None:
        result = _invoke("get", "core", "--full")

        assert result.exit_code == 0, result.output
        expected = (CORE / "SKILL.md").read_text(encoding="utf-8") + "".join(
            f"\n\n---\n\n<!-- references/{ref.name} -->\n\n"
            + ref.read_text(encoding="utf-8")
            for ref in _references()
        )
        assert _references()
        assert result.output == expected

    @pytest.mark.parametrize("args", [("get", "nope"), ("path", "nope")])
    def test_unknown_name_fails_with_available_names(
        self, args: tuple[str, ...]
    ) -> None:
        result = _invoke(*args)

        assert result.exit_code == 1
        assert (
            _unstyle(result.output) == "Error: unknown skill 'nope'. Available: core.\n"
        )


class TestPath:
    def test_prints_skills_directory(self) -> None:
        result = _invoke("path")

        assert result.exit_code == 0, result.output
        assert (Path(result.output.rstrip("\n")) / "core" / "SKILL.md").is_file()

    def test_prints_one_skill_directory(self) -> None:
        result = _invoke("path", "core")

        assert result.exit_code == 0, result.output
        assert Path(result.output.rstrip("\n")) == CORE


def test_wheel_ships_every_skill_file(tmp_path: Path) -> None:
    """`skills get` reads the files from the installed package, so the wheel must carry all of them."""
    root = Path(__file__).parent.parent
    subprocess.run(
        ["uv", "build", "--wheel", "--out-dir", str(tmp_path), str(root)],
        capture_output=True,
        check=True,
    )
    (wheel,) = tmp_path.glob("*.whl")
    with zipfile.ZipFile(wheel) as zf:
        shipped = set(zf.namelist())
    bundled = {
        f"discogskit/skills/{p.relative_to(SKILLS).as_posix()}"
        for p in SKILLS.rglob("*")
        if p.is_file() and "__pycache__" not in p.parts
    }
    assert bundled
    assert bundled <= shipped


@pytest.mark.parametrize("name", sorted(skill_references.GENERATED))
def test_generated_reference_is_current(name: str) -> None:
    """The command, schema and DDL references follow the code: run `just skill-references` after changing either."""
    committed = (CORE / "references" / name).read_text(encoding="utf-8")
    assert committed == skill_references.GENERATED[name](), (
        f"references/{name} is stale: run `just skill-references`"
    )


def _sqlite_queries() -> list[str]:
    """The SQL blocks of the SQLite section of references/queries.md."""
    text = (CORE / "references" / "queries.md").read_text(encoding="utf-8")
    section = re.search(r"^## SQLite\n(.*?)(?=^## |\Z)", text, re.MULTILINE | re.DOTALL)
    assert section is not None
    return re.findall(r"^```sql\n(.*?)^```", section[1], re.MULTILINE | re.DOTALL)


def test_sqlite_example_queries_run(tmp_path: Path) -> None:
    """Every SQLite example in references/queries.md runs against a database `load` wrote."""
    for entity, xml in [
        ("artists", ARTISTS_XML),
        ("labels", LABELS_XML),
        ("masters", MASTERS_XML),
        ("releases", RELEASES_XML),
    ]:
        with gzip.open(tmp_path / f"discogs_20260301_{entity}.xml.gz", "wt") as f:
            f.write(f"<{entity}>\n{xml}</{entity}>\n")
    db = tmp_path / "discogs.db"
    result = runner.invoke(app, ["load", str(tmp_path), "--dsn", str(db)])
    assert result.exit_code == 0, result.output

    queries = _sqlite_queries()
    assert queries
    conn = sqlite3.connect(db)
    try:
        for query in queries:
            conn.execute(query).fetchall()
    finally:
        conn.close()
