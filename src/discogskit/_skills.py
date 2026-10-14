"""`discogskit skills`: serve the agent guide bundled with the installed package, so it matches this version."""

from __future__ import annotations

from importlib.metadata import version
from pathlib import Path
from typing import Annotated

import typer

from discogskit._console import current, error

SKILLS_ROOT = Path(__file__).parent / "skills"

app: typer.Typer = typer.Typer()


def _skill_dirs() -> dict[str, Path]:
    """Each bundled skill's directory by name: the subdirectories holding a SKILL.md."""
    return {
        entry.name: entry
        for entry in sorted(SKILLS_ROOT.iterdir())
        if (entry / "SKILL.md").is_file()
    }


def _description(skill_md: str) -> str:
    """The one-line ``description:`` of the skill's front matter, or ''."""
    for line in skill_md.splitlines():
        if line.startswith("description:"):
            return line.removeprefix("description:").strip()
    return ""


def _render(skill_dir: Path, *, full: bool) -> str:
    """SKILL.md, followed with ``full`` by each ``references/*.md`` in name order."""
    text = (skill_dir / "SKILL.md").read_text(encoding="utf-8")
    if not full:
        return text
    return text + "".join(
        f"\n\n---\n\n<!-- references/{ref.name} -->\n\n"
        + ref.read_text(encoding="utf-8")
        for ref in sorted((skill_dir / "references").glob("*.md"))
    )


def _lookup(name: str) -> Path:
    """The named skill's directory. Exits 1, listing the available names, when there is no such skill."""
    dirs = _skill_dirs()
    if name not in dirs:
        error(f"unknown skill '{name}'. Available: {', '.join(dirs)}.")
        raise typer.Exit(1)
    return dirs[name]


def _complete_name(incomplete: str) -> list[str]:
    return [name for name in _skill_dirs() if name.startswith(incomplete)]


def _out(text: str) -> None:
    """Print ``text`` exactly: no markup, emoji codes, highlighting or wrapping, which would alter Markdown."""
    current().out(text, highlight=False, end="")


@app.callback(invoke_without_command=True)
def _callback(ctx: typer.Context) -> None:
    """List or print the bundled agent skills: usage guides matching this version."""
    if ctx.invoked_subcommand is None:
        list_skills()


@app.command("get")
def get_skill(
    name: Annotated[
        str, typer.Argument(autocompletion=_complete_name, help="Skill name")
    ],
    full: Annotated[
        bool,
        typer.Option(help="Append the skill's references/*.md"),
    ] = False,
) -> None:
    """Print a skill's SKILL.md."""
    _out(_render(_lookup(name), full=full))


@app.command("list")
def list_skills() -> None:
    """List the bundled skills, tagged with the installed version they describe."""
    tag = f"(discogskit {version('discogskit')})"
    for name, skill_dir in _skill_dirs().items():
        desc = _description((skill_dir / "SKILL.md").read_text(encoding="utf-8"))
        _out(f"{name:<8} {desc} {tag}\n")


@app.command("path")
def skill_path(
    name: Annotated[
        str | None,
        typer.Argument(
            autocompletion=_complete_name,
            help="Skill name; without it, the directory holding every skill",
        ),
    ] = None,
) -> None:
    """Print the directory holding the bundled skills, or one skill."""
    _out(f"{SKILLS_ROOT if name is None else _lookup(name)}\n")
