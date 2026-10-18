set quiet := true

_list:
    just --list

# Fail unless the checkout is a clean main equal to origin/main, after fetching origin's main and tags
_require-main:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ "$(git branch --show-current)" != main ]; then
        echo "Error: check out main first"
        exit 1
    fi
    if [ -n "$(git status --porcelain)" ]; then
        echo "Error: working tree is not clean"
        exit 1
    fi
    git fetch --quiet --tags origin main
    if [ "$(git rev-parse main)" != "$(git rev-parse origin/main)" ]; then
        if git merge-base --is-ancestor main origin/main; then
            echo "Error: local main ($(git rev-parse --short main)) is behind origin/main ($(git rev-parse --short origin/main)); run git pull --ff-only on main"
        else
            echo "Error: local main ($(git rev-parse --short main)) differs from origin/main ($(git rev-parse --short origin/main))"
        fi
        exit 1
    fi

# Open a pull request from a new branch off main that only sets the project version
[positional-arguments]
_version-pr branch version message body dry_run:
    #!/usr/bin/env bash
    set -euo pipefail
    branch=$1 version=$2 message=$3 body=$4 dry_run=$5
    if git show-ref --verify --quiet "refs/heads/$branch"; then
        echo "Error: branch $branch already exists locally"
        exit 1
    fi
    if git ls-remote --exit-code --heads origin "refs/heads/$branch" >/dev/null; then
        echo "Error: branch $branch already exists on origin"
        exit 1
    fi
    if [ "$dry_run" = true ]; then
        echo "Dry run: checks passed. Would:"
        echo "  - create $branch from main ($(git rev-parse --short main))"
        echo "  - set the version $(uv version --short) -> $version in pyproject.toml and uv.lock"
        echo "  - commit \"$message\" and push $branch"
        echo "  - open a pull request against main titled \"$message\""
        exit 0
    fi
    git switch --create "$branch" main
    pushed=false
    # On any failure from here on, return to main and say what is left of the branch.
    restore() {
        status=$?
        echo "Error: opening the pull request for $branch failed (exit $status); switching back to main"
        git switch --quiet --discard-changes main
        if [ "$(git rev-parse "$branch")" = "$(git rev-parse main)" ]; then
            git branch --quiet --delete --force "$branch"
            echo "Discarded the version edit and deleted $branch, which had no commit."
        elif [ "$pushed" = false ]; then
            echo "Kept $branch with its commit $(git rev-parse --short "$branch"): push it and open the pull request, or delete it with git branch -D $branch."
        else
            echo "$branch is pushed to origin but has no pull request: open one with gh pr create --base main --head $branch, or delete the branch."
        fi
        if [ -n "$(git status --porcelain)" ]; then
            echo "The working tree still has untracked files left by the failed run:"
            git status --short
        fi
    }
    trap restore EXIT
    uv version --no-sync "$version"
    if [ -n "$(git status --porcelain -- . ':!pyproject.toml' ':!uv.lock')" ]; then
        echo "Error: setting the version changed more than pyproject.toml and uv.lock:"
        git status --short
        exit 1
    fi
    git add pyproject.toml uv.lock
    git commit --quiet -S -m "$message"
    git push --set-upstream origin "$branch"
    pushed=true
    gh pr create --base main --head "$branch" --title "$message" --body "$body"
    trap - EXIT
    git switch main

# Run benchmarks
bench *args:
    uv run python benchmarks/run.py {{ args }}

# Set up benchmark competitor tools
bench-setup *args:
    uv run python benchmarks/setup.py {{ args }}

# Preview release notes for unreleased changes
changelog-preview:
    uvx git-cliff --unreleased

# Find dead code
dead-code:
    scripts/dead-code.sh

# Check for unused or undeclared deps
deps-unused:
    uv run deptry src

# Update deps to latest versions
deps-update:
    uv lock --upgrade
    uv sync --all-extras --all-groups

# Run formatters
format:
    uv run pyproject-fmt pyproject.toml || true
    uv run ruff format

# Check formatting without modifying files
format-check:
    uv run pyproject-fmt --check pyproject.toml
    uv run ruff format --check

# Run linter
lint:
    uv run ruff check

# Run linter and fix issues
lint-fix:
    uv run ruff check --fix

# Run pre-commit on all files
pre-commit:
    prek run --all-files

# Install pre-commit hooks
pre-commit-install:
    prek install

# Update pre-commit hooks to latest versions
pre-commit-update:
    prek autoupdate --freeze

# Run all quality assurance checks
qa: dead-code deps-unused format-check lint type-check verify-types

# Tag main's merged release commit, push only the tag, and monitor the publish workflow
[positional-arguments]
release *args: _require-main
    #!/usr/bin/env bash
    set -euo pipefail
    dry_run=false
    for arg in "$@"; do
        case "$arg" in
            --dry-run) dry_run=true ;;
            *)
                echo "Error: unexpected argument $arg (usage: just release [--dry-run])"
                exit 1
                ;;
        esac
    done
    # uv prints the normalized version (0.2.0.DEV0 -> 0.2.0.dev0), as the release workflow's tag check reads it.
    version=$(uv version --short)
    case "$version" in
        *dev* | *rc*)
            echo "Error: $version is a development or release-candidate version; run just release-prepare and merge its pull request first"
            exit 1
            ;;
    esac
    tag="v${version}"
    if git rev-parse "$tag" >/dev/null 2>&1; then
        echo "Error: tag $tag already exists"
        exit 1
    fi
    if [ "$dry_run" = true ]; then
        echo "Dry run: checks passed. Would create signed tag $tag at main ($(git rev-parse --short main)) and push only $tag to origin."
        exit 0
    fi
    echo "Creating signed tag $tag..."
    git tag -s "$tag" -m "Release $tag" main
    # main only changes through pull requests, so push the tag alone.
    echo "Pushing $tag to origin..."
    git push origin "refs/tags/$tag"
    echo "Waiting for publish workflow to start..."
    sleep 5
    gh run watch --exit-status "$(gh run list --workflow=release.yml --branch="$tag" --limit=1 --json=databaseId --jq='.[0].databaseId')"
    echo "Next: just release-next [patch|minor|major] opens the pull request starting the next development version."

# Open a pull request setting the next development version after a release: minor (default), patch or major
[positional-arguments]
release-next *args: _require-main
    #!/usr/bin/env bash
    set -euo pipefail
    bump=minor
    dry_run=false
    for arg in "$@"; do
        case "$arg" in
            --dry-run) dry_run=true ;;
            major | minor | patch) bump=$arg ;;
            *)
                echo "Error: unexpected argument $arg (usage: just release-next [patch|minor|major] [--dry-run])"
                exit 1
                ;;
        esac
    done
    version=$(uv version --short)
    case "$version" in
        *dev* | *rc*)
            echo "Error: $version is already a development or release-candidate version"
            exit 1
            ;;
    esac
    if ! git ls-remote --exit-code --tags origin "refs/tags/v$version" >/dev/null; then
        echo "Error: v$version is not tagged on origin yet; run just release first"
        exit 1
    fi
    # uv version --bump minor --bump dev would give .dev1, so set .dev0 explicitly.
    next=$(uv version --bump "$bump" --dry-run --short)
    "{{ just_executable() }}" --justfile "{{ justfile() }}" --working-directory "$PWD" _version-pr \
        "chore/start-$next-development" "$next.dev0" \
        "chore: start $next development" \
        "Sets the version to \`$next.dev0\` now that v$version is tagged, so builds from main no longer report the released version." \
        "$dry_run"

# Open a pull request setting the release version: X.Y.Z.devN or X.Y.ZrcN -> X.Y.Z
[positional-arguments]
release-prepare *args: _require-main
    #!/usr/bin/env bash
    set -euo pipefail
    dry_run=false
    for arg in "$@"; do
        case "$arg" in
            --dry-run) dry_run=true ;;
            *)
                echo "Error: unexpected argument $arg (usage: just release-prepare [--dry-run])"
                exit 1
                ;;
        esac
    done
    version=$(uv version --short)
    # uv refuses to bump a version that is already a release (0.2.0 => 0.2.0).
    if ! release=$(uv version --bump stable --dry-run --short); then
        echo "Error: can't derive a release version from $version; is it already one?"
        exit 1
    fi
    if git rev-parse "v$release" >/dev/null 2>&1; then
        echo "Error: tag v$release already exists"
        exit 1
    fi
    "{{ just_executable() }}" --justfile "{{ justfile() }}" --working-directory "$PWD" _version-pr \
        "chore/release-v$release" "$release" \
        "chore: release v$release" \
        "Sets the release version. Merging this pull request is the go/no-go point for v$release: then run \`just release\` on an up-to-date main to tag it." \
        "$dry_run"

# Set local dev environment up
setup:
    uv sync --all-extras --all-groups  # Install dependencies
    uv run prek install  # Install pre-commit hooks
    echo "Run 'source .venv/bin/activate' to activate the Python virtual environment"

# Regenerate the agent skill's command, schema and DDL references from the code
skill-references:
    uv run python scripts/skill_references.py

# Run all tests
test *args:
    uv run pytest {{ args }}

# Run all tests with coverage
test-cov *args:
    uv run pytest --cov --cov-report=term-missing {{ args }}

# Run integration tests only
test-integration *args:
    uv run pytest -m integration {{ args }}

# Run unit tests only
test-unit *args:
    uv run pytest -m "not integration" {{ args }}

# Run type checker
type-check:
    uv run ty check

# Audit public API type annotation coverage
verify-types:
    uv run pyright --ignoreexternal --verifytypes discogskit
