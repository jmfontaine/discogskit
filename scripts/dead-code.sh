#!/usr/bin/env bash
# Find dead code. `just dead-code`, the pre-commit hook and CI all run this script, so they fail the same way.
#
# deadcode prints its findings but still exits 0, so this fails when the output contains a finding code
# (DC01, DC02, ...). It also fails when uv or deadcode exits non-zero: a crash or a missing interpreter.
#
# KLUDGE: Python 3.13, because deadcode 2.4.1 uses ast.Str, which Python 3.14 removed.
set -uo pipefail

status=0
output=$(uv run --python 3.13 deadcode src tests benchmarks 2>&1) || status=$?
printf '%s\n' "$output"

if [ "$status" -ne 0 ]; then
    echo "dead-code: deadcode failed with exit code $status" >&2
    exit "$status"
fi

if grep -qE 'DC[0-9]{2}' <<<"$output"; then
    echo "dead-code: unused code found" >&2
    exit 1
fi
