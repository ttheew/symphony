#!/usr/bin/env bash
set -euo pipefail

TARGETS=("${@:-.}")

run_formatter() {
    local cmd=("$@")
    echo "→ ${cmd[*]}"
    "${cmd[@]}"
}

for target in "${TARGETS[@]}"; do
    run_formatter python3 -m autoflake --in-place --remove-unused-variables --remove-all-unused-imports -r "$target"
    run_formatter python3 -m isort "$target"
    run_formatter python3 -m black "$target"
done
