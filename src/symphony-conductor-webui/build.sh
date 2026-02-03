#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WEBUI_DIST_DIR="$SCRIPT_DIR/dist"
CONDUCTOR_UI_DIR="$SCRIPT_DIR/../symphony/conductor/ui"
CONDUCTOR_UI_DIST_DIR="$CONDUCTOR_UI_DIR/dist"

cd "$SCRIPT_DIR"

echo "Building conductor web UI..."
ENV_PROD=1 npm run build

echo "Copying dist to conductor UI folder..."
rm -rf "$CONDUCTOR_UI_DIST_DIR"
mkdir -p "$CONDUCTOR_UI_DIR"
cp -a "$WEBUI_DIST_DIR" "$CONDUCTOR_UI_DIST_DIR"

echo "Done: $CONDUCTOR_UI_DIST_DIR"
