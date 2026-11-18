#!/usr/bin/env bash
#
# Download the pinned Almanak Code release and place its binaries into
# src/almanak/bin/ under the names the CLI resolves at run time.
#
# The release tag is taken from, in order of precedence:
#   1. a positional argument:      ./scripts/update_binaries.sh v1.0.66
#   2. the .almanak-code-version file
#   3. "latest"
#
# Usage:
#   ./scripts/update_binaries.sh
#   ./scripts/update_binaries.sh v1.0.66
#   ./scripts/update_binaries.sh --platform linux-x86_64
#
# Requires gh, authenticated against the release repository.

set -euo pipefail

if [ "${BASH_VERSINFO[0]:-0}" -lt 4 ]; then
    echo "ERROR: bash 4 or newer is required; macOS ships 3.2 at /bin/bash" >&2
    echo "Install a newer bash, or run this on Linux." >&2
    exit 1
fi

REPO="${ALMANAK_CODE_REPO:-almanak-co/almanak-code}"
PLATFORM_FILTER=""
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
VERSION_FILE="$ROOT/.almanak-code-version"
BIN_DIR="$ROOT/src/almanak/bin"

if [ -f "$VERSION_FILE" ]; then
    TAG="$(tr -d '[:space:]' < "$VERSION_FILE")"
    [ -z "$TAG" ] && TAG="latest"
else
    TAG="latest"
fi

TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

while [[ $# -gt 0 ]]; do
    case "$1" in
        --platform)
            PLATFORM_FILTER="$2"
            shift 2
            ;;
        *)
            TAG="$1"
            shift
            ;;
    esac
done

# Release assets say x64 where the bundled binaries say x86_64.
declare -A ASSET_MAP=(
    ["almanakcode-darwin-arm64.zip"]="almanak-code-darwin-arm64"
    ["almanakcode-darwin-x64.zip"]="almanak-code-darwin-x86_64"
    ["almanakcode-linux-arm64.tar.gz"]="almanak-code-linux-arm64"
    ["almanakcode-linux-x64.tar.gz"]="almanak-code-linux-x86_64"
    ["almanakcode-windows-x64.zip"]="almanak-code-windows-x86_64.exe"
)

declare -A PLATFORM_TO_ASSETS=(
    ["darwin-arm64"]="almanakcode-darwin-arm64.zip"
    ["darwin-x86_64"]="almanakcode-darwin-x64.zip"
    ["linux-arm64"]="almanakcode-linux-arm64.tar.gz"
    ["linux-x86_64"]="almanakcode-linux-x64.tar.gz"
    ["windows-x86_64"]="almanakcode-windows-x64.zip"
)

if [ -n "$PLATFORM_FILTER" ] && [ -z "${PLATFORM_TO_ASSETS[$PLATFORM_FILTER]+x}" ]; then
    echo "ERROR: unknown platform '$PLATFORM_FILTER'" >&2
    echo "Valid platforms: ${!PLATFORM_TO_ASSETS[*]}" >&2
    exit 1
fi

echo "Using $REPO at version: $TAG"

if [ "$TAG" = "latest" ]; then
    gh release download --repo "$REPO" --dir "$TMPDIR" --pattern '*.zip' --pattern '*.tar.gz'
else
    gh release download "$TAG" --repo "$REPO" --dir "$TMPDIR" --pattern '*.zip' --pattern '*.tar.gz'
fi

selected=()
for asset in "${!ASSET_MAP[@]}"; do
    if [ -n "$PLATFORM_FILTER" ] && [ "$asset" != "${PLATFORM_TO_ASSETS[$PLATFORM_FILTER]}" ]; then
        continue
    fi
    selected+=("$asset")
done

# Nothing is installed until every selected asset is present. Installing what a
# partial release does carry would leave the previous version's binary in place
# for the rest, and that binary would then be packaged as if it were this one.
missing=()
for asset in "${selected[@]}"; do
    [ -f "$TMPDIR/$asset" ] || missing+=("$asset")
done
if [ ${#missing[@]} -gt 0 ]; then
    echo "ERROR: release $TAG of $REPO is missing: ${missing[*]}" >&2
    exit 1
fi

mkdir -p "$BIN_DIR"

for asset in "${selected[@]}"; do
    target="${ASSET_MAP[$asset]}"
    asset_path="$TMPDIR/$asset"

    extract_dir="$TMPDIR/extract-$target"
    mkdir -p "$extract_dir"

    case "$asset" in
        *.zip)
            unzip -q -o "$asset_path" -d "$extract_dir"
            ;;
        *.tar.gz)
            tar xzf "$asset_path" -C "$extract_dir"
            ;;
    esac

    # Each archive holds a single binary named almanakcode.
    src="$extract_dir/almanakcode"
    [ -f "$src" ] || src="$extract_dir/almanakcode.exe"
    if [ ! -f "$src" ]; then
        echo "  ERROR: no almanakcode binary inside $asset" >&2
        ls -la "$extract_dir" >&2
        exit 1
    fi

    cp "$src" "$BIN_DIR/$target"
    chmod +x "$BIN_DIR/$target"
    echo "  OK $asset -> $target ($(du -h "$BIN_DIR/$target" | cut -f1))"
done

echo ""
echo "Binaries in $BIN_DIR:"
ls -lh "$BIN_DIR"/almanak-code-* 2>/dev/null || echo "  (none)"
