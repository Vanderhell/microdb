#!/usr/bin/env sh
# SPDX-License-Identifier: MIT
set -eu

VERSION="${1:-dev}"
BUILD_DIR="${2:-build}"
CONFIG="${3:-Release}"
PLATFORM="${4:-linux-x64}"

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
STAGE_ROOT="$ROOT_DIR/dist"
PACKAGE_NAME="microdb-$VERSION-$PLATFORM"
INSTALL_DIR="$STAGE_ROOT/$PACKAGE_NAME"
ARCHIVE_PATH="$STAGE_ROOT/$PACKAGE_NAME.tar.gz"
CHECKSUM_PATH="$ARCHIVE_PATH.sha256"

rm -rf "$INSTALL_DIR"
rm -f "$ARCHIVE_PATH" "$CHECKSUM_PATH"
mkdir -p "$STAGE_ROOT"

cmake --install "$BUILD_DIR" --config "$CONFIG" --prefix "$INSTALL_DIR"
tar -C "$STAGE_ROOT" -czf "$ARCHIVE_PATH" "$PACKAGE_NAME"
sha256sum "$ARCHIVE_PATH" > "$CHECKSUM_PATH"

printf '%s\n%s\n' "$ARCHIVE_PATH" "$CHECKSUM_PATH"
