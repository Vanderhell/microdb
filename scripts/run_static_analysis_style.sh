#!/usr/bin/env bash
# SPDX-License-Identifier: MIT
set -euo pipefail

REPORT_DIR="docs/results"
mkdir -p "$REPORT_DIR"

echo "=== cppcheck (style report, non-blocking) ==="
cppcheck \
  --enable=style \
  --suppress=missingIncludeSystem \
  --suppress=unusedFunction \
  -I include -I src \
  src/microdb.c src/microdb_kv.c src/microdb_ts.c \
  src/microdb_rel.c src/microdb_wal.c src/microdb_crc.c \
  2>&1 | tee "$REPORT_DIR/cppcheck_style_report.txt"

STYLE_COUNT=$(grep -c ": style:" "$REPORT_DIR/cppcheck_style_report.txt" || true)
echo "cppcheck style findings: $STYLE_COUNT"
echo "Style report complete. Output: $REPORT_DIR/cppcheck_style_report.txt"
