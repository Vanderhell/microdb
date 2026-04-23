#!/usr/bin/env bash
# SPDX-License-Identifier: MIT
set -euo pipefail

REPORT_DIR="docs/results"
mkdir -p "$REPORT_DIR"

echo "=== cppcheck ==="
cppcheck \
  --enable=warning,performance,portability \
  --suppress=missingIncludeSystem \
  --suppress=unusedFunction \
  --error-exitcode=1 \
  -I include -I src \
  src/loxdb.c src/lox_kv.c src/lox_ts.c \
  src/lox_rel.c src/lox_wal.c src/lox_crc.c \
  2>&1 | tee "$REPORT_DIR/cppcheck_report.txt"
echo "cppcheck: PASS"

echo "=== Compiler warnings (gcc) ==="
gcc -std=c99 -Wall -Wextra -Wpedantic -Wconversion \
  -Isrc -Iinclude -Iport/posix -Iport/ram \
  -fsyntax-only \
  src/loxdb.c src/lox_kv.c src/lox_ts.c \
  src/lox_rel.c src/lox_wal.c src/lox_crc.c \
  port/posix/lox_port_posix.c port/ram/lox_port_ram.c \
  2>&1 | tee "$REPORT_DIR/compiler_warnings.txt"

WARN_COUNT=$(grep -c "warning:" "$REPORT_DIR/compiler_warnings.txt" || true)
if [ "$WARN_COUNT" -gt 0 ]; then
  echo "WARNING: $WARN_COUNT compiler warnings - document as known deviations"
  exit 1
fi
echo "Compiler clean: PASS"
echo "Static analysis complete. Reports in $REPORT_DIR/"
