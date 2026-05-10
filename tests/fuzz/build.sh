#!/usr/bin/env bash
# SPDX-License-Identifier: MIT
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT_DIR="${ROOT_DIR}/build/fuzz"

mkdir -p "${OUT_DIR}"

CC=clang
CXX=clang++

COMMON_CFLAGS=(
  -O1 -g
  -fno-omit-frame-pointer
)

FUZZ_CXXFLAGS=(
  -std=c++17
  -fsanitize=fuzzer,address,undefined
)

INCLUDES=(
  -I"${ROOT_DIR}/include"
  -I"${ROOT_DIR}/src"
)

echo "=== build fuzz_wal_parser ==="
${CXX} \
  "${ROOT_DIR}/tests/fuzz/fuzz_wal_parser.cpp" \
  "${ROOT_DIR}/src/lox_crc.c" \
  ${COMMON_CFLAGS[@]} \
  ${FUZZ_CXXFLAGS[@]} \
  ${INCLUDES[@]} \
  -o "${OUT_DIR}/fuzz_wal_parser"

echo "Built: ${OUT_DIR}/fuzz_wal_parser"
