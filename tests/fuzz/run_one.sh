#!/usr/bin/env bash
# SPDX-License-Identifier: MIT
set -euo pipefail

NAME="${1:-}"
MAX_TOTAL_TIME_SEC="${2:-600}"

if [[ -z "${NAME}" ]]; then
  echo "Usage: $0 <harness_name> [max_total_time_sec]"
  exit 2
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${ROOT_DIR}/build/fuzz/${NAME}"

if [[ ! -x "${BIN}" ]]; then
  echo "Missing harness binary: ${BIN}"
  echo "Build first: ./tests/fuzz/build.sh"
  exit 2
fi

exec "${BIN}" \
  -max_total_time="${MAX_TOTAL_TIME_SEC}" \
  -timeout=5 \
  -rss_limit_mb=2048 \
  -print_final_stats=1

