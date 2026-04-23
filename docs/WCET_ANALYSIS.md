# WCET Analysis

## Overview

Worst-Case Execution Time (WCET) analysis gives deterministic latency bounds for
embedded systems where missed deadlines are unacceptable. For `loxdb`, WCET
is derived from:

- compile-time limits (`LOX_*` macros),
- concrete algorithmic paths in `src/`,
- hardware-dependent primitive costs (hash, compare, memcpy, flash write/sync).

This document provides formula templates and conservative bounds. Use them with
target-specific measurements to obtain microsecond-level budgets.

## How to use WCET bounds

1. Include [lox_wcet.h](/C:/Users/vande/Desktop/github/loxdb/include/lox_wcet.h).
2. Measure primitive costs once on your target:
   - `T_hash`
   - `T_strncmp(n)`
   - `T_memcpy(n)`
   - `T_flash_write(n)`
   - `T_flash_sync`
3. Substitute constants/macros into per-API formulas below.

## Per-function analysis

### `lox_kv_get()`

- Best case: `O(1)` (first probe hit, non-expired, small copy)
- Worst case: `O(bucket_count)` linear probe through hash table
- RAM accesses: bounded by bucket array walk + one value copy
- WAL writes: none
- Stack depth: fixed, non-recursive
- Formula:
  - `T_wcet = T_hash + LOX_WCET_KV_PROBE_MAX * T_strncmp(KEY_MAX) + T_memcpy(val_len)`

### `lox_kv_set()` (no storage)

- Best case: `O(1)` probe + append/overwrite
- Worst case: probe + value-store compaction/memmove
- RAM accesses: bounded but data-size dependent (`value_capacity`)
- WAL writes: none
- Formula:
  - `T_wcet = T_probe_wc + T_memmove(LOX_WCET_KV_MEMMOVE_MAX) + T_memcpy(val_len)`

### `lox_kv_set()` (WAL sync always)

- Best case: append WAL entry + sync
- Worst case: same as above + worst storage write/sync + optional compact
- Formula:
  - `T_wcet = T_probe_wc + T_memmove(LOX_WCET_KV_MEMMOVE_MAX) + T_flash_write(LOX_WCET_WAL_KV_SET_MAX) + T_flash_sync + T_compact_wc`

### `lox_kv_set()` (WAL flush-only)

- Best case: no immediate sync
- Worst case: deferred compact/flush still possible
- Formula:
  - `T_wcet = T_probe_wc + T_memmove(LOX_WCET_KV_MEMMOVE_MAX) + T_flash_write(LOX_WCET_WAL_KV_SET_MAX) + T_compact_wc`

### `lox_kv_del()`

- Best case: direct hit/tombstone mark
- Worst case: full probe
- RAM accesses: bounded to bucket table + metadata update
- WAL writes: one delete entry when persistence enabled
- Formula:
  - `T_wcet = T_hash + LOX_WCET_KV_PROBE_MAX * T_strncmp(KEY_MAX) + T_flash_write(WAL_DEL_MAX)`

### `lox_ts_insert()`

- DROP_OLDEST policy:
  - Best/Worst: `O(1)` ring write + pointer move
- REJECT policy:
  - Best/Worst: `O(1)` with fast-full rejection
- DOWNSAMPLE policy:
  - Worst: bounded `O(capacity)` shift for oldest merge path
- LOG_RETAIN policy:
  - Worst: bounded `O(capacity)` zone compaction/shift
- WAL writes:
  - scalar: `LOX_WCET_WAL_TS_F32_INSERT_MAX`
  - raw: `LOX_WCET_WAL_TS_RAW_INSERT_MAX`
- Formula:
  - `T_wcet = T_ring_update + T_policy_overflow_wc + T_flash_write(WAL_TS_MAX) + T_flash_sync(mode)`

### `lox_ts_query()` / `lox_ts_query_buf()`

- Best case: narrow range, few samples
- Worst case: full stream scan (`count` samples)
- RAM accesses: bounded by retained sample count
- WAL writes: none
- Formula:
  - `T_wcet = count * (T_sample_decode + T_range_check + T_callback_or_copy)`

### `lox_rel_insert()`

- Best case: free row + append at end of index
- Worst case: insertion with maximal index shift
- RAM accesses: bounded by `max_rows` bitmap/index operations
- WAL writes: row payload + header
- Formula:
  - `T_wcet = T_bitmap_scan + T_shift(LOX_WCET_REL_INDEX_SHIFTS_MAX(max_rows)) + T_flash_write(LOX_WCET_WAL_REL_INSERT_MAX(row_size))`

### `lox_rel_find()`

- Best case: indexed first-hit
- Worst case: full index range walk for duplicated keys
- RAM accesses: bounded by `index_count`/callback exit condition
- WAL writes: none
- Formula:
  - `T_wcet = T_bsearch + K * (T_row_copy + T_callback + T_relock_check)`

### `lox_rel_delete()`

- Best case: no match
- Worst case: many duplicates + index/order maintenance
- RAM accesses: bounded by row/index cardinality
- WAL writes: one delete record
- Formula:
  - `T_wcet = T_find + T_delete_loop_wc + T_flash_write(WAL_REL_DEL_MAX)`

### `lox_compact()`

- Best case: small delta, minimal dirty footprint
- Worst case: full bank rewrite (KV+TS+REL pages), superblock update, WAL reset
- RAM accesses: bounded by in-memory state size
- WAL writes: reset + metadata update
- Formula:
  - `T_wcet = T_erase(bank+wal) + T_write(snapshot_pages) + T_sync + T_superblock_switch`

### `lox_selfcheck()`

- Best case: clean structure walk
- Worst case: full KV pairwise overlap check + full TS/REL traversal
- RAM accesses: bounded by configured capacities
- WAL writes: none
- Formula:
  - `T_wcet = T_kv_checks(bucket_count^2) + T_ts_checks(stream_count) + T_rel_checks(table_count + bitmap_bytes + index_count) + T_wal_checks`

## Operations with unbounded WCET

No public API is mathematically unbounded in current core design; all loops are
bounded by compile-time limits and configured capacities. Practical runtime
variability is still hardware dependent due to backend I/O latency.

## Hardware measurement guide

### ESP32

1. Run primitive microbenchmarks for hash/strcmp/memcpy with cache state noted.
2. Measure flash write/sync latency with same partition/backend used by `loxdb`.
3. Substitute values into formulas and add safety margin.
4. Validate with long-run workload replay and `lox_selfcheck()`.

### Cortex-M4 (generic)

1. Measure primitives with DWT cycle counter.
2. Measure storage backend worst path (erase/write/sync) including wait states.
3. Convert cycles to time at target clock.
4. Cross-check formulas against instrumented stress tests.
