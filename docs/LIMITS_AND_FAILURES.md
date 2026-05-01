# Limits and Failures Contract

## Purpose
Single source of truth for runtime limits, invariants, and expected fail behavior.

## Hard Invariants
- RAM budget invariants:
  - total runtime heap is one-shot budget (`LOX_RAM_KB` or `cfg.ram_kb`).
  - engine split must sum to exactly 100 (`kv_pct + ts_pct + rel_pct`).
  - invalid split fails fast with `LOX_ERR_INVALID`.
- Storage geometry invariants:
  - `erase_size > 0`
  - `write_size == 1` (current release contract)
  - storage callbacks `read/write/erase/sync` must all be non-null for durable mode.
- Engine split invariants:
  - defaults from compile-time macros are used when runtime split is not provided.
  - if any runtime split field is set, all three must be set and valid.

## Preflight Contract
- Use `lox_preflight(const lox_cfg_t*, lox_preflight_report_t*)` before `lox_init`.
- `lox_preflight` is deterministic for identical input config.
- It reports:
  - effective RAM split and arena bytes,
  - storage feasibility (`storage_required_bytes` vs `storage_capacity_bytes`),
  - computed snapshot/WAL sizing (`kv_snapshot_bytes`, `ts_snapshot_bytes`, `rel_snapshot_bytes`, `wal_size`, `bank_size`).
- Typical startup policy:
  1. call preflight,
  2. if non-OK, do not call `lox_init`,
  3. apply fallback profile or show actionable error.

## Engine Limits
## KV
- Max keys: bounded by `LOX_KV_MAX_KEYS` (with transaction staging reserve).
- Value size limits: bounded by `LOX_KV_VAL_MAX_LEN`.
- Full behavior: depends on overflow policy (`overwrite` or `reject`).

## TS
- Max streams: bounded by `LOX_TS_MAX_STREAMS`.
- Sample type constraints: `F32/I32/U32/RAW` with `RAW` limited by configured raw size.
- Overflow behavior: compile-time policy (`DROP_OLDEST`, `REJECT`, `DOWNSAMPLE`, `LOG_RETAIN`).

## REL
- Max tables: bounded by `LOX_REL_MAX_TABLES`.
- Max columns: bounded by `LOX_REL_MAX_COLS`.
- `max_rows` implications:
  - affects REL memory footprint and can trigger `LOX_ERR_FULL` / `LOX_ERR_NO_MEM` during table create/setup.
  - must be selected against available runtime RAM slice, not only storage size.

## Failure Codes
| Code | Meaning | Recoverable | Typical action |
|---|---|---|---|
| LOX_ERR_INVALID | bad config/contract/API misuse | sometimes | fix config/API call order |
| LOX_ERR_NO_MEM | RAM budget/allocation too small | yes | reduce profile or increase `ram_kb` |
| LOX_ERR_FULL | bounded structure saturated | yes | compact/clear/reduce pressure |
| LOX_ERR_STORAGE | I/O error or storage capacity insufficient | yes | validate backend + increase storage budget |
| LOX_ERR_CORRUPT | durable image/page/WAL corruption | depends | recreate/recover image |
| LOX_ERR_EXISTS | duplicate create/register | yes | treat as idempotent or query first |

## What Core Does Not Do
- policy orchestration,
- product governance,
- commercial/security workflow orchestration.
