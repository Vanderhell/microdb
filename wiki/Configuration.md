# Configuration

loxdb is compile-time first, with a small per-instance override surface.

## Key compile-time defines

- `LOX_RAM_KB`: total RAM budget
- `LOX_RAM_KV_PCT`: KV slice percentage
- `LOX_RAM_TS_PCT`: TS slice percentage
- `LOX_RAM_REL_PCT`: REL slice percentage
- `LOX_ENABLE_WAL`: enable or disable WAL support
- `LOX_ENABLE_KV`: include or exclude KV engine
- `LOX_ENABLE_TS`: include or exclude TS engine
- `LOX_ENABLE_REL`: include or exclude REL engine
- `LOX_KV_MAX_KEYS`: KV key count limit
- `LOX_TS_MAX_STREAMS`: TS stream count limit
- `LOX_REL_MAX_TABLES`: relational table limit
- `LOX_REL_MAX_COLS`: relational column limit

## Runtime overrides

`lox_cfg_t` can override:

- total RAM budget
- KV/TS/REL percentage split
- storage backend
- timestamp callback

## Storage Capacity Profiles (tooling/wrapper layer)

For persistent backends and wrappers, standardize storage budgets with `MiB` profiles:

- `2 MiB`
- `4 MiB`
- `8 MiB`
- `16 MiB`
- `32 MiB`

Use `include/lox_capacity_profile.h` for canonical profile-to-bytes mapping.
This is a wrapper/tooling aid and does not change core storage contract checks.

For quick planning and what-if sizing, open:
- `tools/lox_capacity_estimator.html`

## Practical sizing

Typical starting points:

- 8 KB: minimal node
- 32 KB: default IoT device
- 64 KB: moderate gateway
- 128 KB+: ESP32 with more memory or richer workloads

Tune the split based on workload instead of relying on one default ratio for all devices.
