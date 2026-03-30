# Configuration

microdb is compile-time first, with a small per-instance override surface.

## Key compile-time defines

- `MICRODB_RAM_KB`: total RAM budget
- `MICRODB_RAM_KV_PCT`: KV slice percentage
- `MICRODB_RAM_TS_PCT`: TS slice percentage
- `MICRODB_RAM_REL_PCT`: REL slice percentage
- `MICRODB_ENABLE_WAL`: enable or disable WAL support
- `MICRODB_ENABLE_KV`: include or exclude KV engine
- `MICRODB_ENABLE_TS`: include or exclude TS engine
- `MICRODB_ENABLE_REL`: include or exclude REL engine
- `MICRODB_KV_MAX_KEYS`: KV key count limit
- `MICRODB_TS_MAX_STREAMS`: TS stream count limit
- `MICRODB_REL_MAX_TABLES`: relational table limit
- `MICRODB_REL_MAX_COLS`: relational column limit

## Runtime overrides

`microdb_cfg_t` can override:

- total RAM budget
- KV/TS/REL percentage split
- storage backend
- timestamp callback

## Practical sizing

Typical starting points:

- 8 KB: minimal node
- 32 KB: default IoT device
- 64 KB: moderate gateway
- 128 KB+: ESP32 with more memory or richer workloads

Tune the split based on workload instead of relying on one default ratio for all devices.
