# FS/Block Adapter Contract

## Scope
- Applies to `lox_backend_fs_adapter` path selected through `lox_backend_open`.
- Covers managed-class backends routed to filesystem adapter (`fs_stub`, `block_stub`, and future equivalents).
- Does not relax core invariants (`write_size == 1`, fail-fast open validation).

## Open-Time Selection
- `backend_class == MANAGED` and `sync_semantics == DURABLE_SYNC`:
  - route to `lox_backend_managed_adapter`.
- `backend_class == MANAGED` and `sync_semantics == FLUSH_ONLY`:
  - route to `lox_backend_fs_adapter` with `EXPLICIT` policy.
- `backend_class == MANAGED` and `sync_semantics == NONE`:
  - route to `lox_backend_fs_adapter` with `NONE` policy.

## Sync Policies
- `LOX_BACKEND_FS_SYNC_POLICY_EXPLICIT`
  - Writes/erases are forwarded to raw medium.
  - Durability point is explicit `sync`.
  - Mount-time sync probe is required by default.
- `LOX_BACKEND_FS_SYNC_POLICY_WRITE_THROUGH`
  - Writes/erases are followed by raw `sync`.
  - Every mutation is a durability boundary.
  - Mount-time sync probe is optional but allowed.
- `LOX_BACKEND_FS_SYNC_POLICY_NONE`
  - Adapter `sync` is a no-op (`LOX_OK`) and does not call raw `sync`.
  - Durability is delegated to underlying medium semantics.
  - Mount-time sync probe is skipped.

## Power-Fail Semantics
- `EXPLICIT`:
  - Data durability requires successful `sync`.
  - Power cut before durable sync may lose recent mutations.
- `WRITE_THROUGH`:
  - Mutations are durable once write/erase returns `LOX_OK`.
- `NONE`:
  - Adapter makes no durability claim for `sync`.
  - Persistence depends on raw backend behavior.

## Failure Semantics
- Invalid raw storage contract -> `LOX_ERR_INVALID`.
- Sync probe failure on mount (when enabled) -> `LOX_ERR_STORAGE`.
- Write/erase/sync raw failure is propagated as storage error.
- Open-time capability/storage mismatch fails closed in classifier before adapter activation.

## Current Test Coverage
- Unit:
  - `tests/test_backend_fs_adapter.c`
- Open-path routing:
  - `tests/test_backend_open.c`
- Decision/registry coverage for `fs_stub` and `block_stub`:
  - `tests/test_backend_decision.c`
  - `tests/test_backend_registry.c`
- Power-cut/reopen integration:
  - `tests/test_backend_fs_recovery.c`
