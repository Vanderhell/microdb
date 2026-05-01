# Startup Decision Flow

## Purpose
Deterministic startup sequence and decision points before/around `lox_init`.

## Required Sequence
1. platform init
2. storage open/mount
3. preflight checks (`lox_preflight`)
4. image/open policy
5. `lox_init`
6. stream/table registration

## Decision Tree
- If preflight fails:
  - return actionable error before `lox_init`:
    - `LOX_ERR_INVALID`: config/storage contract invalid.
    - `LOX_ERR_NO_MEM`: RAM split infeasible.
    - `LOX_ERR_STORAGE`: storage capacity/geometry infeasible.
- If image mismatch:
  - recreate storage image when profile/storage-size contract changed.
- If `LOX_ERR_CORRUPT`:
  - attempt controlled recreate/recovery path, then single retry.
- If `LOX_ERR_EXISTS`:
  - treat as non-fatal in idempotent object registration flows.
- If `LOX_ERR_NO_MEM`/`LOX_ERR_FULL` during setup:
  - reduce runtime profile (tables/rows/streams/split) and retry with explicit log.

## Recommended Logging
- configuration snapshot
- preflight result
- init fail code + text
- recovery action taken
- final startup outcome (`success` / `blocked` + reason)

## Determinism Requirements
- same inputs => same startup result
- no hidden fallback without log evidence

## Minimal Example
```c
lox_preflight_report_t rep;
lox_err_t rc = lox_preflight(&cfg, &rep);
if (rc != LOX_OK) {
    // log rc + rep.storage_required_bytes/rep.storage_capacity_bytes etc.
    return rc;
}
rc = lox_init(&db, &cfg);
```
