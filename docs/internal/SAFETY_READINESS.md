# Safety Readiness

loxdb is not a certified library. This document describes artifacts to support your project's certification process.

## 1. Scope and limitations

- IEC 62443-4-2
  - Covers: deterministic storage behavior, durability/recovery tests, explicit storage contract checks.
  - Does not cover: full product cybersecurity lifecycle, threat modeling, secure boot/key management.
- ISO 26262 ASIL-B subset
  - Covers: bounded-memory design, explicit error codes, replay/consistency invariants, traceable tests.
  - Does not cover: full safety case, item definition/HARA, ASIL decomposition, tool qualification package.
- DO-178C DAL-D
  - Covers: low-level behavioral evidence (unit tests, invariants, static checks, failure contracts).
  - Does not cover: complete requirements baseline, trace matrices to system requirements, PSAC/verification plans.
- IEC 60730 Class B
  - Covers: fault-aware startup checks in storage path, deterministic API fail-fast behavior.
  - Does not cover: MCU self-test suite, hardware diagnostics mandated by appliance standard.

## 2. Memory safety claims

### CLAIM-MEM-01: No heap allocation after `lox_init()` returns `LOX_OK`.

- Statement: Runtime operation paths do not allocate/free heap memory.
- Evidence: `src/loxdb.c:lox_init` performs the main heap allocation; operational APIs use preallocated arenas (`src/lox_kv.c`, `src/lox_rel.c`, `src/lox_wal.c`).
- Verification method: ASan/UBSan CI lane plus long-run reopen/recovery tests.
- Status: [VERIFY: needs toolchain-specific validation]

### CLAIM-MEM-02: No public API call can write beyond a caller-supplied buffer.

- Statement: Public getters/checkers validate target buffers before copy and return overflow-style errors.
- Evidence: `lox_kv_get` validates `buf_len` and returns `LOX_ERR_OVERFLOW`; REL row helpers validate column sizes/types before copy (`src/lox_rel.c`).
- Verification method: API contract tests and sanitizer lane.
- Status: [VERIFY: needs toolchain-specific validation]

### CLAIM-MEM-03: No use-after-free via public API.

- Statement: Public entry points validate DB handle magic, and deinit clears state before memory release.
- Evidence: `src/loxdb.c:lox_validate_handle`, `src/loxdb.c:lox_deinit`.
- Verification method: invariant test `invariant_magic_cleared_before_heap_free`, null/invalid handle matrix tests.
- Status: Partially verified.

### CLAIM-MEM-04: Stack depth is bounded by compile-time constants.

- Statement: Core code is iterative (no recursion) and stack-local buffers are fixed-size.
- Evidence: `src/*.c` uses bounded locals and loops over compile-time/static bounds (`LOX_*` limits).
- Verification method: source inspection + compiler diagnostics + sanitizer test matrix.
- Status: [VERIFY: needs toolchain-specific validation]

## 3. Failure mode table

| Failure | Trigger | Detection | Return code | Recovery |
|---|---|---|---|---|
| Null handle passed to any API | `db == NULL` | Entry validation in public API | `LOX_ERR_INVALID` | Caller fixes API use |
| malloc fails at `lox_init()` | Heap exhaustion | `malloc` result check | `LOX_ERR_NO_MEM` | Increase RAM budget / reduce profile |
| Storage HAL `read()` returns error | Backend I/O failure | Storage read wrapper checks | `LOX_ERR_STORAGE` | Retry/reopen/failover backend |
| WAL tail truncated by power loss | Partial final WAL entry | Replay stops at first invalid entry | `LOX_OK` | Durable snapshot remains valid |
| WAL header corrupt | Header CRC/magic invalid | WAL header validation | `LOX_OK` | WAL reset path, continue from snapshot |
| Both snapshot banks corrupt | CRC/header mismatch in both banks | Bank validation on boot | `LOX_ERR_CORRUPT` | Reinitialize DB image |
| KV full with REJECT policy | Entry/value store saturated | Insert path capacity checks | `LOX_ERR_FULL` | Delete keys / resize profile |
| KV full with OVERWRITE policy (LRU eviction) | Entry/value store saturated | Insert path + eviction branch | `LOX_OK` | Oldest entry evicted |
| REL table at max_rows | No free row slots | REL insert checks | `LOX_ERR_FULL` | Delete rows / increase table max_rows |
| TS ring full with DROP_OLDEST policy | Stream capacity reached | TS insert policy branch | `LOX_OK` | Oldest sample dropped |
| TS ring full with REJECT policy | Stream capacity reached | TS insert policy branch | `LOX_ERR_FULL` | Caller retries later / clears stream |
| Concurrent access without mutex (`LOX_THREAD_SAFE=0`) | Multi-thread access on no-op lock config | No runtime detector | `LOX_OK` (undefined behavior risk) | Integrator must provide lock hooks |
| TXN rollback (explicit) | `lox_txn_rollback` called | Transaction state machine | `LOX_OK` | Staged mutations discarded |
| Schema mismatch on reopen (`schema_version` changed) | Table schema version drift | REL schema checks/migration path | `LOX_ERR_SCHEMA` | Apply migration callback or reset schema |
| Key TTL expired on get | Current time >= `expires_at` | KV expiry check on get | `LOX_ERR_EXPIRED` | Caller refreshes key |
| `write_size != 1` at init | Unsupported backend contract | Storage contract validation | `LOX_ERR_INVALID` | Use adapter or compliant backend |

## 4. Invariant traceability table

| ID | Invariant | Test file | Test function | Status |
|---|---|---|---|---|
| INV-1 | Active snapshot bank is never overwritten in place | `tests/test_durability_closure.c` | `power_cut_after_each_compact_step_keeps_committed_state` | TESTED |
| INV-2 | Compact/flush writes inactive bank first, then switches superblock | `tests/test_safety_invariants.c` | `invariant_superblock_switches_on_compact` | TESTED |
| INV-3 | Boot selects only validated durable state | `tests/test_durability_closure.c` | `corrupt_active_page_header_crc_fails_boot` | TESTED |
| INV-4 | WAL replay stops at first invalid entry | `tests/test_safety_invariants.c` | `invariant_wal_replay_stops_at_corrupt_entry` | TESTED |
| INV-5 | TXN_KV entries require TXN_COMMIT marker | `tests/test_safety_invariants.c` | `invariant_txn_without_commit_not_visible_after_reopen` | TESTED |
| INV-6 | Callback APIs run callbacks outside internal lock | `tests/test_rel.c` | `rel_find_mutation_during_callback_returns_modified` | TESTED |
| INV-7 | Public DB-handle APIs lock/unlock consistently | `tests/test_thread_safety.c` | `test_counts_balanced_after_sequence` | TESTED |
| INV-8 | Profile/perf hooks are compile-time only | `tests/test_profile_matrix.c` | `test_profile_contract_end_to_end` | TESTED |
| INV-9 | Storage contract fail-fast (`erase_size > 0`, `write_size == 1`) | `tests/test_fail_code_contract.c` | `contract_storage_zero_write_size_on_init_returns_invalid` | TESTED |

## 5. Static analysis posture

- `cppcheck` CI command: `bash scripts/run_static_analysis.sh`
- Compiler warning baseline: `gcc -std=c99 -Wall -Wextra -Wpedantic -Wconversion`
- Sanitizers: Linux `ci-asan-ubsan-linux` job with ASan/UBSan enabled

MISRA-C:2012 deviation notes (current code patterns):

- Rule 15.5 (single point of exit): common `goto unlock`/multi-return cleanup in `src/lox_kv.c` and `src/lox_rel.c`.
- Rule 11.5 (casts from `void *`): arena allocations cast to typed pointers in `src/lox_kv.c` and `src/lox_rel.c`.
- Rule 20.x (function-like macros): lock/logging macros (`LOX_LOCK`, `LOX_UNLOCK`, `LOX_LOG`) are widely used.
- Rule 21.6 (standard library memory/string functions): heavy reliance on `memcpy/memmove/memset/strncmp`.
- Rule 10.3/10.8 style conversions: explicit narrowing from `size_t` to `uint32_t` in capacity/layout paths.

These are controlled deviations for embedded C pragmatism and require project-level MISRA justification records.

## 6. Integration checklist

1. Validate compiler/toolchain behavior used for production build (and rerun full test suite with that toolchain).
2. Provide real mutex hooks when concurrency is possible (`LOX_THREAD_SAFE=1` + lock callbacks).
3. Validate backend HAL contract (`read/write/erase/sync`, `erase_size > 0`, `write_size == 1`).
4. Execute hardware power-loss tests around WAL replay and compaction boundaries.
5. Run CI/test matrix with at least one secondary compiler (for example clang + gcc).
6. Include offline verifier in QA gate for persisted images before release.
7. Map all `lox_err_t` codes into your firmware FMEA and recovery policy.
8. Review schema migration callback behavior and rollback strategy before field updates.
9. Document RAM/storage budget, capacity headroom, and WAL threshold rationale per product SKU.
