# Safety notes (non-certified)

`loxdb` is **not** a certified safety/security library. This document is an honest summary of what the repository *does* and *does not* provide today, so integrators can make appropriate project-level decisions.

## What is tested

Evidence that exists in this repository:

- Deterministic return-code contract (`lox_err_t`) validated by tests (see `tests/`).
- Durability and recovery behavior validated by WAL/recovery tests.
- Multi-profile/configuration coverage via build-time/profile matrices (see `CMakeLists.txt`).
- Sanitizer lane on Linux (ASan/UBSan) in CI.
- Static analysis via cppcheck in CI (non-blocking lanes today).
- Read-only offline verifier (`lox_verify`) for persisted images (see `docs/OFFLINE_VERIFIER.md`).

## What is *not* claimed

No claims are made about:

- compliance with any specific safety/security standard
- MISRA compliance status
- tool qualification packages
- a complete safety case / hazard analysis / threat model

If you need those, you must establish them at the product/program level and treat this library as a component within that process.

## Integration expectations for safety-critical use

If you consider deploying in a safety- or mission-critical context, typical minimum expectations include:

1. Run the full test suite on your production toolchain and flags.
2. Validate the storage HAL contract on your real media (`erase_size`, `write_size`, `read/write/erase/sync`).
3. Provide real locking when concurrency is possible (`LOX_THREAD_SAFE=1` + lock hooks).
4. Execute power-loss testing around WAL replay and compaction boundaries on your target hardware.
5. Pin and document configuration (profile, RAM/storage budgets, split percentages, retention policies).
6. Add your own fault-injection, stress, and long-duration validation gates appropriate to the product.

