# Release Notes Draft (Next Release)

## Title

`loxdb v1.3.7`

## Release text (GitHub Release body)

This release expands the free MIT core with runtime integrity checks, WCET
documentation/bounds, and logarithmic time-series retention.

Highlights:

- Added `lox_selfcheck()` to validate internal KV/TS/REL/WAL invariants at runtime.
- Added WCET package:
  - `include/lox_wcet.h`
  - `docs/WCET_ANALYSIS.md`
  - `tests/test_wcet_bounds.c`
- Added TS logarithmic retention path:
  - `LOX_TS_POLICY_LOG_RETAIN`
  - `lox_ts_register_ex(...)`
  - per-stream log-retain zone configuration.
- Added dedicated test coverage:
  - `tests/test_selfcheck.c`
  - `tests/test_wcet_bounds.c`
  - `tests/test_ts_log_retain.c`

Validation summary:

- New suites pass:
  - `test_selfcheck`
  - `test_wcet_bounds`
  - `test_ts_log_retain`
- Full preset regression remained green after integration.

## Contract links

- `README.md`
- `CHANGELOG.md`
- `RELEASE_LOG.md`
- `docs/WCET_ANALYSIS.md`
- `docs/SAFETY_READINESS.md`
- `docs/OFFLINE_VERIFIER.md`
- `docs/PROFILE_GUARANTEES.md`

## Repository topics reference

See `docs/repository-topics.md`.
