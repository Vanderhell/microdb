# Professional readiness

loxdb is a systems/embedded-oriented library. This document lists the engineering properties that make the library usable in serious production systems with strict quality expectations.

This is not a certification claim. It is a checklist of what to define, prove, and ship.

## 1) Explicit contract
- Define guarantees vs non-guarantees (durability, consistency, concurrency, data loss boundaries).
- Define integration obligations (storage HAL contract, locking model, time source, power-loss assumptions).
- Define error-code semantics and what each error means for recovery.

## 2) Determinism & bounded resources
- Provide compile-time limits for memory and object sizes (no hidden unbounded growth).
- Avoid unbounded recursion; keep stack usage bounded and explain worst-case stack depth.
- Provide deterministic latency guidance and a method to derive WCET-style bounds on target.

## 3) Failure handling & recovery
- Fail fast on unsupported storage capabilities (contract checks at init/open).
- Document crash consistency rules and the exact recovery behavior after power loss.
- Provide a failure-mode table: trigger -> detection -> return code -> expected recovery action.
- Provide an offline integrity verifier for persisted images (pre-release QA gate).

## 4) Verification evidence
- Maintain a traceable invariant table: invariant -> test(s) -> status.
- Include deterministic fault-injection tests around read/write/erase/sync and power-cut boundaries.
- Include property/fuzz-style stress tests with seed capture + replay.
- Include long-soak tests and publish summarized artifacts (trend dashboards are ideal).

## 5) Static analysis & hardening
- Keep a compiler warnings baseline and treat new warnings as failures (per toolchain).
- Run sanitizers where available (ASan/UBSan) and keep the lane green.
- Run at least one static analyzer (e.g., cppcheck) and track findings over time.
- If using coding rules (e.g., MISRA/CERT), document deviations and enforce what you can mechanically.

## 6) Security posture (library-level)
- Avoid undefined behavior and memory-safety hazards as a primary security control.
- Provide guidance for secure integration boundaries (key management and secure boot are system responsibilities).
- Ensure no debug/diagnostic features leak sensitive data by default.

## 7) Configuration governance
- Provide a small set of named profiles (safe/default/aggressive) with explicit RAM/storage budgets.
- Define forbidden configuration combinations and validate them at init.
- Make configuration artifacts reproducible (profiles checked into repo; build presets pinned).

## 8) Release evidence
For each release, capture and archive:
- exact source revision and toolchain versions
- CI results and test reports (including fault-injection and soak summaries)
- static analysis outputs
- SBOM and dependency provenance
- documentation snapshot (contracts, limits, known limitations)

## 9) API stability & compatibility
- Version the API and document compatibility rules (source/binary/data-format).
- Treat on-disk format changes as a first-class contract: document migration strategy and failure behavior.

## Where this repo already provides evidence
- docs/SAFETY_READINESS.md - contracts, failure-mode table, invariant traceability, static analysis posture
- docs/WCET_ANALYSIS.md - latency bound methodology
- docs/results/ - validation summaries and trend dashboard

