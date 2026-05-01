# LoxDB Core Specification: Boundary with LoxDB PRO, Non-Duplication, and Improvement Plan

Date: 2026-05-01
Repository: loxdb

## 1) Executive Summary

`loxdb` should remain the deterministic embedded storage engine core.
`loxdb_pro` should remain the productization/governance/security/operations layer.

Current state is close to correct, but there is boundary pressure in areas like image-management, policy, and operational UX. The main risk is accidental duplication of responsibilities across repositories.

This document defines what must stay in core, what must move/stay in PRO, where duplication risk exists, and what to implement next.

## 2) Role of This Repository (Core Contract)

Core is responsible for:
- deterministic data engine behavior (KV/TS/REL),
- strict storage contract + durability semantics,
- predictable memory and fail-code behavior,
- portable storage HAL interface,
- correctness, recovery, integrity checks,
- minimal optional wrappers/adapters that remain engine-adjacent.

Core is not responsible for:
- commercial governance features,
- fleet/operational control planes,
- security policy orchestration,
- business-grade admission policy bundles,
- certification packaging workflows.

## 3) Non-Duplication Boundary (Core vs PRO)

### 3.1 Must stay in `loxdb` (single source of truth)
- `lox_*` data model APIs and fail-code semantics.
- storage bootstrap/recovery/WAL semantics.
- deterministic admission primitives tied directly to engine math (resource feasibility checks).
- low-level offline verification primitives that validate image consistency.
- capability-based backend decision primitives (engine-facing only).

### 3.2 Must stay in `loxdb_pro` (single source of truth)
- governance/policy orchestration (quotas, op filtering, policy packs).
- security-at-rest orchestration, tamper response workflows.
- operational modules (backup/restore scheduler/alerting/monitor).
- product CLI and operator UX.
- certification evidence packaging pipelines.

### 3.3 Allowed shared pattern (without duplication)
- Core exposes narrow, stable primitives.
- PRO composes them into higher-level workflows.
- No business logic forks of core algorithms inside PRO.

## 4) Duplication Risk Assessment

### Risk A: Admission logic duplicated in both repos
- Symptom: core has feasibility checks, PRO re-implements separate heuristics.
- Rule: Core computes feasibility facts; PRO only applies policy decisions.

### Risk B: Media image-management in core public API
- Symptom: core starts owning filesystem inventory/catalog lifecycle.
- Rule: core may expose image validation primitives, but file inventory UX belongs to PRO or examples/tools.

### Risk C: Multiple error taxonomies
- Symptom: pro-specific statuses drift from core `lox_err_t` meanings.
- Rule: core codes stay canonical; PRO maps (never redefines) meanings.

### Risk D: Security wrappers in both layers
- Symptom: secure wrappers emerge in core and PRO with overlapping responsibility.
- Rule: core stays crypto-agnostic; PRO owns secure storage wrappers.

## 5) Concrete Improvement Plan for `loxdb`

## Phase 1: Boundary Hardening (high priority)
1. Add explicit boundary doc in core docs map:
   - what belongs to core vs PRO.
2. Freeze core public API ownership table:
   - each public header tagged as core-owned or optional-tooling-only.
3. Add CI gate for boundary drift:
   - detect forbidden include dependencies or namespace leaks toward PRO module concepts.

## Phase 2: Deterministic Startup/Feasibility
1. Introduce first-class `lox_preflight_*` API in core:
   - RAM feasibility (kv/ts/rel slices),
   - storage layout feasibility,
   - predicted hard fail reasons.
2. Add startup decision contract:
   - deterministic status map before `lox_init`.
3. Keep API policy-neutral:
   - return facts, not profile policy.

## Phase 3: Diagnostics and Explainability
1. Expand structured diagnostics:
   - machine-parseable preflight result structs,
   - stable reason codes for "why init will fail".
2. Add deterministic reproduction metadata:
   - seed/config snapshot helpers for test replay.

## Phase 4: Core-Only Utility Scope Cleanup
1. Keep image verify helpers in core tooling.
2. Keep multi-image inventory/catalog APIs out of core public C API.
3. If needed, place inventory utilities under tools/examples with explicit non-core label.

## 6) Proposed `loxdb` Artifacts to Add

1. `docs/CORE_PRO_BOUNDARY.md`
- normative boundary matrix and ownership rules.

2. `docs/PREFLIGHT_API_CONTRACT.md`
- preflight data model, status codes, deterministic guarantees.

3. `docs/API_OWNERSHIP_TABLE.md`
- every public header/function classified as core/optional/tooling.

4. `tests/test_preflight_contract.c`
- deterministic feasibility tests.

5. `tests/test_boundary_no_pro_leak.cmake`
- check for forbidden symbol/include leakage.

## 7) Compatibility Rules

- Any feature requiring policy, scheduling, retention orchestration, external operator workflow, or commercial packaging belongs in PRO.
- Core additions must be usable without PRO and without dynamic policy engines.
- Optional modules in core must not redefine PRO concerns.

## 8) What to Avoid in Core Going Forward

- no policy packs,
- no cert-report orchestration logic,
- no secure-at-rest product wrappers,
- no high-level media catalog lifecycle in public engine API.

## 9) Near-Term Prioritized Backlog (Core)

1. `lox_preflight_*` API + tests.
2. Boundary doc + ownership table.
3. CI boundary-drift gate.
4. Structured startup explainability (`why init fails`).
5. Keep existing optional wrappers but classify them explicitly as non-PRO substitutes.

## 10) Success Criteria

- User can predict init success/failure before calling `lox_init`.
- No overlapping responsibility with PRO governance/security modules.
- Core remains small, deterministic, portable, and licensable as independent OSS engine.
