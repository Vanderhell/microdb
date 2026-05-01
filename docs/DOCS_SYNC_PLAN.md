# Documentation Sync Plan (loxdb <-> loxdb_pro)

Date: 2026-05-01
Status: active baseline

## Goal
Keep `loxdb` (core) and `loxdb_pro` (product layer) documentation aligned without duplicating ownership.

## Ownership Rules

### Canonical in `loxdb` (core)
- Engine behavior, invariants, fail-code semantics, storage contract.
- Porting/backend integration at engine boundary.
- Core API and deterministic runtime limits.

### Canonical in `loxdb_pro` (product)
- Policy/admission decisions over core facts.
- Operations workflows (backup/retention/scheduler/monitor/alerting).
- Compliance/evidence packaging and operator guidance.

### Shared by reference (no copy)
- Boundary and ownership docs should reference each other, not fork content.

## Required Link Parity (must exist)

### In `loxdb` README/docs map
- link to PRO boundary/ops overview docs as external dependency context.

### In `loxdb_pro` README/docs map
- link to core limits/fail-code/startup docs as semantic source.

## Sync Checklist (per change cycle)
1. Update docs in canonical repo first.
2. Update cross-links in the other repo.
3. Run doc lint/check pass (headings/links/ownership tags).
4. Record change in both repos' change logs or release notes.

## Current Gaps to Watch
- README content tree in `loxdb_pro` has encoding artifacts; keep functional link sections authoritative.
- Ensure no duplicated fail-code definitions drift from core.

## Next Steps
- Add lightweight doc consistency script (broken links + ownership label checks).
- Add release gate item: "Docs sync verified for core/pro boundary".
