# Change Cycle Checklist (Core: loxdb)

Use this checklist for every change batch.

## 1) Scope
- [ ] Define change goal in 1-3 sentences.
- [ ] Classify scope: `core api` / `engine internals` / `docs only` / `tests only`.
- [ ] Confirm no boundary violation against `docs/CORE_PRO_BOUNDARY.md` equivalent guidance.

## 2) Design Guardrails
- [ ] Identify affected invariants (`RAM`, `storage`, `fail-codes`, `recovery`).
- [ ] Decide if behavior change is backward-compatible.
- [ ] If behavior changes, update contract docs before merge.

## 3) Implementation
- [ ] Apply minimal patch set.
- [ ] Keep deterministic behavior (no hidden fallback).
- [ ] Add/adjust logs only where operationally useful.

## 4) Tests
- [ ] Run targeted unit/integration tests for touched area.
- [ ] Run at least one persistence/reopen/recovery lane when storage path touched.
- [ ] Reproduce one negative-path test when fail behavior touched.

## 5) Documentation Sync
- [ ] Update canonical core docs (`LIMITS_AND_FAILURES`, `STARTUP_DECISION_FLOW`, etc.).
- [ ] Update `docs/README.md` if new docs were added.
- [ ] Verify cross-repo links per `docs/DOCS_SYNC_PLAN.md`.

## 6) Release Hygiene
- [ ] Summarize change impact (what changed, risk, rollback).
- [ ] Add changelog/release note entry if user-visible.
- [ ] Confirm tree is ready for commit.

## 7) Done Criteria
- [ ] Behavior is deterministic and explained.
- [ ] Tests pass for touched scope.
- [ ] Docs and links are synchronized.
