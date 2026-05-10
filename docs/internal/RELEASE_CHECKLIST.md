# Release Checklist

Use this checklist before creating a public release tag.
Last verification pass: 2026-04-19 (desktop + ESP + packaging).

## 1. Validation Gates

- [x] Desktop full validation passed:
  - `tools/run_full_validation.ps1 -SkipBuild -SkipEsp32`
  - latest summary archived in `docs/results/`
  - latest run: `docs/results/validation_summary_20260419_180500.md` (all profiles `PASS`)
- [x] ESP validation passed on current firmware:
  - `deterministic`, `balanced`, `stress`
  - logs archived in `docs/results/`
  - latest run: `docs/results/validation_summary_20260419_193234.md` (all ESP profiles `PASS` on `COM17`)
- [x] Hard verdict updated:
  - `docs/results/hard_verdict_YYYYMMDD.md`
  - latest: `docs/results/hard_verdict_20260419.md`
- [x] Release preset smoke lanes are green for both platforms:
  - `test_backend_managed_stress_smoke`
  - `test_backend_fs_matrix_smoke`
  - `test_optional_backend_strip_gate`
  - desktop Release run on 2026-04-19: PASS; ESP runtime lanes PASS in `validation_summary_20260419_193234.md`
- [x] Release preset footprint/tiny guards are green:
  - `test_tiny_size_guard`
  - `test_footprint_min_size_gate_release`
  - `test_tiny_size_guard` PASS (Release, 2026-04-19)
  - `test_footprint_min_size_gate_release` not registered in this host preset; equivalent `test_footprint_min_baseline` PASS (Release, 2026-04-19)
- [x] SPDX header guard is green:
  - `tools/check_spdx_headers.ps1`

## 2. Product Contract Gates

- [x] Positioning sentence is final:
  - `docs/PRODUCT_POSITIONING.md`
- [x] Profile guarantees table is updated with latest results:
  - `docs/PROFILE_GUARANTEES.md`
- [x] Fail-code contract is current:
  - `docs/FAIL_CODE_CONTRACT.md`
- [x] Supported/unsupported scope is explicit in product docs
- [x] Statement is explicit: effective capacity is not target capacity
- [x] Statement is explicit: stress is not a low-latency profile
- [x] Statement is explicit: deterministic is the profile for controlled latencies
- [x] Statement is explicit: reopen/compact are maintenance operations, not normal write latency

## 3. Adoption Gates

- [x] Quick onboarding is valid:
  - `docs/GETTING_STARTED_5_MIN.md`
- [x] Product brief is current:
  - `docs/PRODUCT_BRIEF.md`
- [x] README links are correct:
  - `README.md`
  - local relative-link check PASS (2026-04-19)

## 4. Build and Packaging Gates

- [x] Target build config is clean (`Debug`/`Release` as intended)
- [x] Artifacts generated for all promised platforms
- [x] Checksums generated for each distributable archive
- [x] Archive names match release notes template
- [x] Gate mapping to release criteria is complete:
  - Durability/recovery: `test_durability_closure`, `test_resilience`, `test_wal`
  - API contract: `test_api_contract_matrix`, `test_fail_code_contract`
  - Capacity/profiles: `test_profile_core_*`, `test_storage_capacity_profiles`
  - Backend adapters: `test_backend_open`, `test_backend_*_recovery`
  - all listed mapping tests PASS on 2026-04-19 (Debug)

## 5. Release Metadata Gates

- [x] Tag version selected (example: `v1.1.0`)
  - selected: `v1.0.0`
- [x] Release notes prepared:
  - `docs/release-notes.md`
- [x] Repository topics checked:
  - `docs/repository-topics.md`

## 6. Final Go/No-Go

- [x] No unresolved `FAIL` in latest validation summary
- [x] No unresolved contract/documentation mismatch
- [x] Tag + release publish approved
- [x] No hidden worst-case latency in release text
- [x] No unsupported claims (`enterprise-grade`, etc.) without hard evidence
- [x] No marketing filler that weakens technical clarity
