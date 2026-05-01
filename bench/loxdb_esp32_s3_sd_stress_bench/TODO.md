# TODO

- Implement robust init admission flow for loxdb stress bench:
  - add preflight profile validation before `lox_init` (RAM/layout feasibility checks),
  - add deterministic fallback profile ladder (A/B/C) and select first valid profile,
  - replace fatal-only startup with actionable user-facing status/errors (`what failed` + `how to fix`).

## Regulated-readiness (Aerospace / Aviation / Medical)

- Define formal system contract:
  - explicit guarantees/non-guarantees,
  - deterministic RAM/storage/latency limits,
  - defined behavior on limit exceed.
- Implement deterministic startup safety:
  - mandatory preflight admission before `lox_init`,
  - fail-safe startup (no opaque FATAL),
  - recovery playbook per error code.
- Add verification & validation matrix:
  - traceability `requirement -> design -> test -> result`,
  - unit/integration/system tests,
  - fault-injection (power-loss, partial write, corrupt page, low-memory).
- Add configuration governance:
  - approved profile ladder (safe/default/aggressive),
  - forbidden config combinations,
  - reproducible config artifacts per release.
- Add release/audit artifacts:
  - versioned test reports, coverage, static analysis outputs,
  - changelog discipline and release evidence bundle,
  - SBOM + dependency/supply-chain checks.
- Aerospace/avionics track:
  - map lifecycle/documentation to DO-178C expectations (by target DAL),
  - prepare determinism/recovery evidence for integration context,
  - identify tool qualification needs where applicable.
- Medical track:
  - align lifecycle outputs to IEC 62304,
  - align risk management outputs to ISO 14971,
  - prepare QMS-compatible documentation structure (ISO 13485 context).
