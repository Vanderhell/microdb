# loxdb editions

This repository is the **core** embedded database engine. A planned commercial edition (`loxdb_pro`) is intended to add higher-level operational tooling and workflows **on top of** the core, without duplicating core responsibilities.

This document is a distillation of:
- `ROOT_SPEC_CORE_VS_PRO.md`
- `docs/LOXDB_PRO_BACKLOG.md`

## loxdb (this repository) — MIT licensed OSS core

`loxdb` core is responsible for:

- deterministic engine behavior (KV / TS / REL)
- strict storage contract and durability semantics (WAL + recovery)
- predictable memory behavior and stable return-code contract (`lox_err_t`)
- a portable storage HAL (`lox_storage_t` callbacks)
- correctness evidence (tests, sanitizer lanes, static analysis)
- small, engine-adjacent optional helpers (for example read-only image verification)

Non-goals for core:

- operational control planes, fleet workflows, and “operator UX”
- governance/policy orchestration layers (quotas, policy packs, admission bundles)
- security-at-rest orchestration and tamper-response workflows
- certification packaging pipelines and compliance “kits”
- multi-image media catalogs and filesystem inventory lifecycle in the core public API

Stability intent:

- The MIT-licensed core API surface (`lox_*` symbols and current public headers) is intended to remain stable across releases.
- Features shipped in the OSS core are not intended to be removed or relicensed away from MIT in future versions.

## loxdb_pro — planned commercial edition (separate repository)

`loxdb_pro` is planned as a separate product/repository, shipping **additional modules** that compose core primitives into operational workflows.

Target scope (examples, distilled from the PRO backlog):

- multi-database image management on shared media (SD/eMMC/FS catalogs)
  - discovery/scanning, classification (valid/corrupt/non-loxdb), fingerprinting
  - lifecycle operations (create/use/rename/clone/delete) with safety rails (dry-run)
  - optional manifest/catalog for fast startup and drift reconciliation
- extended observability and operational tooling
  - structured diagnostics around scan/open/manage operations
  - operator-friendly commands/UX (`db list`, `db info`, `db verify`, etc.)
- commercial support and integration assistance

Boundary rules (non-duplication):

- Core exposes narrow, stable primitives; PRO composes them into workflows.
- Core remains policy-neutral and crypto-agnostic; PRO may implement policy/security orchestration.
- PRO must map (not redefine) core error semantics; no separate competing error taxonomy for core behavior.

