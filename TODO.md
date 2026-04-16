# TODO

## Storage Architecture Roadmap (Core + Adapters)

### North Star
- Keep `microdb` core strict and durable.
- Achieve hardware universality through adapter/translation layers, not by weakening core contracts.

### Non-Negotiable Core Boundaries
- Core owns: transactions, WAL/snapshot ordering, recovery rules, fail-closed boot, diagnostics contract, error contract.
- Adapters own only: medium constraints, alignment/page/block rules, low-level flush/sync/erase mapping.

### Implementation Order
1. Add storage capability descriptor.
2. Introduce explicit backend classes (`BYTE`, `ALIGNED`, `MANAGED`).
3. Add open-time compatibility gate (`direct` / `via_adapter` / `unsupported`).
4. Implement aligned adapter (RMW + bounce buffer + alignment enforcement).
5. Implement filesystem/block adapter.
6. Implement managed/FTL adapter (NAND/eMMC via managed layer, not raw NAND first).
7. Add backend matrix tests and hard gates.
8. Add configurable capacity profiles (`2/4/8/16/32 MiB`) in C++ wrapper/backend config.
9. Add HTML capacity estimator (records-fit calculator) aligned with DB layout rules.
10. Package adapters modularly so linker pulls only used backends.

### Progress Snapshot
- [done] 8) Capacity Profiles: done (2/4/8/16/32 MiB, C + C++ profile mapping, profile tests).
- [done] 9) HTML Capacity Estimator: done (	ools/microdb_capacity_estimator.html).
- [done] C++ wrapper baseline: done in staged commits (lifecycle/stats + KV + TS + REL + txn + typed helpers).
- [in_progress] 10) Modular Backend Packaging + Linker-Friendliness.
- [done] 10.1 optional backend targets + strip-link gate.
- [done] 1) capability descriptor module (microdb_backend_adapter.h) in optional adapter layer.
- [done] 3) open-time compatibility classifier module (direct/via_adapter/unsupported) in optional adapter layer.
- [done] Decision flow helper by backend name (registry + compat composition).
- [in_progress] 4) ALIGNED adapter implementation started (RMW + bounce buffer shim + tests as optional module).
- [done] Optional backend open wiring helper (decision + adapter activation path) added as separate module.
- [done] Managed adapter skeleton module added (optional, linker-friendly split) + backend open wiring support.
- [done] Managed adapter expectations + mount sync-probe failure semantics added with tests.
- [done] Managed-path power-cut/reopen integration tests added (through backend-open wiring + WAL replay checks).
- [done] Managed-path stress/fault matrix added (mixed KV/TS/REL + repeated crash/power-loss reopen cycles).
- [done] Managed stress sliced into smoke/long CTest lanes.
- [done] Runtime envelope thresholds wired into managed stress smoke/long lanes (`--max-ms` gate).
- [done] Per-platform calibration hooks and baseline documentation added (`MICRODB_MANAGED_STRESS_*_MAX_MS` + docs/MANAGED_STRESS_BASELINES.md).
- [done] Calibrated thresholds wired into CI matrix via `CMakePresets.json` (`ci-debug-linux` / `ci-debug-windows`).
- [next] Extend release workflow to consume dedicated release presets with profile-specific managed stress budgets.

### 1) Storage Capability Descriptor
- Add capability fields to storage HAL:
  - backend class
  - minimal write unit
  - erase granularity
  - atomic write granularity
  - sync semantics level
  - managed/unmanaged marker
- Require capability publication at `open/init`.

### 2) Backend Classes
- `BYTE`: native byte-write durable backends (SPI NOR, RAM/file test backends).
- `ALIGNED`: page/block aligned devices requiring adapter semantics.
- `MANAGED`: FTL/filesystem managed persistence.

### 3) Open-Time Validation (Fail-Fast)
- At `open`, classify backend as:
  - core-compatible directly
  - core-compatible through adapter
  - unsupported (fail with explicit error)
- No speculative "maybe works" path.

### 4) Aligned Adapter
- Provide byte-write semantics to core via:
  - read-modify-write staging
  - bounce buffer
  - strict offset/length alignment rules
- Preserve core ordering and durability guarantees.

### 5) Filesystem/Block Adapter
- Map flush/sync policy explicitly.
- Define persistence points and power-fail behavior contract.
- Keep corruption-test harness coverage.

### 6) Managed/FTL Adapter
- Target eMMC/SD/NAND through managed interface, not raw NAND first.
- Define FTL expectations and failure semantics.
- Verify with power-cut/recovery tests before claiming support.

### 7) Backend Matrix Tests
- Per backend class, add required tests:
  - open/reopen
  - transaction correctness
  - compact behavior
  - corruption handling
  - power-cut recovery
  - near-full behavior
  - effective capacity reporting
  - worst-case latency envelope
- CI gates must fail on contract regressions.

### 8) Capacity Profiles (C++ Wrapper / Backend Config)
- Add capacity profiles: `2/4/8/16/32 MiB` (use `MiB`, not `Mb`).
- Keep this outside core invariants; model as backend/wrapper configuration.
- Ensure effective capacity and near-full behavior are validated for low and high profiles.
- At minimum, gate tests on `2 MiB` and `32 MiB`.

### 9) HTML Capacity Estimator
- Create a small HTML tool that estimates how many records fit into configured capacity.
- Use the same sizing rules as runtime layout (headers, WAL/snapshot overhead, alignment, metadata).
- Treat it as tooling/documentation aid only; must not alter core behavior.

### 10) Modular Backend Packaging + Linker-Friendliness
- Prepare future memory support (NAND, eMMC, SD, other managed media) as optional modules.
- Register backends/adapters explicitly (compile-time flags or target-based linkage), no implicit global pull-in.
- Keep per-backend code split in separate translation units/libraries so dead-stripping removes unused modules.
- Avoid hard references from core to optional adapters; use thin interfaces/factory registration boundaries.
- Preserve fail-fast open-time compatibility checks regardless of which modules are linked.

#### 10.1) Concrete Next Step
- Add explicit CMake options/targets for optional backend modules (initially stubs + integration points).
- Ensure base `microdb` target has zero hard link dependency on optional backend modules.
- Add a linker-size/contents gate proving unused modules are stripped from default build.

### Current Truth (Do Not Relax)
- Current core storage contract:
  - `erase_size > 0`
  - `write_size == 1`
- This remains strict until adapter path is fully implemented and validated.
