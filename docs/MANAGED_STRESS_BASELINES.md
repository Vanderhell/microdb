# Managed Stress Runtime Baselines

This file tracks runtime envelope baselines used as release acceptance criteria
for managed backend stress lanes.

## Configurable Gates

Configured in `CMakeLists.txt` as cache variables:

- `MICRODB_MANAGED_STRESS_SMOKE_MAX_MS` (default: `5000`)
- `MICRODB_MANAGED_STRESS_LONG_MAX_MS` (default: `20000`)

Override example:

```powershell
cmake -S . -B build -DMICRODB_MANAGED_STRESS_SMOKE_MAX_MS=3000 -DMICRODB_MANAGED_STRESS_LONG_MAX_MS=12000
```

## CI Presets (Current)

Defined in `CMakePresets.json` and consumed by `.github/workflows/ci.yml`:

- `ci-debug-linux`:
  - `MICRODB_MANAGED_STRESS_SMOKE_MAX_MS=3000`
  - `MICRODB_MANAGED_STRESS_LONG_MAX_MS=12000`
- `ci-debug-windows`:
  - `MICRODB_MANAGED_STRESS_SMOKE_MAX_MS=5000`
  - `MICRODB_MANAGED_STRESS_LONG_MAX_MS=20000`

## Release Presets (Current)

Defined in `CMakePresets.json` and consumed by `.github/workflows/release.yml`:

- `release-linux`:
  - `MICRODB_MANAGED_STRESS_SMOKE_MAX_MS=4000`
  - `MICRODB_MANAGED_STRESS_LONG_MAX_MS=16000`
- `release-windows`:
  - `MICRODB_MANAGED_STRESS_SMOKE_MAX_MS=7000`
  - `MICRODB_MANAGED_STRESS_LONG_MAX_MS=25000`

## Reference Baseline (Windows, Debug, local)

- Date: 2026-04-16
- `test_backend_managed_stress_smoke`: ~70 ms
- `test_backend_managed_stress_long`: ~40 ms

Keep configured gates well above baseline jitter but low enough to catch serious
performance regressions.

## Periodic Refresh

Scheduled workflow:

- `.github/workflows/managed-baseline-refresh.yml`
- cadence: weekly (Monday 03:00 UTC) + manual trigger
- outputs per-platform JSON artifacts with measured smoke/long runtimes and
  configured budgets

Recommended update loop:

1. Review last 8-12 baseline artifacts per platform.
2. Set acceptance band to stay above p95 runtime + safety margin.
3. Update preset thresholds in `CMakePresets.json`.
