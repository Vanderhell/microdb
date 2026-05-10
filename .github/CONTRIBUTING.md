# Contributing to loxdb

Thank you for contributing to `loxdb`.

## Ground Rules

- Be respectful and follow the [Code of Conduct](CODE_OF_CONDUCT.md).
- Keep changes focused and minimal.
- Include tests for behavioral changes.
- Keep public API, docs, and tests aligned.
- Use English for all commit messages, PR text, and documentation.

## Development Setup

1. Clone the repository.
2. Configure and build:
   - `cmake --preset ci-debug-linux` (Linux/macOS)
   - `cmake --preset ci-debug-windows` (Windows)
3. Run tests:
   - `ctest --preset ci-debug-linux --output-on-failure`
   - `ctest --preset ci-debug-windows --output-on-failure`

## CI and Local Validation

Before opening a PR, run at least:

- `./tools/check_spdx_headers.ps1`
- `ctest --preset ci-debug-linux --output-on-failure`

If your change touches persistence, recovery, memory safety, or engine internals, also run sanitizer lane:

- `cmake --preset ci-asan-ubsan-linux`
- `ctest --preset ci-asan-ubsan-linux --output-on-failure`

## Coding Style

- Language: C99 (and optional C++ wrapper).
- Keep one-responsibility functions where possible.
- Prefer explicit error handling and fail-fast checks.
- Avoid hidden allocations and preserve deterministic behavior.

## Tests

- Add or update tests in `tests/` for new behavior.
- Keep unit tests deterministic and fast.
- For bug fixes, include a regression test when possible.

## Documentation

Update relevant docs when behavior or public APIs change:

- `README.md`
- `docs/PROGRAMMER_MANUAL.md`
- `docs/PROFILE_GUARANTEES.md`
- `docs/FAIL_CODE_CONTRACT.md`
- `CHANGELOG.md`
- `RELEASE_LOG.md` (for release-facing changes)

## Pull Request Checklist

- [ ] Build succeeds on your target platform.
- [ ] Tests pass locally.
- [ ] SPDX headers are present in modified source/script files.
- [ ] Public contract changes are documented.
- [ ] Changelog entry added under `Unreleased` when applicable.

## Commit Guidance

- Use clear commit messages in imperative form.
- Keep unrelated changes in separate commits.
- Do not mix refactors with behavioral fixes unless necessary.

## Release-Impacting Changes

If your change modifies public APIs, error codes, formats, or behavior contracts, include:

- Changelog update (`CHANGELOG.md`)
- Release log update (`RELEASE_LOG.md`)
- Any required migration notes in docs

