# Release Log

This file tracks release-level outcomes and notable delivery notes.
For detailed code-level change history, see [CHANGELOG.md](CHANGELOG.md).

## Unreleased

- No pending release notes.

## v1.2.0 - 2026-04-20

### Added

- Optional wrappers and backend adapter modules documented in `README.md`.
- GitHub wiki synchronized in English and aligned with `docs/`.
- Automated GitHub release publishing on tag push (`v*`) via `.github/workflows/release.yml`.

### Fixed

- Multiple sanitizer and leak issues across WAL, TS, REL alignment and test reopen paths.
- CI stability improvements for ASAN/UBSAN validation lane.

### Process

- Strengthened repository hygiene with contribution/security/community templates.

## Release Process

1. Ensure `CHANGELOG.md` has finalized entries for the target version.
2. Tag the release with `vX.Y.Z`.
3. Push the tag (`git push origin vX.Y.Z`).
4. GitHub Actions `release.yml` builds artifacts and publishes GitHub Release.
5. Append final entry to this file with date and highlights.

