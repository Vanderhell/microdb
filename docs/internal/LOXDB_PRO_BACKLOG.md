# LOXDB PRO Backlog (Out of Core Scope)

Date opened: 2026-05-02

This file tracks items that were originally described in `TODO.md` but are intentionally out of scope for the open-source
`loxdb` core repository, per `ROOT_SPEC_CORE_VS_PRO.md`.

## DB Image Management on Media (PRO)

Goal: full DB image management API and tooling for media containing multiple LOX DB files/images.

- discovery:
  - scan selected directory/tree for candidate DB files
  - classify each file as `valid loxdb`, `corrupt loxdb`, or `non-loxdb`
  - detect mixed-version artifacts and report format/version fields
- identity and fingerprint:
  - read/validate magic headers (WAL, superblock, page magics)
  - expose DB fingerprint/id (stable UUID-like identifier or deterministic hash)
  - detect duplicate/copy images (same fingerprint, different path/name)
- metadata:
  - file path, size, timestamps (created/modified/accessed)
  - engine occupancy and pressure snapshot (kv/ts/rel/wal usage)
  - table/stream counts and names (optional extended inspection mode)
  - recovery state flags (clean, replay-needed, inconsistent)
- lifecycle operations:
  - create new DB image with explicit profile/template (size/split/wal policy)
  - open by id/path, set active DB, close active DB
  - rename/move DB image
  - clone/copy DB image
  - delete one DB image
  - bulk delete by filter (age/pattern/state) with dry-run preview
- capacity and allocation view:
  - report medium total/free/used bytes
  - report cumulative size of all detected LOX images
  - report per-image allocated vs logically used space
  - optional compaction recommendation per image based on pressure metrics
- index/catalog:
  - optional media catalog file (manifest) for fast startup listing
  - lazy reconcile between manifest and real FS scan
  - auto-heal manifest when drift is detected
- filtering and query:
  - filter images by name, tag, profile, age, size, health state
  - sort by recent use, largest first, highest pressure, most stale
  - paging support for large media inventories
- bench + UX integration:
  - serial commands: `db list`, `db use <id>`, `db create`, `db rm`, `db info`
  - optional LCD page showing active DB id/name + free space + image count
  - startup policy switch: `fresh`, `reuse-last`, `reuse-by-name`, `auto-pick-healthy`
- diagnostics:
  - structured error codes for scan/open/delete/manage operations
  - verbose trace mode for field debugging on HW
  - integrity check command (`db verify`) with summarized findings

