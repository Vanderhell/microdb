# microdb Wiki

microdb is a compact C99 embedded database for microcontrollers and small edge runtimes.
It combines three engines behind one API:

- KV for config, cached state, and TTL-backed values
- Time-series for telemetry and rolling samples
- Relational for small fixed-schema indexed tables

## Highlights

- single allocation in `microdb_init()`
- fixed RAM budget with predictable behavior
- zero external dependencies
- RAM-only mode or persistent mode through a storage HAL
- WAL-backed recovery when persistence is enabled

## Start here

- [Getting Started](Getting-Started)
- [Architecture](Architecture)
- [Configuration](Configuration)
- [Storage HAL](Storage-HAL)
- [Testing](Testing)

## Repository

- Source: `https://github.com/Vanderhell/microdb`
- README: `https://github.com/Vanderhell/microdb#readme`
