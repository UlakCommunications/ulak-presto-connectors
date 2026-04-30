# ulak-presto-connector-base — CLAUDE.md

Shared base library for the Trino plugin family in this repo
(`ulak-presto-influxdb-connector`, `ulak-presto-postgres-connector`,
`ulak-presto-quickwit-connector` all depend on it). Hosts shared utilities
+ the GeoIP UDFs.

## GeoIP UDFs

[`IPToCountry.java`](src/main/java/com/facebook/presto/ulak/geolocation/IPToCountry.java)
exposes three Trino SQL UDFs backed by MaxMind `com.maxmind.geoip2:geoip2:5.0.0`
(Apache-2.0):

- `ip_to_country(varchar) → varchar`
- `ip_to_latitude(varchar) → varchar`
- `ip_to_longitude(varchar) → varchar`

Reads MMDB from a configurable path (env vars
`ULAK_GEOIP_COUNTRY_DB` / `ULAK_GEOIP_CITY_DB`; defaults
`/usr/lib/trino/plugin/GeoLite2-Country.mmdb` and `.../GeoLite2-City.mmdb`).
If the file is missing or fails to open, `openReader` logs a `WARN` and
keeps the reader `null`; UDFs return `""` instead of crashing the
connector. Country / city / location lookups are wired through
`DatabaseReader.country()` and `.city()`; `IOException` and
`GeoIp2Exception` (incl. `AddressNotFoundException`,
`InvalidDatabaseException`) are caught at debug level → empty result.

Smoke-tested against three real MMDB files (MaxMind GeoLite2-Country,
GeoLite2-City, and the `convert.py`-produced IP2Location MMDB with
`database_type=IP2LITE-City`) — `geoip2:5.0.0` accepts all three; both
`country()` and `city()` calls succeed against the IP2Location-converted
file despite the non-`GeoIP2-*` header. See `CHANGELOG.md` "K04-followup".
End-to-end test (Trino + the connector jar + SQL UDF) still
recommended via the local `docker-compose.yml`.

See [`TODO.md`](TODO.md) category K for open items, plus category L
(architectural review 2026-04-28) covering test scaffold, multi-catalog
singleton refactor, resource leaks, and Maven hygiene.

## Known architectural debt (category L)

A two-prong audit on 2026-04-28 surfaced two high-impact issues:

1. **Multi-catalog registration is broken.** Six classes
   (`ConnectorBaseUtil`, `UlakQuickwitMetadata`,
   `QuickwitRecordSetProvider`, `QuickwitSplitManager`,
   `UlakRecordSetProvider`, `UlakSplitManager`) use a
   `private static <T> single` + `getInstance()` pattern that captures
   the first catalog's config and silently returns it to every
   subsequent catalog. Any second `quickwit_*` / `influxdb_*` /
   `postgres_*` catalog gets the wrong URL/index/pool. Fix tracked as
   **L04**.

2. **Zero test coverage.** No `src/test/` directory exists in any of
   the four modules; root `pom.xml` does not configure surefire/failsafe.
   Refactor without tests is risky, so the L plan does test scaffold
   (**L02**) + unit tests (**L03**) before the singleton rewrite
   (**L04**).

Plus a top-10 list of P1/P2 code-quality issues (resource leaks,
swallowed exceptions, credential logging, `System.out` in production)
tracked as **L05–L07**.

### Status snapshot (2026-04-30)

The L category is mostly landed on the `multi_catalog_refactor`
branch and deployed to `yucemonitoring` cluster as
`maya-nexus:35000/maya/trino:0.0.1-multi-catalog-refactor-l13`:

| L item | Status |
|---|---|
| L01 anomaly fixtures | ✓ done |
| L02 maven test scaffold | ✓ done |
| L03 pure-logic unit tests | ✓ done (101 → 128 tests) |
| L04a singleton refactor (5 SPI classes) | ✓ done — multi-catalog bug fixed |
| L04b ConnectorBaseUtil per-catalog state | deferred (gated on real customer need) |
| L05 resource leak hygiene | ✓ done |
| L06 logging + security hygiene | ✓ done |
| L07 maven hygiene tail | ✓ done |
| L08 Quickwit error mask removal + TrinoException + Redis NPE | ✓ done |
| L09 aggs column alias (`X/key` ↔ `/X/key`) | ✓ done |
| L10 TrinoException for transaction safety + null-body / template guards | ✓ done |
| L11 stable TVF schema (analyze == execute) | ❌ **REVERTED** — re-attempt in **L14** |
| L13 Rhino script swallow → ApiException rethrow | ✓ done |
| L14 deeper TVF schema fix (3-site bind) | open |
| L15 Rhino classloader debug under Trino plugin | open |

Live cluster smoke test passed: two catalogs on the same connector
(`mayapostgres` ×3, `quickwit` ×2 in dashboards) coexist cleanly,
which is the user-reported `iki tane ekleyemiyoruz` symptom fixed.

## GeoIP data policy — never commit

MaxMind GeoLite2 EULA and IP2Location LITE EULA both **forbid
redistribution**. No `.mmdb` / IP2Location LITE `.csv` / `.zip` is
committed to any git remote. `.gitignore` (repo root) enforces this:

```
geolocation/*
!geolocation/README.md
!geolocation/ip2location/
geolocation/ip2location/IP2LOCATION-LITE-DB11.CSV/
geolocation/ip2location/IP2LOCATION-LITE-DB11.CSV.zip
*.mmdb
*.MMDB
```

Customer mounts the MMDB at runtime — see
[`geolocation/README.md`](../geolocation/README.md) for the K8s
init-container + `emptyDir` pattern (recommended) and the PVC + CronJob
alternative.

## History rewrite event (2026-04-28)

The `master` branch was rewritten with `git filter-repo` to drop binary
geolocation blobs that had leaked into one stale commit (`bbde25e Merge
branch 'develop' into 'master'`). All four push-able remotes were
force-updated:

- `origin` → gitlab.ulakhaberlesme.com.tr/maya/presto-influxdb-connector
- `github` → github.com/mfyuce/presto-influxdb-connector
- `github_ulakcom` → github.com/UlakCommunications/ulak-presto-connectors
- `gitlab_mfyuce` → gitlab.com/mfyuce/presto-influxdb-connector

`develop` and the other 22 branches + 53 tags were already clean and
were not touched. **Anyone with an old clone of `master`** must run
`git fetch origin && git reset --hard origin/master` (or re-clone).
In-progress local work on master needs manual rebase.

Pre-rewrite backup bundle: `/tmp/ulak-presto-connectors-pre-purge-20260428-100923.bundle`
on the machine where the rewrite was performed (180 MB).
