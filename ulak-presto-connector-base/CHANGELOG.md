# Changelog — ulak-presto-connector-base

Items move here from [`TODO.md`](TODO.md) when finished. Keep entries
concrete enough that a future reader can locate the change without
chasing commits.

## 2026-04-28 — GeoIP cleanup

After the repo-wide history rewrite (`git filter-repo` over all 28
branches + 53 tags) that purged license-restricted binary geolocation
data, the connector-base GeoIP layer was made functional and safe to
ship without baked-in `.mmdb` files. See
[`CLAUDE.md`](CLAUDE.md#history-rewrite-event-2026-04-28) for the
history-rewrite event details.

### Done

- **K01 — Dockerfile: drop binary GeoIP `COPY` lines.** The image is
  now data-less; the customer mounts MMDB at runtime per
  [`geolocation/README.md`](../geolocation/README.md).
- **K02 — `IPToCountry.java`: configurable MMDB path.** New env vars
  `ULAK_GEOIP_COUNTRY_DB` and `ULAK_GEOIP_CITY_DB`; defaults still
  `/usr/lib/trino/plugin/GeoLite2-{Country,City}.mmdb` so existing
  deployments keep working without config change.
- **K03 — `IPToCountry.java`: graceful fallback.** `openReader` logs
  a `WARN` and returns `null` when the file is missing or fails to
  open; lookups short-circuit on `null` reader; UDFs return `""`. The
  connector loads cleanly without MMDB instead of throwing
  `RuntimeException` at class-load.
- **K04 — `IPToCountry.java`: real lookup logic re-enabled.** All UDFs
  (`ip_to_country`, `ip_to_latitude`, `ip_to_longitude`) now hit
  `DatabaseReader.country()` / `.city()` and surface real values.
  `IOException` and `GeoIp2Exception` (incl. `AddressNotFoundException`,
  `InvalidDatabaseException`) caught at debug level → empty result, no
  crash. **Runtime test against real MMDB still outstanding** — tracked
  inline on the K04 row in TODO.md as a follow-up note.

- **K07 — `IPToCountry.java`: off `geoip2:5.x` deprecated APIs.**
  Switched from `getCountry()` / `getName()` / `getLocation()` /
  `getLatitude()` / `getLongitude()` to the record-style accessors
  `country()`, `name()`, `location()`, `latitude()`, `longitude()`.
  Compile is now warning-free (`@Deprecated(forRemoval=true)` notices
  gone), unblocks future bump to geoip2 6.x.
- **K04-followup — smoke test + Trino E2E against real MMDB.**
  Standalone Java test (`/tmp/GeoTest.java`, not committed) loaded each
  MMDB and queried `8.8.8.8` / `1.1.1.1` / `212.156.4.5`. Then a full
  Trino E2E via `docker-compose.yml` (uncommenting the GeoIP mount +
  env-var lines) confirmed the UDFs over SQL:
  `SELECT ip_to_country('8.8.8.8')` → `"United States"`, lat 37.751,
  lon -97.822. `ip_to_country('212.156.4.5')` → `"Türkiye"`, lat
  41.0145, lon 28.9533. `ip_to_country('not-an-ip')` → `""`
  (graceful). Standalone results:
  - `GeoLite2-Country.mmdb` (database_type `GeoLite2-Country`):
    ✓ correctly returns "United States" / "Türkiye" (1.1.1.1 unmapped).
  - `GeoLite2-City.mmdb` (database_type `GeoLite2-City`):
    ✓ correct country + lat/lon for the same IPs.
  - `IP2LOCATION-LITE-DB11.CSV.MMDB` (database_type `IP2LITE-City`,
    produced by `geolocation/ip2location/convert.py`):
    ✓ **accepted** by `geoip2:5.0.0` despite the non-`GeoIP2-*`
    `database_type` header — both `country()` and `city()` calls
    succeed. Country names follow IP2Location's spelling
    ("United States of America", "Turkiye"). 1.1.1.1 (Cloudflare)
    actually resolves to Australia here, where MaxMind's free LITE
    returned null.
  Conclusion: the K04 risk note about `IP2LITE-*` rejection was
  unfounded; **`convert.py` does not need a header fix**.

- **K06 — Dependabot triage on UlakCommunications GitHub mirror.**
  Pulled the alert list via `gh api`: 11 total alerts, 7 already
  fixed historically. The 4 still-open alerts are all **the same
  CVE-2025-66453 (`GHSA-3w8q-xq97-5j7x`, severity LOW)** —
  Rhino DoS via `toFixed()` on attacker-controlled floats — flagged
  once per `pom.xml` (base + 3 connectors). Bumped:
  - `org.mozilla:rhino` 1.8.0 → 1.8.1
  - `org.mozilla:rhino-engine` 1.8.0 → 1.8.1
  - `org.mozilla:rhino-runtime` 1.7.15 → 1.7.15.1 *(no 1.8.x line
    exists for this artifact on Maven Central; 1.7.15.1 is the
    upstream patch)*
  Build still passes (`mvn compile` over base + 3 connectors).
  Dependabot will close the 4 alerts automatically when the new
  versions land in the default branch.

## 2026-04-28 — Architectural review (category L)

In-progress. Branch `multi_catalog_refactor`. Each L-item lands as a
separate commit on the branch; the whole branch lands in `develop` as
one merge once the docker-compose smoke test passes.

### Done

- **L01 — Test fixtures harvested from `backend/anomaly` Grafana
  dashboards.** 101 distinct `quickwit.system.raw_query(...)` calls
  pulled from `grafana_dashboard*.json` + `grafana_alerts_watchdog.json`,
  reduced to 82 distinct Aggs DSL strings + their full parameter set
  (`qwindex`, `sqlversion`, `columns`, `replacefromcolumns`, `hasjs`,
  `aggs`, ...). Three fixture files committed under
  `ulak-presto-quickwit-connector/src/test/resources/fixtures/anomaly/`:
  - `aggs-dsl.json` — the 82 distinct DSL strings.
  - `raw-query-params.json` — 101 named-parameter records.
  - `raw-queries.sql.json` — the 101 full SQL queries (for parsing tests).
  Index covers all anomaly subsystems (`anomaly-events`,
  `-events-cpe`, `-events-gateway`, `-events-metrics`,
  `-events-collective`, `-flows`, `-model-stats`) and both
  `sqlversion=0.1` and `0.2`.

- **L02 — Maven test scaffold.** Root `pom.xml` now carries a
  `<dependencyManagement>` block for the test stack (JUnit Jupiter
  5.11.4 BOM, AssertJ 3.27.0, Mockito 4.11.0) plus a surefire 3.5.2
  `<pluginManagement>` entry that overrides `JAVA_TOOL_OPTIONS` so the
  docker-compose JDWP setting does not collide with the forked test
  JVM. The four module poms declare the same four test dependencies
  with explicit versions (parent migration is deferred to L07). Each
  module pom now has its own surefire plugin block. `SmokeTest` in
  base + quickwit modules confirms wiring (`mvn test` over both is
  green: 3 tests pass).

- **L03 — Unit tests for pure-logic classes.** Two modules covered:
  - `ulak-presto-connector-base`: `IPToCountryTest` (8 tests) locks in
    the K03 graceful-fallback contract for `ip_to_country`,
    `ip_to_latitude`, `ip_to_longitude` — null/blank inputs and missing
    MMDB return empty, never throw, never null.
  - `ulak-presto-quickwit-connector`: `AggsDslCompilerTest` —
    7 hand-written cases (null / empty / JSON-passthrough /
    array-passthrough / garbage-rejection / simple histogram /
    two-histograms-rejected) plus an 82-fixture parameterised
    round-trip over the production DSL strings harvested in L01. Every
    fixture compiles to syntactically-valid JSON.
  - The 12 fixtures with Grafana template tokens
    (`size=${top}`, `size=${top:csv}`, `interval=${__interval_ms}ms`,
    `interval=${resolution_in_seconds}s`) used to fail the round-trip
    with `Invalid integer: ${top:csv}`. Diagnosis: production never
    sees raw `${...}` because Grafana substitutes them before sending
    SQL to Trino; the compiler is correct to reject them. Fix: the
    parameterised test now pre-renders templates via
    `renderTemplates(dsl)` (regex `\$\{[^}]+\}` → `"10"`) so the
    round-trip exercises the same shape Trino sees at runtime.
  - Module totals: connector-base 9 tests / 0 fail, quickwit 92 tests /
    0 fail. **L03 is green; L04 unblocked.**

### Pending follow-up (still in TODO)

- **K05** — license attribution audit on Grafana panels in
  `backend/anomaly`.
- **K06** — dependabot vulnerability triage on
  `UlakCommunications/ulak-presto-connectors` GitHub mirror.
