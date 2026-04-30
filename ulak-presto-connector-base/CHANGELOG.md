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

- **L04a — Singleton refactor (Phase 1, multi-catalog fix).** The five
  classes that used the `private static <T> single` +
  `public static getInstance(...)` pattern lose it: each Connector now
  constructs its own per-catalog instance directly via `new`.
  - `UlakQuickwitMetadata`: `single` field gone; constructor public;
    the previously-static `connectorId` becomes a `private final` field
    populated from the catalog name. `RawQuery.RawQueryFunction` now
    reads `metadata.getConnectorId()` instead of the static reference.
  - `QuickwitRecordSetProvider`, `QuickwitSplitManager`,
    `UlakRecordSetProvider`, `UlakSplitManager`: same drop, constructors
    now public.
  - `UlakQuickwitConnector`, `InfluxdbConnector`, `UlakPostgresConnector`
    each switch their `getInstance(...)` calls to `new ...()`. Two
    `quickwit_a` + `quickwit_b` (or `pg_a` + `pg_b`) catalogs no longer
    alias the first registrant's URL/index/timeouts — the user-reported
    "iki tane catalog ekleyemiyoruz" symptom is gone for the typical
    deployment shape.
  - Tests added:
    - `L04StructuralTest` (connector-base, 6 cases) — reads each of the
      five source files as text, asserts the `static <T> single`
      regex and `public static getInstance(` regex no longer match.
      Also asserts the quickwit connector wires via `new ...()`. Cheap
      regression guard that runs on the local JDK.
    - `UlakQuickwitMetadataMultiCatalogTest` + `UlakBaseMultiCatalogTest`
      lock in the behaviour (two distinct instances keep distinct
      `qwUrl`, `qwIndex`, `connectorId`, `defaultParams`). **Disabled
      until CI runs JDK 25**: Trino SPI 479 is class-file v69 and the
      local GraalVM 24 build cannot load `ConnectorSplitManager` /
      `ConnectorRecordSetProvider` / `ConnectorMetadata`. Re-enable on
      JDK 25 — assertions are unconditional. Until then,
      `L04StructuralTest` + `mvn compile` clean is the regression
      surface.
  - Module totals after L04a: connector-base 16 tests / 0 fail / 2
    skipped, quickwit 95 tests / 0 fail / 3 skipped.
  - **L04b — `ConnectorBaseUtil` per-catalog state — DEFERRED.** The
    shared static state (`isCoordinator`, `workerId`,
    `workerIndexToRunIn`, `keywords`, `redisUrl`, `JedisPool`,
    `objectMapper`) is set identically by all three connectors at
    startup, so two catalogs sharing the same Redis (the typical
    deployment) work correctly today. Per-catalog Redis pools would
    be needed only if two catalogs in the same Trino node had to
    target different Redis instances — left as a follow-up TODO row
    (L04b) gated on a real customer requirement.

- **L11 — Stable TVF schema (analyze == execute) + sqlversion-aware column alias.**
  Two follow-ups after the live cluster watch.
  - **Returned-table-mismatch fix**: queries like
    `SELECT * FROM TABLE(quickwit.system.raw_query("columns" => 'h,m,...'))`
    failed with `returned table does not match the node's output` —
    Trino's `RewriteTableFunctionToTableScan` asserts that the schema
    declared at analyze() exactly matches what comes back from execute().
    `RawQueryFunction.analyze()` was running the Quickwit round-trip a
    second time (via `getColumnsInternal`) which can produce a different
    column set than `getRecordSet()` (extra alias keys from L09, a row
    appearing between calls, etc.). When the caller already declared a
    `columns =>` list in SQL, trust that list and skip the Quickwit
    call — analyze and execute then line up.
  - **Defensive 4-way alias re-introduced (L09b), but gated on
    `sqlversion != "0"`**: User confirmed v=0.1/0.2 dashboards already
    emit the buckets-stripped form, while v=0 dashboards still rely on
    the verbose `X/buckets/...` shape. The original
    stripped + leading-slash aliases stay unconditional (they only
    add aliases, never remove). The buckets-stripped variants are
    added only when sqlversion is post-0, so v=0 dashboards keep
    their existing column names.

- **L10 — TrinoException for transaction safety + null-body / unsubstituted-template guards.**
  Surfaced during a 5-minute live cluster watch. Three new failures
  appeared on top of L08:
  - **`IllegalStateException: Current transaction already committed`** —
    very high frequency. Cause: L08's `throw new RuntimeException(e)`
    inside `UlakQuickwitMetadata.{getTableMetadata,getColumnHandles,
    listTableColumns}` corrupts Trino's per-query transaction state.
    Trino expects `TrinoException` for connector-side metadata failures;
    a generic `RuntimeException` leaves the transaction in an
    inconsistent committed state, causing every subsequent metadata
    call on that thread to fail with `IllegalStateException`. Switched
    to `throw new TrinoException(GENERIC_INTERNAL_ERROR, e)`.
  - **`Quickwit 400: EOF while parsing a value at line 1 column 0`** —
    `QwUtil.executeOneQuery` had one remaining swallow site: Gson parse
    failure on the inner query JSON would leave `toQuery=null`, then
    `searchPostHandlerCall(...)` POSTed an empty body. Now the parse
    failure rethrows as `ApiException` with the underlying Gson message,
    and a separate null-guard refuses to send empty POSTs.
  - **`Date histogram parse error: NumberMissing("h")`** —
    unsubstituted Grafana template like `${retention_period_in_hours}h`
    survived to Quickwit, which choked on the trailing `h`. Defensive
    guard added: if the query body still contains `${...}` after Trino
    has handed it off, refuse the call with a clear message naming the
    token, instead of letting Quickwit produce an opaque tantivy error.

- **L09 — Aggs row column-name compatibility (`/6/key` vs `6/key`).**
  Surfaced after L08 expose-the-real-error landed: the throughput
  dashboard's `interface` template variable kept failing with
  `Column '/6/key' cannot be resolved`. Probe test on JFlat showed the
  parser produces 3 rows / 3 columns from the response, but the column
  names came out as `6/buckets/key` etc. and the dashboard SQL referred
  to `"/6/key"` (with leading slash). The historical `"1/5/key"` form
  in SLA dashboards (no leading slash) worked, so the fix is a
  backward-compat alias in `QwUtil.parseResponseHits`: every column is
  now stored under both `X/key` and `/X/key`. Existing `select
  "1/5/key"` calls keep working; new `select "/6/key"` calls also
  resolve. Tests still 127 / 0 / 5 skipped.

- **L08 — Quickwit error handling + getTableMetadata swallow + RedisCacheWorker NPE.**
  Surfaced after the live-cluster smoke test, where dashboard panels were
  receiving a Grafana `400 Bad Request` whose underlying cause
  (`Quickwit: query requires a default search field and none was supplied`)
  was being masked by three separate bugs in the connector:
  - `QwUtil.executeOneQuery` deserialised the raw HTTP body straight into
    `SearchResponseRest` regardless of HTTP status. Quickwit returns
    `{"message":"..."}` on errors; that JSON does not match
    `SearchResponseRest`'s strict `validateJsonObject`, so the parse
    threw `IllegalArgumentException: field "message" not defined` and
    the real Quickwit error was lost. Now the body is checked against
    `Response.isSuccessful()` first; on non-2xx (or on a parse error)
    the helper `extractQwErrorMessage(body)` extracts `message` and the
    method throws `ApiException("Quickwit <code>: <message>")` with the
    real cause.
  - `UlakQuickwitMetadata.{getTableMetadata, getColumnHandles, listTableColumns}`
    used to swallow generic `Exception` (logged but ignored) and let
    callers see a `null` `List<ColumnMetadata>`. Trino SPI then threw
    `INTERNAL_ERROR: columns is null`, surfaced to the user as 400.
    Now those handlers re-throw `RuntimeException(e)` so the underlying
    cause propagates.
  - `RedisCacheWorker.run:146` called `ObjectMapper.readValue(json, ...)`
    without checking that `json` was non-null. When a Redis key TTL
    expired between the scan and the get, `jedis.get` returned `null`
    and Jackson threw `IllegalArgumentException: argument "content" is
    null` once per scan loop. Added an explicit `if (json == null) continue;`.
  - Plus `QwUtil.replaceTrinoQWVars` now also rewrites `:IN []` and
    `:IN [ ]` to `:*` (the `[*]` and `[-]` variants were already
    handled). Closes the case where Grafana's `${sites:pipe}` substitutes
    to an empty list.
  - Tests still 126 / 0 / 5 skipped; clean compile.

- **L-category smoke test (docker-compose).** End-to-end verification of
  the L04a fix on `trinodb/trino:479`. Setup: a temporary
  `quickwit_b.properties` catalog file alongside the existing
  `quickwit.properties` — both with `connector.name=quickwit`, distinct
  catalog names, distinct `qw-index`. Brought up with `docker compose
  up -d trino` (host port remapped to 18080 via a local-only override
  because 8080 was busy). Results:
  - `Plugin quicwitconnector` loads, `UlakQuickwitPlugin` installs once.
  - **Both catalogs register cleanly:** `Loading catalog quickwit_b` →
    `Added catalog quickwit_b using connector quickwit`; same for
    `quickwit`. Before L04a the second registrant would alias the first
    via `static single`; after L04a both succeed.
  - `SHOW CATALOGS` returns `quickwit`, `quickwit_b`, `system`, `tenant`.
  - `SHOW SCHEMAS FROM quickwit` and `SHOW SCHEMAS FROM quickwit_b`
    each return `default_schema` + `information_schema` independently.
  - `SELECT ip_to_country('8.8.8.8')` returns `""` (the K03 graceful
    fallback — no MMDB is mounted in this run; no crash, no exception).
  - No errors in `docker logs trino`.
  Smoke-test artifacts (the temporary `quickwit_b.properties` and the
  `docker-compose.override.yml` port remap) were deleted after the run;
  the gate the user asked for (`lokal compose docker test sonra
  pushlarız`) is met. The branch is now ready to merge into `develop`
  and force-push to all four remotes.

- **L07 — Maven hygiene tail.** Three concerns:
  - **Dead `<parent>` blocks dropped** from all four module poms — each
    carried a stale `<!-- <parent>...presto-maya-*-base...0.432-SNAPSHOT
    </parent> -->` block plus two parallel `<!-- <packaging>... </packaging> -->`
    lines surrounding the real `<packaging>jar</packaging>`. Eight lines
    of dead XML × 4 modules removed; the active `<packaging>jar</packaging>`
    is preserved.
  - **`commons-dbcp` 1.4 → `commons-dbcp2` 2.13.0** in the postgres
    connector. The 1.x line is end-of-life since 2014 (last release
    `1.4`, no security patches). DBCP 2.x is API-compatible for the
    handful of methods `PGUtil` uses (`setUrl`, `setUsername`,
    `setPassword`, `setMinIdle`, `setMaxIdle`,
    `setMaxOpenPreparedStatements`); only the import line changed
    (`org.apache.commons.dbcp.BasicDataSource` →
    `org.apache.commons.dbcp2.BasicDataSource`). Compile clean.
  - **SNAPSHOT deps documented.** `quickwit-java-client:0.0.1.36-SNAPSHOT`
    and `json2flat-maya:1.0.3-SNAPSHOT` are internal Maya forks resolved
    from the private Nexus at `192.168.57.202:8081`
    (`maya-maven-snapshot`); they are not on Maven Central and cannot be
    pinned to a release version until upstream publishes one. Each
    `<dependency>` declaration now carries an XML comment pointing at
    the Nexus and the canonical declaration in
    `ulak-presto-quickwit-connector/pom.xml` (Quickwit) or
    `ulak-presto-connector-base/pom.xml` (json2flat).
  - Tests still 126 / 0 / 5 skipped; clean compile across all four
    modules.

- **L06 — Logging + security hygiene.** Three concerns addressed:
  - **System.out in production.** Deleted the entire
    `AggsDslCompiler.main(String[])` demo method that ended in two
    `System.out.println(normalizeAggs(...))` calls. The same DSL shapes
    are now exercised by `AggsDslCompilerTest`'s 82-fixture set, so the
    demo's coverage is preserved without polluting stdout under Trino.
  - **Credential redaction in `QueryParameters`.** `replaceEnv` used
    to log `resEnv` (the env-var value) at INFO — a plaintext
    `RO_POSTGRES_PASSWORD` ended up in `trino-server.log`. Now the only
    log is a DEBUG line stating `present={true|false}`; the value
    never leaves memory. `encodeUriComponent` lost its `logger.info(s)`
    line for the same reason. Added a small `redactIfSecret(name, value)`
    helper (regex `(?i).*(pass|pwd|secret|token|key|credential).*`)
    and wired it into the `getQueryParameters` exception logger so a
    failing parse for a secret-named parameter doesn't leak the value.
  - **Swallowed exceptions.** Audited every `catch (Exception e)` in
    production code (16 sites). 13 already passed `e` to the logger;
    fixed the four genuine swallows:
    - `QueryParameters:247` — `logger.error("getQueryParameters: {} / {}", param, value)` was missing `e`; now passes `e` and redacts `value` if `param` is secret-shaped.
    - `QwUtil:233` — used `e.getMessage()` (loses stack trace); now passes `e`.
    - `QwUtil:257` — dropped `e` entirely; now passes `e`.
    - `RawQuery:201` — was `// fall through to default` with zero
      logging on a hasjs script failure; now logs at DEBUG with the raw
      value and `e`.
  - Tests added: `QueryParametersRedactionTest` (14 cases) covers the
    redaction helper across secret-shaped names, non-secret names, and
    null/empty values.
  - Module totals after L06: connector-base 30 / 0 / 2, quickwit 95 / 0 / 3.

- **L05 — Resource leak fixes.** Three sites tightened:
  - `ConnectorBaseUtil.select()` — replaced manual
    `pool.getResource()` + `finally { jedis.close(); }` with
    try-with-resources (`try (Jedis jedis = pool != null ?
    pool.getResource() : null)`); the null-jedis branch is implicit in
    try-with-resources and the body collapses by ~10 LOC. Also dropped a
    dead "eager caching" comment block and an unused `containsKey` guard
    around `inProgressLocks.remove(hash)`.
  - `ConnectorBaseUtil.invalidateCache()` — same try-with-resources
    refactor; early-return when pool is null; fixed `remove(hash, hash)`
    (two-arg form silently no-ops because the value isn't `hash`) to
    `remove(hash)`.
  - `ConnectorBaseUtil` JVM shutdown hook — closes `jedisPool` on JVM
    exit so idle pool connections don't leak.
  - `InfluxdbUtil.influxDBClients` — was a non-thread-safe
    `LinkedHashMap` populated by check-then-put. Switched to
    `ConcurrentHashMap` + `computeIfAbsent` so a race between two
    catalog-init threads can't double-create clients. Added a JVM
    shutdown hook that closes every cached `InfluxDBClient` and clears
    the map.
  - `UlakRecordCursor.close()` — was `// Empty method`. Now nulls the
    `row` reference for early GC and carries an honest comment that
    rows are pre-materialised by `ConnectorBaseUtil.select()` so there
    is no real connection / stream to release.
  - Test surface unchanged (110 / 0 / 5 skipped); compile clean across
    all four modules.

### Pending follow-up (still in TODO)

- **K05** — license attribution audit on Grafana panels in
  `backend/anomaly`.
- **K06** — dependabot vulnerability triage on
  `UlakCommunications/ulak-presto-connectors` GitHub mirror.
