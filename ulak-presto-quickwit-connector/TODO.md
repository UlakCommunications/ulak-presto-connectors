# TODO — ulak-presto-quickwit-connector

## Multi-tier history rollup routing — implemented, NOT deployed (2026-08-27)

Connector-side piece of the broader multi-granularity rollup effort (15m done; 30/45/60/90/... planned on the `qw-rollup-engine` side, not started). Range-based, not `resolution_in_seconds`/bucket-width-based — a dashboard's chosen bucket width says nothing about how much data the query has to scan.

- [x] **New `HistoryTier.java`** — `build(finestThresholdSeconds, additionalTiersCsv)` merges the classic single `history-time-threshold-seconds` value (implicit 15m tier, unchanged/backward-compatible) with an optional new `history-tiers` catalog property (CSV of `minutes:thresholdSeconds` pairs, e.g. `60:604800` for a 1h tier past 7 days). `select(tiers, rangeSeconds)` picks the coarsest tier the range clears. 8 unit tests in `HistoryTierTest`.
- [x] **`HistoryIndexResolver.java` simplified** — was aggsJson/`fixed_interval`-parsing (dropped, see below); now just `resolve(historyIndex, targetMinutes)`: swaps the one 2+-digit granularity token in the caller-supplied `history_index` name (`metrics3_15` → `metrics3_60`, `rollup_15m_site_app` → `rollup_60m_site_app`). 2+-digit requirement avoids colliding with the version digit in `metrics3`/`flows3`. Falls back to the original name whenever ambiguous. 9 unit tests.
- [x] **`QwUtil.historyIndexExists()`** — existence backstop via the already-used `IndexesApi.getIndexesMetadatas()`, 30s-TTL cache per `qwUrl` (avoids a metastore round trip on every history-eligible query). `QwUtil.select()`'s switch now: pick tier from range → derive candidate name → only switch if it actually exists in Quickwit, else fall back to the configured `historyIndex` unchanged. No live-cluster unit test (matches existing `hasTimestampField` precedent — needs `QuickwitIntegrationTest`-style live verification instead).
- [x] **Config threading** — new optional `history-tiers` catalog property, plumbed `UlakQuickwitConnectorFactory` → `UlakQuickwitConnector` → `UlakQuickwitMetadata` → `QwUtil.select()` (all 6 call sites) and `getColumnsInternal()` (2 call sites), telescoping-constructor style matching how `history-time-threshold-seconds` itself was added. Omitting the property changes nothing (empty tier list beyond the implicit 15m one) — verified via full module test suite, 184/184 pass, 0 regressions.
- [ ] **Rejected/abandoned approach** — first cut derived the target tier from the query's own `aggs` (`fixed_interval`/DSL `interval=`). Checked against the real 82-DSL fixture set (`fixtures/anomaly/aggs-dsl.json`): 65/74 histogram panels use a `resolution_in_seconds` dashboard variable (fine), but 8/74 use Grafana's pixel-based `$__interval_ms` which essentially never lands on a clean tier boundary — would silently never escalate for those panels. Range-based avoids this entirely. Code removed, not left behind as dead weight.
- [ ] **Not committed, not deployed.** No `qw-rollup-engine` task exists yet for any tier beyond 15m — `history-tiers` must stay unset (or point only at tiers that are actually backfilled) until that side is built; the existence check is a backstop, not a substitute for the admin-controlled `enable_history` gate.
- [ ] **Pre-deploy check** — audit live catalogs for any `enable_history=true` dashboard currently relying on the single-tier behavior with a query range that would newly qualify for an escalated tier once `history-tiers` is set; confirm none regress before turning a new tier on.

## QW10 — all-null row filter fix (2026-05-15)

- [x] **QW10** `parseResponseHits()`: `if (!allNulls)` guard was commented out → empty aggregation buckets produced 1 all-null row in Grafana instead of empty panel. Uncommented. 3 unit tests in `QwUtilParseTest` (SPI-free, Java-24). Commit `9c22330`, Jenkins #359. 68/68 dashboards 0 error confirmed.

## QW9 — COLUMN_NOT_FOUND fixes (2026-05-14)

- [x] **QW9-01** `traverseAggregations`: expose `/aggId/key` alongside `aggId/key` for sqlversion=0.1. Commit `d147c18`. Deployed Jenkins #355.
- [x] **QW9-02** `analyze()` + `parseResponse()`: fall back to declared `columns` param when `traverseAggregations` returns empty. Commit `b129765`. Deployed Jenkins #356. 0 errors confirmed.
- [x] **QW9-03** [P1] `getTableHandle()` base32 decode — Guava `BaseEncoding.base32()` in `PlainTableQuery.decodeIfBase32Encoded()`, called from `getTableHandle()`. 9 unit tests in `PlainTableModeTest`. Commit `2d712eb`, Jenkins #357. 68/68 dashboards 0 error confirmed.

## QW 0.8 compatibility — in progress (2026-05-13)

- [x] **QW8-06** `FastFieldOptions` schema registry — FIXED 2026-05-14, java-client `56224e6`, deployed Jenkins #350.

## Architectural review action plan (2026-04-22) — Trino/Presto connector scope

From the full cross-repo review in `backend/anomaly` → `TODO.md` "Architectural review & action plan" (91 items, A–R). Items below are Trino/Presto-Quickwit connector scope (category J). Master/full list: https://gitlab.ulakhaberlesme.com.tr/backend/anomaly/-/blob/master/TODO.md

- [ ] **J46 [P1, 0.5d] Connector health + Grafana datasource failover** — Trino down = every dashboard blank. Connector side: Redis cache (`//cache=true //ttl=N`) already serves stale results while Trino is restarting. Remaining work (K8s scope): (1) Prometheus `KubeDeploymentReplicasMismatch` alert on Trino pod; (2) Grafana datasource health-check panel; (3) optional fallback datasource for critical dashboards. Track in `backend/anomaly` infra TODO.
- [x] **J47 [P1, 1d] `sqlversion` deprecation plan** — Target: `0.2`. ADR written: `docs/adr/0003-sqlversion-deprecation.md`. WARN log added for `0` and `0.1` at parse time. Code removal deferred to future major release.
- [x] **J48 [P1, 0.3d] Auto-ID collision — fail loud, not silent** — `AggsDslCompiler.IdAllocator.claim()` now logs WARN with preferred id, assigned fallback id, and guidance when a collision occurs.
- [x] **J49 [P2, 0.5d] `CAPABILITIES.md`** — written at `CAPABILITIES.md`: agg types, auto-id rules, order= modifier, replacefromcolumns scope, sqlversion column naming table, all inline params, Quickwit 0.8 limitations.
- [x] **J50 [P2, 1d] Connector integration tests** — `QuickwitIntegrationTest.java`: 5 sentinels (S1 HTTP connectivity, S2 aggs v0.1 column names, S3 aggs v0.2 bare names, S4 raw hits, S5 SSRF guard). Skipped unless `QUICKWIT_TEST_URL` env var set. CI setup documented in test class Javadoc.
- [x] **J51 [P1, 0.5d] Injection audit** — Attack surfaces documented and tested in `InjectionAuditTest.java`. A: //qwurl= injection mitigated by R02 allowlist. B: JSON field injection is passthrough by design (dashboard authors must use Grafana ${var:text} escaping). C: index traversal tracked as J52. D: Rhino injection mitigated by R01.
- [x] **J52 [P2, 1d] Multi-tenant query isolation at Trino** — Audit complete: `docs/adr/0004-multi-tenant-isolation.md`. URL escape mitigated by R02. Remaining: Trino rules.json catalog ACL (infra scope). Optional: `qw-allowed-indexes` property (J57b).
- [x] **J53 [P2, 0.5d] Slow-query observability** — `ConnectorBaseUtil.select()` now logs `WARN SLOW_QUERY` with elapsed ms, name, index, row count when execution exceeds 5 s; DEBUG log for all queries. Loki/QW ingestion of Trino logs remains infra scope.
- [x] **J54 [P2, 0.3d] Quickwit version-compatibility matrix** — written at `docs/quickwit-compatibility.md`: history table, 0.8→0.9/1.0 upgrade risk list, upgrade procedure.
- [x] **J55 [doc, 0.3d] ADR `0003-sqlversion-deprecation.md`** — written at `docs/adr/0003-sqlversion-deprecation.md`.
- [x] **J56 [P2, 1d] Plain Table Query Mode** — Phases 1 + 2 complete. `FROM quickwit.public."<index-name>"` works as a first-class query surface. Detection: `PlainTableQuery.isPlainMode()` (no `//` in name). Schema: `QwUtil.getColumnsFromDocMapping()` reads `DocMapping.fieldMappings` from Quickwit `IndexesApi`; field types mapped to Trino types via `fieldTypeToTrino()`. Query: `PlainTableQuery.buildMatchAllQuery/buildFilteredQuery()` generates `{"query":"...","max_hits":N}` with `//qwindex=` prefix; injected in `QuickwitSplitManager.getSplits()`. Filter pushdown (`applyFilter()`): equality and single-range predicates on any column converted to Quickwit Lucene syntax. Limit pushdown (`applyLimit()`): updates `max_hits`; guaranteed. Multi-range and expression predicates passed back to Trino.

## Code review findings (2026-05-12) — QW connector scope

- [x] **R01 [Critical, security] Rhino ClassShutter missing** — `QwUtil.java:217-234`: `cx.initStandardObjects()` exposes full Java stdlib (Runtime, File, network). Fixed: `ClassShutter` (blocks all Java classes) + `initSafeStandardObjects()` + 100k instruction limit.
- [x] **R02 [Critical, security] SSRF via embedded `//qwurl=`** — Fixed: `UlakQuickwitMetadata.validateQwUrl()` enforces catalog allowlist. Default: only `qw-connection-url` is allowed. Set `qw-allowed-urls=*` to allow any URL (opt-in), or `qw-allowed-urls=url1,url2` for a whitelist.
- [x] **R03 [High] `defaultClients` map is not thread-safe** — `QwUtil.java:54,76-111`: plain `LinkedHashMap` with TOCTOU race under concurrent queries. Fixed: `ConcurrentHashMap` + `computeIfAbsent`.
- [x] **R04 [High] `//name=` header writes cache flag, not name** — `QuickwitRecordSetProvider.java:100`: `h.isCache().get()` should be `h.getName().get()`. Fixed.
- [x] **R05 [High] All three timeouts use `connectTimeout` in `analyze()`** — `RawQuery.java:150-152`: read and write timeouts silently use connect timeout value. Fixed.
- [x] **R06 [High] `Optional` compared with `==` in `equals()`** — `RawQuickwitQueryTableHandle.java:150`: `cache == other.cache` must use `.equals()`. Fixed.
- [x] **R07 [High] `getSchemas()`/`getTableNames()` NPE when called** — Fixed: null guard in logger + null client guard before IndexesApi instantiation.
- [x] **R08 [Medium] `buildSearchRequestJson` called twice in split manager** — `QuickwitSplitManager.java:47-48`: double call produces diverging `//from=`/`//to=` timestamps. Fixed.
- [x] **R09 [Medium] `noDataIndex` never increments → duplicate `no-data-0` columns** — `RawQuery.java:156-157`: lambda captures effectively-final int; Trino rejects duplicate column names. Fixed with `AtomicInteger`.
- [x] **R10 [Medium] `prefix.getTable().get()` without `isPresent()` guard** — Fixed: `!prefix.getTable().isPresent()` check added before `.get()`.
- [x] **R11 [Medium] `trimTimeEdges` deletes all rows when all timestamps equal** — `QwUtil.java:475-480`: `removeIf(v == fMax || v == fMin)` wipes everything when `maxTime == minTime`. Fixed.
- [x] **R12 [Medium] ClassCastException on `Integer` numeric in aggregation** — `QwUtil.java:509,513`: `(long)(double)obj` cast fails if Gson returns `Integer`. Fixed with instanceof chain.
- [x] **R13 [Medium] `c[j]` without length guard → AIOOBE on short rows** — `QwUtil.java:596-601`: missing fields in a document produce shorter rows than headers. Fixed.
- [x] **R14 [Medium] `"null"` string treated as absent for required args** — `RawQuery.java:183`: a valid `qwindex` literally named `"null"` is dropped silently. Fixed.
- [x] **R15 [Low] `QwUtil.main()` with hardcoded `10.20.4.53` internal IP** — `QwUtil.java:664,819`: production class with debug entry point exposes internal network topology in published JAR. Removed.
- [x] **R16 [Low] DSL parser does not handle `[`/`]` bracket nesting** — `AggsDslCompiler.java:378-404`: comma inside `[...]` splits incorrectly. Fixed.
- [x] **R17 [Low] `order=id:X:dir` split unbounded** — `AggsDslCompiler.java:305`: `split(":")` should be `split(":", 3)`. Fixed.
- [x] **R18 [Low] Inconsistent `@JsonProperty` on Optional getters** — `RawQuickwitQueryTableHandle.java:186-202`: all Optional getters now have `@JsonProperty`.
- [x] **R19 [Low] Logger fields not `static final`** — Multiple QW connector classes. Fixed.

## Notes — Aggs DSL reference
- Form: `[histogram(...), terms(...), max(...), sum(...), avg(...), min(...), count(...)]`. Compiled by `AggsDslCompiler.java` to Quickwit aggs JSON.
- **Auto-ID**: histogram → `"date"`; terms/metrics → field name (last segment after last dot). Numeric fallback on collision — see J48.
- **Column paths**: `<id>/key`, `<id>/key_as_string`, `<id>/value`.
- **`sqlversion` modes** (see CLAUDE.md of anomaly repo for full table): `"0"` legacy flat + `replacefromcolumns`; `"0.1"` recursive traversal (`<id>/value`); `"0.2"` stripped suffixes (`<id>` bare).
- **`top_hits` not supported** in Quickwit 0.8 — removed from gateway/CPE dashboards.
