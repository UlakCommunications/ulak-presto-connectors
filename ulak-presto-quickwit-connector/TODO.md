# TODO — ulak-presto-quickwit-connector

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
- [ ] **J56 [P2, 1d] Plain Table Query Mode** — implement `FROM quickwit.public."<index-name>"` as a first-class query surface (no embedded `//param` required). Phase 1: schema inference from `DocMapping.fieldMappings` via `IndexesApi.getIndexesMetadatas()`; detect plain mode by absence of `//` in table name; return raw hits via existing `parseResponseHits()`. Phase 2: `applyFilter()` pushdown — convert Trino `TupleDomain<ColumnHandle>` to Quickwit query string (`field:value`, `field:IN [v1 v2]`, `field:[min TO max]`); `applyLimit()` → `max_hits`. Files: `UlakQuickwitMetadata`, `QwUtil`, `QuickwitSplitManager`, `QuickwitRecordSetProvider`. Not a `sqlversion` variant — separate code path gated on plain table name detection.

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
