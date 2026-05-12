# TODO — ulak-presto-quickwit-connector

## Architectural review action plan (2026-04-22) — Trino/Presto connector scope

From the full cross-repo review in `backend/anomaly` → `TODO.md` "Architectural review & action plan" (91 items, A–R). Items below are Trino/Presto-Quickwit connector scope (category J). Master/full list: https://gitlab.ulakhaberlesme.com.tr/backend/anomaly/-/blob/master/TODO.md

- [ ] **J46 [P1, 0.5d] Connector health + Grafana datasource failover** — Trino down = every dashboard blank. Start: Prometheus readiness alert on Trino pod; Grafana proxy or fallback datasource; consider query-level cache for cold panels.
- [ ] **J47 [P1, 1d] `sqlversion` deprecation plan** — 3 parallel compile paths (`0` legacy, `0.1`, `0.2`) is maintainer debt. Pick target (likely `0.2` — bare column names), deprecate others, write Grafana dashboard migration guide. Write ADR (J55).
- [ ] **J48 [P1, 0.3d] Auto-ID collision — fail loud, not silent** — "name taken → numeric id fallback" silently breaks SQL column refs when data adds a new field. Start: connector logs WARN on collision; build-time duplicate-auto-id check in `AggsDslCompiler`.
- [ ] **J49 [P2, 0.5d] `CAPABILITIES.md`** — document supported agg/modifier combinations vs Quickwit 0.8 limitations (no `top_hits`; `id=` rules; `order=id:X:dir` coupling with auto-id; `replacefromcolumns` only in sqlversion `0`). Saves panel authors trial-and-error.
- [ ] **J50 [P2, 1d] Connector integration tests** — 3–5 sentinel queries (histogram+terms+max, top-N, JOIN pattern) run in CI against a test Quickwit. Catches silent regressions on Trino or Quickwit upgrade.
- [ ] **J51 [P1, 0.5d] Injection audit** — `raw_query(json_string)` takes arbitrary JSON; Grafana dashboard variables flow into it. Document escape discipline; test crafted payloads for SQL/JSON injection via dashboard variable.
- [ ] **J52 [P2, 1d] Multi-tenant query isolation at Trino** — Trino is single instance; verify "tenant = cluster" boundary is enforced at the query layer (can tenant A's Grafana reach tenant B's Quickwit via Trino catalog?). If not: per-tenant Trino instances OR catalog-level ACL.
- [ ] **J53 [P2, 0.5d] Slow-query observability** — per-dashboard / per-panel query latency + Quickwit request count. Trino query log → Loki or `anomaly-query-stats` QW index.
- [ ] **J54 [P2, 0.3d] Quickwit version-compatibility matrix** — document which connector version works with which Quickwit version; current 0.8.2 lock; upgrade risk list for 0.9/1.0.
- [ ] **J55 [doc, 0.3d] ADR `0003-sqlversion-deprecation.md`** — record the J47 deprecation decision and migration plan.
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
