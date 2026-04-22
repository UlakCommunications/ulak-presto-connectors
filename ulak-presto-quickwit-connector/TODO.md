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

## Notes — Aggs DSL reference
- Form: `[histogram(...), terms(...), max(...), sum(...), avg(...), min(...), count(...)]`. Compiled by `AggsDslCompiler.java` to Quickwit aggs JSON.
- **Auto-ID**: histogram → `"date"`; terms/metrics → field name (last segment after last dot). Numeric fallback on collision — see J48.
- **Column paths**: `<id>/key`, `<id>/key_as_string`, `<id>/value`.
- **`sqlversion` modes** (see CLAUDE.md of anomaly repo for full table): `"0"` legacy flat + `replacefromcolumns`; `"0.1"` recursive traversal (`<id>/value`); `"0.2"` stripped suffixes (`<id>` bare).
- **`top_hits` not supported** in Quickwit 0.8 — removed from gateway/CPE dashboards.
