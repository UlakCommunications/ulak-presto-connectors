# ulak-presto-quickwit-connector — CLAUDE.md

System-wide architecture doc lives in `backend/anomaly/CLAUDE.md` (cross-repo observability/anomaly stack). This connector exposes Quickwit indexes as Trino TVF `quickwit.system.raw_query(...)` — the single query surface for every Grafana panel in the anomaly stack. Blast radius on regression: whole observability dashboard layer.

**Conventions:**
- Parent repo: `ulak-presto-connectors` (current branch `no_jflat_in_aggs`)
- Connector compiles Aggs DSL → Quickwit aggs JSON via `AggsDslCompiler.java`
- `sqlversion` modes (`0`, `0.1`, `0.2`) = three parallel compile paths; deprecation plan in TODO J47

See `TODO.md` for open items (category J from the 2026-04-22 architectural review).
