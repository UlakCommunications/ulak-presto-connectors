# ulak-presto-quickwit-connector — CLAUDE.md

System-wide architecture doc lives in `backend/anomaly/CLAUDE.md` (cross-repo observability/anomaly stack). This connector exposes Quickwit indexes as Trino TVF `quickwit.system.raw_query(...)` — the single query surface for every Grafana panel in the anomaly stack. Blast radius on regression: whole observability dashboard layer.

**Conventions:**
- Parent repo: `ulak-presto-connectors` (current branch: `develop`)
- Connector compiles Aggs DSL → Quickwit aggs JSON via `AggsDslCompiler.java`
- `sqlversion` modes (`0`, `0.1`, `0.2`) = three parallel compile paths; `0` and `0.1` deprecated (WARN logged), target is `0.2`. ADR: `docs/adr/0003-sqlversion-deprecation.md`

## Key classes (2026-05-12 state)

| Class | Role |
|-------|------|
| `RawQuery.java` | TVF `raw_query(...)` — declares 13 params, runs `analyze()` |
| `RawQuickwitQueryTableHandle.java` | Table handle; carries `computedColumns` (L14 fix) |
| `UlakQuickwitMetadata.java` | Schema inference; `applyFilter()`/`applyLimit()` for plain table mode |
| `QwUtil.java` | Core search + response parsing; delegates Rhino to `RhinoExecutor` |
| `RhinoExecutor.java` | Sandboxed Rhino JS execution; TCL fix for Trino plugin classloader (L15) |
| `PlainTableQuery.java` | SPI-free string helpers for J56 plain table mode |
| `AggsDslCompiler.java` | Compiles `[histogram(...), terms(...)]` DSL → Quickwit aggs JSON |
| `QuickwitSplitManager.java` | Split creation; plain mode → match-all query injection |

## Query surfaces

1. **TVF** (primary): `SELECT * FROM TABLE(quickwit.system.raw_query(qwindex=>'idx', ...))`
2. **Plain table** (J56): `SELECT * FROM quickwit.default."<index-name>"` — no `//params` required; schema from `DocMapping.fieldMappings`; filter/limit pushed down to Quickwit

## Security

- **R01** Rhino sandbox: `ClassShutter` blocks all Java access + `initSafeStandardObjects()` + 100k instruction limit
- **R02** SSRF: `validateQwUrl()` enforces catalog-level URL allowlist (`qw-allowed-urls` property)
- **R28** Redis cache key: SHA-256 (not 32-bit hashCode)

## Quickwit version compatibility

Connector uses `quickwit-java-client` (generated from QW 0.7.1 spec). Cluster runs QW 0.8. Compatibility fixes live in `monitoring_temp/maya-quickwit/quickwit-java-client` (develop, commits b9b5076–fc052c4):
- Leaf models: lenient (unknown fields allowed) — handles new QW 0.8 fields like `coerce`, `output_format`
- `*OneOf*` discriminator classes: strict validation kept — required for oneOf schema selection
- `VersionEnum`: `_0_8("0.8")` added to 5 Versioned*OneOf classes
- `FastFieldOptions`: handles `{"normalizer":"raw"}` object (QW 0.8 `dynamic_mapping.fast`)

**OPEN: QW8-06** — `FastFieldOptions.setActualInstance()` schema registry still missing `FastFieldOptionsOneOfEnabledWithNormalizer` → `Invalid instance type` error. Fix in next session.

## Known open items

- **QW8-06** `FastFieldOptions` schema registry — see `TODO.md` top section, fix next session
- **J46** Connector health + Grafana failover — K8s infra scope, track in `backend/anomaly`
- **L04b** `ConnectorBaseUtil` per-catalog state — gated on customer need (two catalogs with different Redis URLs)
- **R22** Credential history rewrite — `master` branch still has old credential blobs; will be cleaned when `develop` is merged via MR

See `TODO.md` for full item list.

**Repo-wide note (2026-04-28):** `master` branch was rewritten with `git filter-repo` to purge leaked binary GeoIP data; old clones need `git fetch origin && git reset --hard origin/master`. GeoIP policy + connector-wide TODOs (K01–K06) live in `../ulak-presto-connector-base/{CLAUDE,TODO}.md`.
