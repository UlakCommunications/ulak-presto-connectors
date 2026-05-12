# TODO — ulak-presto-connector-base

Open items only. Completed work is moved to
[`CHANGELOG.md`](CHANGELOG.md).

## GeoIP cleanup (2026-04-28) — connector-base scope

Remaining open items after the history rewrite + IPToCountry refactor.
Context in [`CLAUDE.md`](CLAUDE.md).

- [ ] **K05 [P2, 0.3d] License attribution audit** — Grafana panels and
      reports in `backend/anomaly` that surface GeoIP data must include
      the EULA-mandated attribution strings:
      *"This product uses IP2Location LITE data
      ([https://lite.ip2location.com](https://lite.ip2location.com))."*
      and *"This product includes GeoLite2 data created by MaxMind
      ([https://www.maxmind.com](https://www.maxmind.com))."* Audit
      downstream observability dashboards for compliance; add to
      dashboard description or footer panel where missing.

## Architectural review (2026-04-28) — multi-catalog bug + code quality

Triggered by user report "Trino'da aynı connector'den iki tane
ekleyemiyoruz" plus a broader code-quality audit. Two parallel exploration
agents identified six static-singleton sites that break two-catalog
registration, plus a top-10 list of P1/P2 issues, plus zero test coverage.
Plan: write tests first (otherwise refactor is risky), then refactor
incrementally. Each L-item lands as its own commit; entries move from
this file to `CHANGELOG.md` once done.


- [ ] **L04b [P3, 1d] ConnectorBaseUtil per-catalog state — gated on
      real need.** L04a already replaced the `static single` +
      `getInstance()` pattern across the five SPI classes; two catalogs
      sharing infra (typical deployment) work today. Remaining static
      state on `ConnectorBaseUtil` (`isCoordinator`, `workerId`,
      `workerIndexToRunIn`, `keywords`, `redisUrl`, `JedisPool`,
      `objectMapper`, `inProgressLocks`) only bites if two catalogs in
      one Trino node target *different* Redis URLs. Refactor into a
      per-catalog runtime object (or a `Map<catalogName, Runtime>`
      registry) when a customer asks for it — not a speculative cut.

- [ ] **L14 [P3, 1-2d] Deeper TVF schema fix (returned-table-mismatch).**
      Low priority — production rarely hits this on the live cluster
      and the symptom is just one query failing, not the connector
      melting down.
      Surfaced live: `RewriteTableFunctionToTableScan` rule's
      `Preconditions.checkState` fails with "returned table does not
      match the node's output" when the descriptor returned from
      `RawQueryFunction.analyze()` and the column list returned from
      `UlakQuickwitMetadata.getTableMetadata` disagree. They disagree
      because each path runs its own Quickwit search and the responses
      differ (a row appears between calls, alias keys from L09 appear
      only at execute time, etc.). L11 was a half-fix (only patched
      `analyze()`) and was reverted (commit `5f3b3ec`). The right fix
      binds three sites to a single column-list source:
      `analyze()` (returnedType descriptor), `getTableMetadata`
      (`getColumnsBase` result), and `parseResponseHits` (runtime row
      column map). Probably easiest to compute once at handle creation
      and stash it on `RawQuickwitQueryTableHandle` so all three read
      from the same handle field.

## Code review findings (2026-05-12) — base connector scope

- [x] **R20 [Critical] `encodeUriComponent` logs encoded secret at INFO level** — `QueryParameters.java:452`: comment above method says "do not log"; log line removed.
- [x] **R21 [Critical] Credential files tracked in git** — `trino/etc/catalog/quickwit.properties` + `tenant.properties` contain live Redis + PostgreSQL passwords. Untracked via `git rm --cached`, added to `.gitignore`, `.example` files created. History rewrite (filter-repo) tracked as R22.
- [ ] **R22 [Critical] Credential history rewrite** — `filter-repo` to purge `trino/etc/catalog/quickwit.properties` and `tenant.properties` blobs from all branches + tags + force-push 4 remotes. Rotate Redis + PostgreSQL credentials before and after.
- [x] **R23 [High] `getJedisPool()` double-checked locking without `volatile`** — `ConnectorBaseUtil.java:129-139`: `jedisPool` not volatile; two threads can create two pools, one leaked. Fixed with `volatile`.
- [x] **R24 [High] `arrangeCase` reads `keywords` map without synchronization** — `ConnectorBaseUtil.java:141-151`: concurrent write in `setKeywords` (synchronized) vs unsynchronized read causes `ConcurrentModificationException`. Fixed.
- [x] **R25 [High] `getObjectMapper()` unsynchronized lazy-init race** — `ConnectorBaseUtil.java:156-162`: `objectMapper` not volatile. Fixed with `volatile`.
- [x] **R26 [High] Blind credential substring replace corrupts other values** — `QueryParameters.java:425-426`: second `source.replace(resEnv, ...)` replaces the raw credential value everywhere in source string. Fixed by removing blind second replace.
- [x] **R27 [Medium] Base32 false-positive corrupts plain queries** — `QueryParameters.java:145-149`: plain names like `METRICS3` satisfy Base32 constraints and get silently decoded. Added `//` prefix check before Base32 attempt.
- [x] **R28 [Medium] 32-bit `hashCode()` as Redis cache key** — Fixed: `QueryParameters.sha256Hex()` computes full SHA-256 of the normalized query; stored as `cacheKey` field; used by `ConnectorBaseUtil` for all Redis operations. `int hash` kept for in-memory `RedisCacheWorker.stats` map (not security-critical).
- [x] **R29 [Medium] Comment-strip regex passed to literal-string replace** — `QueryParameters.java:119`: `StringUtils.replace` treats pattern as literal; regex metacharacters never evaluate. Dead code; removed.
- [x] **R30 [Medium] `split("=")` truncates values containing `=`** — `QueryParameters.java:165-168`: `qwurl=http://host?a=b` loses everything after second `=`. Fixed with `split("=", 2)`.
- [x] **R31 [Medium] Raw `NullPointerException` instead of `TrinoException`** — `ConnectorBaseUtil.java:328`: throw `TrinoException(INVALID_FUNCTION_ARGUMENT)` for clean Trino error propagation. Fixed.
- [x] **R32 [Low] `redactIfSecret` misses URL-embedded passwords** — `QueryParameters.java:403-404`: pattern doesn't match `redis-url` or `qwurl`. Added `url` to pattern.
- [x] **R33 [Low] Logger fields not `static final`** — Base connector classes. Fixed.

- [ ] **L15 [P3, 0.5d, low priority] Rhino classloader debug — `Math.floor` fails
      under Trino plugin classloader.** L13 made the Rhino failure
      visible (`hasjs script execution failed: <ExceptionClass>:
      <message>`); now figure out *why* `Math.floor(1777551625/1000)`
      fails when Rhino is loaded by the Trino plugin classloader. The
      shaded jar bundles `org.mozilla:rhino:1.8.1` +
      `rhino-engine:1.8.1` + `rhino-runtime:1.7.15.1`. Hypothesis:
      version skew between rhino and rhino-runtime, or
      Trino-plugin-isolation hides Rhino's stdlib initialisation. Repro
      = reopen Throughput Chart / Network Throughput while watching
      Trino logs for the new ApiException — the Rhino exception class
      will name the exact failure (NPE? ClassNotFound? EvaluatorException?).



