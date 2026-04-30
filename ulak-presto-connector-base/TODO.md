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

- [ ] **L06 [P1, 0.3d] Logging + security hygiene** — drop the
      `System.out.println` calls in `AggsDslCompiler:471,481`, redact env
      var values in `QueryParameters:407` (do not log
      `REDIS_PASSWORD` / `*_POSTGRES_PASSWORD` / token-shaped strings),
      audit swallowed `catch (Exception e) {}` blocks (`QueryParameters:247-249`,
      others) — convert to `WARN` with stack-trace or rethrow.

- [ ] **L07 [P2, 0.5d] Maven hygiene tail** — pin or upgrade SNAPSHOT
      deps (`json2flat-maya:1.0.3-SNAPSHOT`, `quickwit-java-client:0.0.1.36-SNAPSHOT`)
      to released versions if available, otherwise document why they
      stay SNAPSHOT. Migrate `commons-dbcp:1.4` → `commons-dbcp2` (or
      HikariCP). Drop dead commented-out `<parent>` blocks in module poms.


