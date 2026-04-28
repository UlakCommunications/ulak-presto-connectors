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

- [ ] **L01 [P1, 0.5d] Test fixtures from sibling projects** — pull real
      SQL / Aggs DSL examples from `backend/anomaly` and the SQLI project
      (paths to be confirmed during exploration). Drop them under
      `src/test/resources/` per module so unit tests exercise production
      shapes, not invented strings.

- [ ] **L02 [P1, 0.5d] Maven test scaffold** — root `pom.xml` becomes a
      true parent with `<dependencyManagement>` (Trino, JUnit 5 BOM,
      AssertJ, Mockito), `surefire-plugin` 3.x, and consistent test
      dependencies in each of the 4 module poms. `src/test/java` +
      `src/test/resources` directories created where missing.

- [ ] **L03 [P1, 1d] Unit tests for pure-logic classes** — JUnit 5 against
      `AggsDslCompiler` (round-trip DSL → Quickwit aggs JSON for several
      shapes from L01 fixtures), `QueryParameters` (table-name parsing,
      env-var lookup with redaction), `IPToCountry` (UDF behaviour with
      and without MMDB), `QwUtil` (timeout defaults, default client
      construction). Goal: zero external dependencies, runs under
      `mvn test`.

- [ ] **L04 [P1, 2d] Singleton refactor — multi-catalog fix** — replace
      `getInstance()` + `static single` pattern across six classes with
      per-catalog instances:
      `ConnectorBaseUtil`, `UlakQuickwitMetadata`, `QuickwitRecordSetProvider`,
      `QuickwitSplitManager`, `UlakRecordSetProvider`, `UlakSplitManager`.
      `ConnectorBaseUtil`'s static state (`isCoordinator`, `workerId`,
      `keywords`, `JedisPool`, `redisUrl`, `inProgressLocks`) becomes
      instance state on a per-catalog object passed via `ConnectorContext`
      / constructor injection. Verify two `quickwit_a` + `quickwit_b`
      catalogs can coexist with distinct config.

- [ ] **L05 [P1, 0.5d] Resource leak fixes** — `ConnectorBaseUtil.select()`
      Jedis acquire path (try-with-resources for `pool.getResource()`),
      `InfluxdbUtil` static client cache (close-on-eviction + JVM shutdown
      hook), `UlakRecordCursor.close()` (release the underlying iterator
      / connection instead of being a no-op).

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


