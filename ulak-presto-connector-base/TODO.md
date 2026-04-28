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

- [ ] **K06 [P1, 1d] Dependabot vulnerability triage** — GitHub flagged
      7 vulnerabilities (2 moderate, 5 low) on the
      `UlakCommunications/ulak-presto-connectors` default branch:
      <https://github.com/UlakCommunications/ulak-presto-connectors/security/dependabot>.
      Read each, triage transitive vs direct, bump the affected
      `pom.xml` versions across `ulak-presto-connector-base` and the
      four connectors as needed.

