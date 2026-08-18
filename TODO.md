# TODO.md — Next Steps and Open Actions

## Current State
- **Catalog Threshold & History Routing**:
  - `history-time-threshold-seconds` dynamically read from catalog config (10800s default) and integrated end-to-end (MR !33 merged into `develop`).
  - Helm charts in `helm_repo1` updated with 3-hour history threshold values.
- **Overlay Topologies & Data Generation**:
  - `data_gen` top-level topology generators updated for dynamic WAN IP and mutual WireGuard endpoint meshes.
  - Overlay dashboard queries tested, verified, and functioning.
- **CI/CD & Builds**:
  - `push.sh` updated to directly use `mybuilder` container BuildKit driver.
  - All unit tests pass cleanly (`./mvnw clean test`).

## Next Steps
1. **Jenkins Pipeline Update**:
   - Ensure Jenkins `maya-trino-platform` job uses `https://` for git tag pushing or sync SCM branch with `develop`.
2. **End-to-End History Verification**:
   - Monitor long-running history queries (>3h) on Grafana dashboards in production/staging environments.
3. **Performance & Memory Monitoring**:
   - Track Trino coordinator memory usage and query execution times with concurrent Quickwit aggregations.
