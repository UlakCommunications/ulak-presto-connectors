# TODO.md — Next Steps and Open Actions

## Current State
- **Rollup Standardization Complete**:
  - `qw-rollup-engine` tasks standardized to produce `_sum` flow metrics (`u_sum`, `ac_sum`, etc.).
  - `QwQueryRewriter.java` refactored to remove hardcoded exceptions and deterministic metric suffix mappings applied.
  - Redundant string-based `math.ceil` replaces removed in favor of native Rhino execution.
  - Multi-arch Docker Buildx configured and tested in `push.sh`.
- **Performance Optimization Complete**:
  - `JFlat` successfully removed and replaced by a custom performant JSON flattener logic in `QwUtil.java`.
  - All 219 production dashboard queries extracted via Playwright tested successfully against the new logic, resulting in massive performance improvements (0.56s avg latency).
- **CI/CD & Deployment Verified**:
  - `maya-anomaly-platform` Build #282 (`qw-rollup-engine`) passed and deployed on `yucemonitoring`.
  - `maya-trino-platform` Build #379 (`maya/trino`) passed and deployed on `yucemonitoring`.
  - Live query validation on Trino coordinator and Quickwit search API succeeded.

## Next Steps
1. **Merge feature branches into `develop`**:
   - Merge `feature/qw-non-cache-ttl-tuning` and history-related branches into `develop`.
2. **Dashboard Verification**:
   - Verify Grafana QoS and Flow dashboards with `enable_history=true` over time ranges > 1h.
3. **Monitor Performance**:
   - Observe Trino coordinator and Quickwit query latencies during rollup index queries.

### Pending History Index Issues (Discovered Aug 20, 2026)
- The user reported slow throughput in `metrics3` views and requested to verify if `enable_history` and `history_index` are correctly set.
- **Discovery**: 
  1. Grafana dashboards use `maya_global_settings` macros for queries.
  2. The `view_interface_with_site_filter` macro successfully queries `quickwit.otlp_metric` with `//historyenabled=${enable_history}` headers, which are correctly rewritten to `metrics3` and its history index.
  3. However, macros ending in `_rustrino` (e.g., `view_interface_with_site_filter_rustrino`, which is used by the Hub Network Throughput dashboard) execute `from qw_agg('metrics3', ...)` via a PostgreSQL datasource (`rustrino-pg`).
  4. These `rustrino` views **DO NOT** contain `historyenabled` or `historyindex` headers in their macro definitions. As a result, the `qw_agg` table function executes without historical routing, causing timeouts or slow queries on large time ranges for throughput panels.
- **Next Steps**: We must modify the `maya_global_settings` database to inject `//historyenabled=${enable_history}` into the `_rustrino` views, or update the `qw_agg` postgres plugin/connector to automatically append it.
