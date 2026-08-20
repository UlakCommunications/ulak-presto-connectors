# CLAUDE.md — Presto / Trino Quickwit Connector

## Key Commands
- **Unit Tests:** `./mvnw clean test` (or `./mvnw test -pl ulak-presto-quickwit-connector`)
- **Docker Build & Push:** `./push.sh <version> <platform>` (e.g. `./push.sh 0.0.1-develop-latest linux/amd64`)
- **Restart Coordinator:** `kubectl --kubeconfig ~/.kube/yucemonitoring.config -n yucemonitoring rollout restart deployment/maya-trino-multi-coordinator`
- **Trino CLI:** `kubectl --kubeconfig ~/.kube/yucemonitoring.config -n yucemonitoring exec -it deployment/maya-trino-multi-coordinator -- trino`

## Architecture & Configuration
- **History Time Threshold:** Configurable via `history-time-threshold-seconds` in `quickwit.properties` (defaults to `10800` / 3 hours). Queries with time range > threshold route to the configured `historyindex`.
- **History Query Rewriting:** `QwQueryRewriter.java` standardizes aggregation field rewriting (`field_<agg>`). 
- **Rhino Javascript Engine:** Query evaluation executes with native `Math` support via scope injection (`var math = Math;`).
- **JFlat Removed:** We use a highly performant custom `flattenJsonNode` inside `QwUtil.java` to parse and flatten Quickwit JSON responses.

## READY FOR HANDOVER (Thu Aug 20 13:22:00 +03 2026)
Session completed. Successfully replaced JFlat with a custom recursive `flattenJsonNode` in `QwUtil.java`, resolving high GC and string allocation overhead while preserving existing Grafana dashboard JSON path queries. Handled Trino history routing investigations for `metrics3` views by discovering that `rustrino` PostgreSQL views (e.g. `view_interface_with_site_filter_rustrino`) wrap `qw_agg` calls without appending the required `//historyenabled=${enable_history}` comments to trigger Trino's `history_index` logic. Documented findings in TODO.md for the next session. All code merged, tested, and Jenkins deployments launched for `maya-trino-platform` (Build 387).
