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
