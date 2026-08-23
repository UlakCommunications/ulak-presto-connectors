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
- **Redis cache multi-catalog isolation:** `QueryParameters.connectionId` scopes `RedisCacheWorker` refreshes to the catalog that owns each cached query — see [`ulak-presto-connector-base/CLAUDE.md`](ulak-presto-connector-base/CLAUDE.md) for why this matters.
- **Image registries:** OGM (`192.168.109.203`) and `yucemonitoring` pull `maya-nexus:35000/...` from *different* physical registries (OGM has its own local mirror). `./push.sh <tag> <platform> true 192.168.109.203` pushes to both; copy the same build to `yucemonitoring`'s tag with `docker buildx imagetools create` instead of rebuilding.

## READY FOR HANDOVER (Sun Aug 23 2026)
Root-caused and fixed two separate production bugs found while debugging a user report ("cache query'de tablo bulunamıyor"): (1) `RedisCacheWorker` only filtered background cache-refresh by `DBType`, so multiple `mayapostgres` catalogs on different Postgres databases could refresh each other's cached queries against the wrong DB — fixed with a new `connectionId` field, deployed + verified on OGM and `yucemonitoring`. (2) OGM's `data-gen` was silently producing zero data for hours with no errors — root-caused via `py-spy` to a `ThreadPoolExecutor` reentrancy deadlock in `export_spans()` (fixed), plus a hardcoded `ANOMALY_SECS` env var shadowing its ConfigMap value (fixed). Along the way found OGM's control plane is down due to a stuck Longhorn PVC attachment (`maya-quickwit` can't mount its data volume) — **still unresolved**, needs console access; see TODO.md. All connector-base changes covered by unit tests (167/167 passing).
