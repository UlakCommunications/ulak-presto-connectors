# CLAUDE.md — Presto / Trino Quickwit Connector

## Key Commands
- **Unit Tests:** `./mvnw clean test` (or `./mvnw test -pl ulak-presto-quickwit-connector`)
- **Docker Build & Push:** `./push.sh <version> <platform>` (e.g. `./push.sh 0.0.1-develop-latest linux/amd64`)
- **Restart Coordinator:** `kubectl --kubeconfig ~/.kube/yucemonitoring.config -n yucemonitoring rollout restart deployment/maya-trino-multi-coordinator`
- **Trino CLI:** `kubectl --kubeconfig ~/.kube/yucemonitoring.config -n yucemonitoring exec -it deployment/maya-trino-multi-coordinator -- trino`
- **OGM cluster access:** `ssh ulak@192.168.109.203`, then `kubectl` directly — the node holds its own admin kubeconfig, no `--kubeconfig` flag needed (unlike `yucemonitoring` above).

## Architecture & Configuration
- **History Time Threshold:** Configurable via `history-time-threshold-seconds` in `quickwit.properties` (defaults to `10800` / 3 hours). Queries with time range > threshold route to the configured `historyindex`.
- **History Query Rewriting:** `QwQueryRewriter.java` standardizes aggregation field rewriting (`field_<agg>`). 
- **Rhino Javascript Engine:** Query evaluation executes with native `Math` support via scope injection (`var math = Math;`).
- **JFlat Removed:** We use a highly performant custom `flattenJsonNode` inside `QwUtil.java` to parse and flatten Quickwit JSON responses.
- **Redis cache multi-catalog isolation:** `QueryParameters.connectionId` scopes `RedisCacheWorker` refreshes to the catalog that owns each cached query — see [`ulak-presto-connector-base/CLAUDE.md`](ulak-presto-connector-base/CLAUDE.md) for why this matters.
- **Image registries:** OGM (`192.168.109.203`) and `yucemonitoring` pull `maya-nexus:35000/...` from *different* physical registries (OGM has its own local mirror). `./push.sh <tag> <platform> true 192.168.109.203` pushes to both; copy the same build to `yucemonitoring`'s tag with `docker buildx imagetools create` instead of rebuilding. On OGM specifically, master has no internet egress — any Longhorn-system image needs manual mirroring (see DONE.md), and for `longhorn-manager` itself, patching `image:` alone isn't enough: its own `command:` args carry a separate `--manager-image` (and `--engine-image`/etc.) reference that Longhorn's internal upgrade check compares against, and must be patched to match or it deadlocks.
- **Quickwit metastore is Postgres-backed** (`QW_METASTORE_URI`), independent of the `qwdata` Longhorn volume — deleting/recreating that PVC wipes split *files* but not the metastore's *records* of them, leaving orphaned split references that error on query. Clear via `PUT /api/v1/indexes/{id}/clear` (keeps index config, just resets state) rather than delete+recreate.

## READY FOR HANDOVER (Mon Aug 24 2026)
OGM cluster fully recovered from a severe cascading crisis and is stable: Longhorn/iSCSI stuck volume → quickwit WAL corruption → otel memory leak → self-inflicted DiskPressure (unsafe 100Gi PVC expansion) → full PVC/Volume/Replica/Engine rebuild at 30Gi → ImagePullBackOff cascade → a `longhorn-manager` self-referential upgrade deadlock (root cause: `--manager-image` CLI arg not updated by `kubectl set image`, see Architecture above). Also found+fixed a Postgres-metastore/qwdata-volume desync affecting 7 indexes (orphaned split metadata from the PVC rebuild). Backfilled 1 month of synthetic rollup history for all 753 sites (~599MB) — user explicitly parked the full 2-year backfill for now. Full blow-by-blow in `DONE.md` ("Session 23–24 August 2026", items 1–23) and `machine_ops/OGM_DEMO_INSTALL_DONE.md` (section 12, A–Q). Open: `monitoring-jobs` image rebuild for `ROLLUP_RETENTION` (registry ambiguity), flow/metric ratio still short of 3:1, master's no-internet-egress is workaround-only, and a possible dev-vs-OGM gap around paused `anomaly-events-*` alert rules found while reviewing — see TODO.md.
