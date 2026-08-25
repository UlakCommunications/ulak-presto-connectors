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
- **Image registries:** OGM (`192.168.109.203`) and `yucemonitoring` pull `maya-nexus:35000/...` from *different* physical registries — same alias, different IP per cluster. Build+push to each explicitly (`docker buildx build --output=type=registry,registry.insecure=true -t <registry-ip>:35000/maya/<image>:<tag> . --push`); always `kubectl set image` using the `maya-nexus:35000` hostname form, never the raw IP (containerd only trusts the hostname as insecure/HTTP). Longhorn-image-mirroring gotchas: see TODO.md.
- **Quickwit metastore is Postgres-backed** (`QW_METASTORE_URI`), independent of the `qwdata` Longhorn volume — deleting/recreating that PVC wipes split *files* but not the metastore's *records* of them, leaving orphaned split references that error on query. Clear via `PUT /api/v1/indexes/{id}/clear` (keeps index config, just resets state) rather than delete+recreate.
- **`qw-rollup-engine`** (sibling repo, `../qw-rollup-engine`, own Cargo/git) is what populates the rollup indexes this connector's history-routing reads from. Tasks with a high-cardinality top-level `terms()` dimension (host/site UUID) can exceed Quickwit's aggregation bucket limit (65000) — the generic fix is `"host_prefix_chars": N, "host_prefix_field": "<field>"` on the task, which transparently fans the query out into 16^N hex-prefix-filtered sub-queries and sums the results. **Gotcha:** this machine's `~/.cargo/config.toml` redirects ALL Rust builds to a shared `target-dir` (`~/.cargo-target`) — the project-local `qw-rollup-engine/target/` is a dead path; always get the built binary from the shared dir, not the project-local one.
