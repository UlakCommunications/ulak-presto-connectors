# CLAUDE.md — Presto / Trino Quickwit Connector

## Key Commands
- **Unit Tests:** `./mvnw clean test` (or `./mvnw test -pl ulak-presto-quickwit-connector`)
- **Docker Build & Push:** `./push.sh <version> <platform>`. Jenkins `prod=true`: override `prod_ip=192.168.109.203` for OGM, and `version` must match an existing sibling-image tag (e.g. `sqla`'s base `sqli` image resolves by that same string).
- **Yucemonitoring `kubectl`:** prefix with `--kubeconfig ~/.kube/yucemonitoring.config -n yucemonitoring`. Coordinator restart: `rollout restart deployment/maya-trino-multi-coordinator`. Trino CLI: `exec -it deployment/maya-trino-multi-coordinator -- trino`.
- **OGM cluster access:** `ssh ulak@192.168.109.203`, then `kubectl` directly — no `--kubeconfig` (unlike yucemonitoring).

## Architecture & Configuration
- **History Time Threshold:** `history-time-threshold-seconds` in `quickwit.properties` routes queries over that range to `historyindex`. Code default `10800`/3h, but check the live ConfigMap per cluster — OGM currently runs `3600`/1h.
- **History Query Rewriting:** `QwQueryRewriter.java` standardizes aggregation field rewriting (`field_<agg>`).
- **Rhino JS engine:** query evaluation gets native `Math` via scope injection (`var math = Math;`).
- **Custom JSON flattener:** `QwUtil.flattenJsonNode` (not a library) parses Quickwit responses. **Flattened column names don't match a static trace of `flatten()`/`traverseAggregations()`** — a bucket's key appears once per sibling leaf-metric as `<leafAggId>/<ancestorAggId>/key` per ancestor level, not the prefix-stripped path the code implies (confirmed the hard way 2026-08-26). Any column-path fix must be verified by executing it through Trino live, not just matched against Quickwit's raw response shape.
- **Redis cache multi-catalog isolation:** `QueryParameters.connectionId` scopes `RedisCacheWorker` refreshes to the owning catalog — see [`ulak-presto-connector-base/CLAUDE.md`](ulak-presto-connector-base/CLAUDE.md).
- **`ConnectorBaseUtil.select()` holds its Jedis connection for the whole downstream fetch, not just the cache GET/SET** — a slow downstream (e.g. Quickwit) ties up a Redis connection per waiting query, so `JedisPool` exhaustion under load traces back here, not Redis itself. Diagnosed on OGM demo 2026-08-26, not fixed — see TODO.md.
- **Image registries:** OGM and `yucemonitoring` pull `maya-nexus:35000/...` from different physical registries per cluster — push to each explicitly, `kubectl set image` by hostname, never raw IP.
- **Quickwit metastore is Postgres-backed** (`QW_METASTORE_URI`), independent of the `qwdata` volume — clear orphaned splits via `PUT /api/v1/indexes/{id}/clear`, not delete+recreate.
- **Quickwit rejects `"0"` as a datetime-range lower bound** (`invalid query: expected DateTime boundary`) — use `"*"` instead.
- **`qw-rollup-engine`** (sibling repo, `../qw-rollup-engine`) populates the rollup indexes this connector reads — see its README for `host_prefix_chars` (per-task bucket-limit fix) and, as of 2026-08-26, first-run backward backfill. Gotcha: builds go to shared `~/.cargo-target`, not the project-local `target/`.

## READY FOR HANDOVER (Wed Aug 26 2026, later same day)
Diagnosed (not fixed) OGM demo's live Redis-pool-exhaustion — see `ConnectorBaseUtil.select()` bullet above. Implemented + live-tested `qw-rollup-engine`'s first-run backward backfill (forward always prioritized) against the real OGM cluster — ordering, restart-resume, and the never-touches-a-running-task guarantee all confirmed; the `"0"`-range-bound gotcha above was found and fixed along the way. Committed and pushed; **not built/deployed** — see TODO.md.
