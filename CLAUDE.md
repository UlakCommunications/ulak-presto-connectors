# CLAUDE.md — Presto / Trino Quickwit Connector

## Key Commands
- **Unit Tests:** `./mvnw clean test` (or `./mvnw test -pl ulak-presto-quickwit-connector`)
- **Docker Build & Push:** `./push.sh <version> <platform>`. Jenkins `prod=true` builds: override `prod_ip=192.168.109.203` for OGM (its default isn't OGM), and `version` must match an existing sibling-image tag (e.g. `sqla`'s base `sqli` image is resolved by that same version string).
- **Restart Coordinator:** `kubectl --kubeconfig ~/.kube/yucemonitoring.config -n yucemonitoring rollout restart deployment/maya-trino-multi-coordinator`
- **Trino CLI:** `kubectl --kubeconfig ~/.kube/yucemonitoring.config -n yucemonitoring exec -it deployment/maya-trino-multi-coordinator -- trino`
- **OGM cluster access:** `ssh ulak@192.168.109.203`, then `kubectl` directly — no `--kubeconfig` flag needed (unlike `yucemonitoring` above).

## Architecture & Configuration
- **History Time Threshold:** `history-time-threshold-seconds` in `quickwit.properties` (default `10800`/3h) — queries over that range route to `historyindex`.
- **History Query Rewriting:** `QwQueryRewriter.java` standardizes aggregation field rewriting (`field_<agg>`).
- **Rhino Javascript Engine:** query evaluation gets native `Math` via scope injection (`var math = Math;`).
- **Custom JSON flattener:** `QwUtil.flattenJsonNode` (not a library) parses/flattens Quickwit responses.
- **Flattened column names don't match a static trace of `flatten()`/`flattenJsonNode()`/`traverseAggregations()`** — confirmed 2026-08-26 the hard way (a column-path fix derived from source-reading was wrong, shipped live, had to be reverted). A bucket's key appears once per sibling leaf-metric as `<leafAggId>/<ancestorAggId>/key` for every ancestor level, not the prefix-stripped path the code implies. Verify any column-path fix by running it through Trino live — matching Quickwit's raw response shape alone isn't enough.
- **Redis cache multi-catalog isolation:** `QueryParameters.connectionId` scopes `RedisCacheWorker` refreshes to the owning catalog — see [`ulak-presto-connector-base/CLAUDE.md`](ulak-presto-connector-base/CLAUDE.md).
- **Image registries:** OGM and `yucemonitoring` pull `maya-nexus:35000/...` from different physical registries (same alias, different IP per cluster) — push to each explicitly; always `kubectl set image` using the hostname form, never the raw IP.
- **Quickwit metastore is Postgres-backed** (`QW_METASTORE_URI`), independent of the `qwdata` volume — clear orphaned splits via `PUT /api/v1/indexes/{id}/clear`, not delete+recreate.
- **`qw-rollup-engine`** (sibling repo, `../qw-rollup-engine`) populates the rollup indexes this connector reads — see its README for `host_prefix_chars` (generic per-task bucket-limit fix). Gotcha: builds go to a shared `~/.cargo-target` (`~/.cargo/config.toml`), not the project-local `target/`.

## READY FOR HANDOVER (Wed Aug 26 2026, later same day)
Chased the Query Sweep's 3 broken dashboards. `Top_Sites_Traffic` deprioritized (unused, per user). Attempted a `Quality_of_Service`/`SLA_Chart` fix — applied live, disproven by running it through Trino, fully reverted; real bug still unidentified (see architecture bullet above, DONE.md item 3). Connector null-guard extended to bucket keys (tested, not deployed) — valid but not this bug. yucemonitoring's stale-`//columns=` audit reviewed and confirmed trustworthy, ready via `!`, untouched. New paused thread: yucemonitoring is missing dedicated netlink/bfd/etc. rollup indexes OGM has — see TODO.md.
