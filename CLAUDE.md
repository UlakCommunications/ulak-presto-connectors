# CLAUDE.md — Presto / Trino Quickwit Connector

## Key Commands
- **Unit Tests:** `./mvnw clean test` (or `./mvnw test -pl ulak-presto-quickwit-connector`)
- **Docker Build & Push:** `./push.sh <version> <platform>` (e.g. `./push.sh 0.0.1-develop-latest linux/amd64`)
- **Restart Coordinator:** `kubectl --kubeconfig ~/.kube/yucemonitoring.config -n yucemonitoring rollout restart deployment/maya-trino-multi-coordinator`
- **Trino CLI:** `kubectl --kubeconfig ~/.kube/yucemonitoring.config -n yucemonitoring exec -it deployment/maya-trino-multi-coordinator -- trino`
- **OGM cluster access:** `ssh ulak@192.168.109.203`, then `kubectl` directly — no `--kubeconfig` flag needed (unlike `yucemonitoring` above).

## Architecture & Configuration
- **History Time Threshold:** `history-time-threshold-seconds` in `quickwit.properties` (default `10800`/3h) — queries over that range route to `historyindex`.
- **History Query Rewriting:** `QwQueryRewriter.java` standardizes aggregation field rewriting (`field_<agg>`).
- **Rhino Javascript Engine:** query evaluation gets native `Math` via scope injection (`var math = Math;`).
- **Custom JSON flattener:** `QwUtil.flattenJsonNode` (not a library) parses/flattens Quickwit responses.
- **Redis cache multi-catalog isolation:** `QueryParameters.connectionId` scopes `RedisCacheWorker` refreshes to the owning catalog — see [`ulak-presto-connector-base/CLAUDE.md`](ulak-presto-connector-base/CLAUDE.md).
- **Image registries:** OGM and `yucemonitoring` pull `maya-nexus:35000/...` from different physical registries (same alias, different IP per cluster) — push to each explicitly; always `kubectl set image` using the hostname form, never the raw IP.
- **Quickwit metastore is Postgres-backed** (`QW_METASTORE_URI`), independent of the `qwdata` volume — clear orphaned splits via `PUT /api/v1/indexes/{id}/clear`, not delete+recreate.
- **`qw-rollup-engine`** (sibling repo, `../qw-rollup-engine`) populates the rollup indexes this connector reads — see its README for `host_prefix_chars` (generic per-task bucket-limit fix). Gotcha: builds go to a shared `~/.cargo-target` (`~/.cargo/config.toml`), not the project-local `target/`.

## READY FOR HANDOVER (Tue Aug 25 2026)
`qw-rollup-engine`: built a generic `host_prefix_chars` site-partitioning fix for Quickwit bucket-limit overflows, used it on 2 tasks; separately revived 3 metric types (`maya_bfd`/`maya_system_services`/`maya_ifstatus`) dead for months from a Quickwit `top_hits` panic bug. All 5 fixes deployed+verified on OGM and yucemonitoring — likely also root cause of a recurring Quickwit OOMKill. Connector: fixed a null-cast-to-double bug, deployed to OGM. Also fixed live: RedisCacheWorker's `services.public.` bug (both clusters) and the Hub Network Throughput panel's stale `//columns=` (OGM only). Full detail: `DONE.md` ("Session 25 August 2026"). Open: same panel fix + an agent's 18 candidate fixes still needed on yucemonitoring; other rollup tasks unswept for the same bucket-limit risk; OGM's clock skew — see `TODO.md`.
