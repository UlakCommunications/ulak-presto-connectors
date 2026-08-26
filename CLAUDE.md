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
- **Redis cache multi-catalog isolation:** `QueryParameters.connectionId` scopes `RedisCacheWorker` refreshes to the owning catalog — see [`ulak-presto-connector-base/CLAUDE.md`](ulak-presto-connector-base/CLAUDE.md).
- **Image registries:** OGM and `yucemonitoring` pull `maya-nexus:35000/...` from different physical registries (same alias, different IP per cluster) — push to each explicitly; always `kubectl set image` using the hostname form, never the raw IP.
- **Quickwit metastore is Postgres-backed** (`QW_METASTORE_URI`), independent of the `qwdata` volume — clear orphaned splits via `PUT /api/v1/indexes/{id}/clear`, not delete+recreate.
- **`qw-rollup-engine`** (sibling repo, `../qw-rollup-engine`) populates the rollup indexes this connector reads — see its README for `host_prefix_chars` (generic per-task bucket-limit fix). Gotcha: builds go to a shared `~/.cargo-target` (`~/.cargo/config.toml`), not the project-local `target/`.

## READY FOR HANDOVER (Wed Aug 26 2026)
`ROLLUP_RETENTION` deployed to OGM, verified live. Ran a full 8-day/753-site rollup backfill (tile-and-shift, hex-prefix-partitioned) — clean, ~311M docs, no duplication. Measured real rollup density (~4GB/day, all sites) — `ROLLUP_RETENTION=1 month` likely won't fit the 90GB volume once it fills that deep, see TOBEDECIDED.md. Ran a 210-combo dashboard Query Sweep (15 dashboards × 14 ranges): 3 broken on every range (incl. a *new* null-cast-to-double instance on `Quality_of_Service`), published as an Artifact — detail in TODO.md. `backend/sqla`'s Jenkins build: fixed 3 of 4 missing deps, still blocked on `libclang`. Full detail: `DONE.md` ("Session 25-26 August 2026").
