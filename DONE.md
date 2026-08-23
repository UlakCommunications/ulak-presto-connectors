# DONE.md — Completed Work

## Session 21 August 2026: Redis cache correctness, cross-catalog cache isolation, OGM data-gen deadlock

### 1. Redis cache bugs (see `ulak-presto-connector-base/TODO.md` C01/C02/C04 for full detail)
- **C01** `RedisCacheWorkerItem` used to extend the TTL of a stale/erroring cache entry on a failed background refresh instead of clearing it — a permanently-broken query's bad result could live forever. Fixed: delete the key on error.
- **C02** No idle-eviction for `cache=true` entries — a query behind a since-deleted dashboard panel got refreshed by the background worker forever. Added `QueryParameters.lastAccess` + `idleTtlInSeconds` (default 6h, `//idlettl=`); `RedisCacheWorker` now evicts idle entries instead of refreshing them.
- **C04 (the big one)**: multiple `mayapostgres` catalogs (`maya_tenant`/`maya_grafana`/`maya_envanter`) share one Redis keyspace and all register `DBType.PG`; `RedisCacheWorker` only filtered by `DBType`, so one catalog's background worker could refresh another catalog's cached query against the *wrong* Postgres database. Live symptom: `relation "public.site_temp_version_state" does not exist` on a query that works fine run directly through the correct catalog. Fixed with `QueryParameters.connectionId` (catalog's own pgUrl/qwUrl/influx url) + a connection-identity check in `RedisCacheWorker`, applied across all three connectors that spin up a worker (postgres, quickwit, influxdb).
- All three fixes are covered by unit tests (167/167 passing across the repo) and deployed live to both OGM (`maya-nexus:35000/maya/trino:3.1.4-20260810-OGM`, tag kept stable, content replaced) and `yucemonitoring` (`maya-nexus:35000/maya/trino:0.0.1`) — both built from one image, copied byte-identical between registries via `docker buildx imagetools create` rather than rebuilt twice.

### 2. OGM `data-gen` was silently producing zero data — root cause + fix
- Symptom: no data reaching Quickwit for hours, no errors in logs, no crash — just silence.
- Root cause found via `py-spy dump` on the live worker processes (added `py-spy` to the `data-gen` image + a temporary `SYS_PTRACE` capability, removed again once done): `export_spans()` in `data_gen_cyclic.py` passed the *same* per-process `ThreadPoolExecutor` (`pool`, 4 workers) both as the outer site-level dispatcher and, when a single site's span list exceeded `chunk_size=500`, as the executor for a *nested* sub-chunk dispatch. Once all 4 pool threads were busy with outer per-site tasks, each one's attempt to submit inner chunks back into the same (fully occupied) pool deadlocked permanently — classic thread-pool reentrancy. It only triggered in the `NORMAL` phase (where `FLOW_MULTIPLIER=20` pushes flow-span counts over 500); the `ANOMALY` phase's small span counts never hit it, which made the bug look intermittent.
- Fix: the nested `export_spans(..., executor=pool)` calls inside `process_site_normal`/`process_site_anomaly` now pass `executor=None` — sub-chunks send sequentially within the already-parallel outer worker thread instead of re-entering the pool.
- Also switched `multiprocessing` start method to `spawn` (Linux default `fork` + gRPC's C-core is a known unsafe combination) and added an explicit `timeout=15` to the gRPC `Export()` call, as defense-in-depth so a future stall fails loud instead of silent — neither turned out to be the actual root cause, but both are legitimate hardening.
- Separately fixed: OGM's `data-gen` deployment had `ANOMALY_SECS: "90"` **hardcoded** in the pod spec's literal `env:` list, which silently shadows the `data-gen-config` ConfigMap's `ANOMALY_SECS` value (Kubernetes: literal `env:` always wins over `envFrom`). User wanted OGM anomaly-free; `kubectl set env deployment/data-gen -n maya3 ANOMALY_SECS=0` fixed it at the actual point of truth.
- Deployed to OGM only — `yucemonitoring`'s `data-gen` runs an older, different (non-cyclic, non-multiprocess) script; not migrated, see TODO.

### 3. Discovered (not fixed): OGM cluster infra instability
- `ssb-sdwan-master`'s k8s API server + SSH became intermittently/then persistently unreachable while investigating the above. Root cause traced to a stuck Longhorn PVC (`pvc-b2109990-cf13-44dc-a997-d8d4c0c2f5d8`, backing `maya-quickwit`'s data volume): CSI `VolumeAttachment` was stuck exclusively attached to `ssb-sdwan-master` with no pod actually using it there, while `maya-quickwit`'s pod was rescheduled to `ssb-sdwan-worker1` and could never mount it (`Multi-Attach error`). This is unrelated to any code change this session — see TODO, still open as of 2026-08-23.

### 4. Housekeeping
- A real (per user: temporary/throwaway) `postgres_viewer` Postgres password got committed in plaintext in `trino/etc/catalog/maya_tenant.properties` + `mayapostgres.properties` (commit `eea9592`), already pushed to the `gitlab_mfyuce` mirror — `.gitignore` only covered the older `quickwit.properties`/`tenant.properties` names, not the newer `maya_*` ones. User confirmed no rotation needed (temp creds); `.gitignore` gap itself is still open, see TODO.

## Session 17–18 August 2026: Catalog History Threshold, Overlay Topology Generation & CI/CD Enhancements

### 1. Catalog-Scoped History Time Threshold (MR !33)
- **Feature:** Added dynamic reading of `history-time-threshold-seconds` from catalog properties (`quickwit.properties`), eliminating hardcoded Java thresholds.
- **Propagation:** Value is read by `UlakQuickwitConnectorFactory`, passed via `UlakQuickwitConnector` -> `UlakQuickwitMetadata` -> `RawQuery` -> `QwUtil.select()`.
- **Default & Fallback:** Defaults to `10800` (3 hours) if unspecified in properties.
- **Helm Configuration:** Configured `history-time-threshold-seconds: 10800` in `helm_repo1` for `trino-single`, `trino-multi`, and `base-trino`.
- **Unit Testing:** Added unit test `testCatalogHistoryThresholdResolution` in `QwUtilParseTest`.
- **MR Review & Merge:** Reviewed GitLab MR !33 and merged cleanly into `develop`.

### 2. Overlay Dashboard Topology & WireGuard Endpoint Matching (`data_gen`)
- **Problem:** Grafana Overlay dashboard (`ff9jl1pb8nrpca`) showed empty `Dst Peer` / `peers_sec` and zero tunnel traffic (Panel 24) because `data_gen` emitted static WireGuard endpoints (`1.2.3.4:51820`), failing the self-join `SPLIT_PART(m1.endpoint, ':', 1) = m2.ip`.
- **Fix:** Rewrote topology logic in `metric_generators.py` (`get_site_topology`) to query PostgreSQL `site` and `overlay_site_relation` tables. Established paired mutual Hub <-> Spoke meshes where each Spoke `wg0` endpoint points to the Hub's WAN IP (`{hub_wan_ip}:51820`) and vice-versa.
- **Performance:** Added gRPC export chunking (`chunk_size=500`) and ThreadPoolExecutor parallelization (`workers=8`) in `run_k8s.py`.
- **Deployment:** Deployed updated ConfigMap `data-gen-cyclic-script` to `yucemonitoring` and verified live cyclic generation (`fail_pg=0, fail_export=0`).
- **Verification:** Verified that the Overlay dashboard variable `peers_sec` populates with destination peers and Panel 24 renders live RX/TX tunnel traffic.

### 3. Docker Buildx CI/CD in `push.sh`
- **Problem:** Single-platform builds (e.g. `linux/amd64`) fell back to the `default` Docker engine builder, which does not support BuildKit `--output=type=registry,registry.insecure=true` to insecure registries.
- **Fix:** Updated `push.sh` to always target the containerized `mybuilder` BuildKit instance (`docker buildx use mybuilder`).

### 4. History Query Rewriting for Rollup Schemas
- **Enhancement:** Updated `QwUtil.java` query rewriting logic so rollup indexes starting with `rollup_` or containing standard suffixes are properly rewritten to `_sum` metrics.

---

## Session 17 August 2026: Standardized Rollup Schemas, Connector Rewriter Simplification & CI/CD Deployment

### 1. Standardized Flow Rollup Metrics in `qw-rollup-engine`
- **Problem:** Flow rollup tasks originally stored metrics without standard aggregation suffixes (`u`, `ac`, `ab`, `t`, `u_ac`, `t_ab`), leading to ad-hoc exceptions in the connector and workaround comments (`//nohistorysuffix=`).
- **Fix:** Standardized all 5 flow rollup tasks (`site_app`, `site_src_ip`, `site_dst_ip`, `site_ip_proto`, `site_src_dst_ip`) to use `_sum` suffixes (`u_sum`, `ac_sum`, `ab_sum`, `t_sum`, `u_ac_sum`, `t_ab_sum`) in both `metrics` mapping and `aggs` definitions. Updated `processor.rs` (`apply_top_n_filter`) with fallback checking for `{field}_sum`.

### 2. Simplified Presto / Quickwit Connector Query Rewriter (`QwQueryRewriter.java` & `QwUtil.java`)
- **Problem:** The connector contained hardcoded lists and `//nohistorysuffix=` parser loops to bypass suffix rewriting for flow metrics.
- **Fix:** Completely eliminated hardcoded lists and dynamic `//nohistorysuffix=` parsing. The query rewriter now deterministically and idempotently rewrites all aggregation metrics to `field_<agg>` across the board.

### 3. Cleaned Up Redundant String-based Math Replaces (`QwUtil.java`)
- **Problem:** `QwUtil.java` performed string replaces (`query.replace("math.ceil", "Math.ceil")...`) before passing to Rhino.
- **Fix:** Removed string-based replaces. Rhino execution natively supports `Math.*` with `var math = Math;` in JavaScript script scope.

### 4. Jenkins CI/CD & Deployment to `yucemonitoring`
- **Rollup Engine Build:** `maya-anomaly-platform` Build #282 passed (`SUCCESS`). Image `maya/qw-rollup-engine:latest` deployed to `yucemonitoring`.
- **Trino Connector Build:** Fixed multi-arch Buildx builder configuration in `push.sh`. `maya-trino-platform` Build #379 passed (`SUCCESS`) with all 166 unit tests passing. Image `maya/trino:0.0.1-develop-latest` deployed to `yucemonitoring`.
- **Live Verification:** Verified `qw-rollup-engine` task processing and executed live `quickwit.system.raw_query` tests on the Trino coordinator.

## 16. Replaced JFlat with Performant Custom Flattener
- **Problem:** `JFlat` library parsing and recursive mapping caused a severe performance bottleneck during object instantiation and tree traversal, increasing query times by 10-20 seconds on large datasets.
- **Fix:** Wrote a highly performant native `flattenJsonNode` method inside `QwUtil.java` to recursively flatten JSON trees into maps, exactly matching `JFlat`'s column naming conventions (e.g., `/span_attributes/x`). `JFlat` object initialization and imports were completely removed.
- **Verification:** Ran a comprehensive regression test using Playwright to extract 219 production queries from 79 Grafana dashboards. The native flattener passed all backward compatibility tests, and queries completed in 0.56 seconds on average.
