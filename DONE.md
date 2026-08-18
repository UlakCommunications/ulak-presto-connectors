# DONE.md — Completed Work

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
