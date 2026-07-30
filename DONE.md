# DONE.md — Completed Work

## Session 30 July 2026: QoS History Resolution & Dynamic Suffix Exclusions

### 1. QoS History "No Data" Fix (`tasks.json`)
- **Problem:** The `netlink` rollup task (`netlink_${ROLLUP_INTERVAL}m`) in `tasks.json` did not group by class name (`m_name`) or site ID (`m_site_id`). Toggling `enable_history` on the QoS dashboard caused the queries to return 0 rows since these fields did not exist in the rollup index `metrics3_15`.
- **Fix:** Added `m_site_id`, `m_name`, and `m_site_name` as dimensions to `netlink_${ROLLUP_INTERVAL}m` in `tasks.json` and nested the aggregations in the template. Deployed configuration via Kubernetes ConfigMap and restarted `qw-rollup-engine`. Cleared the checkpoint key in Redis to backfill rollup data from today 00:00.

### 2. QoS Dashboard Query Typo Fix
- **Problem:** The targets inside `Class Traffic`, `Queue Depth`, and `QoS` panels of the Quality of Service dashboard (`cez1fkculu1hca`) had a typo `//columns1=` instead of `//columns=`. When history returned 0 rows, Trino could not dynamically discover the schema and failed with a compilation error.
- **Fix:** Created a script `fix_qos_dashboard.py` to replace all `columns1=` with `columns=`. Kept the original `folderId: 65` from the metadata payload to prevent Grafana from moving the dashboard to the General folder.

### 3. Dynamic Suffix Exclusions (`QwQueryRewriter.java` & `QwUtil.java`)
- **Problem:** Excluded flow metrics were hardcoded in the Java rewriter code, violating connector design guidelines and breaking Trino dynamic querying for customized rollup schemas.
- **Fix:** Modified the Presto Quickwit connector to parse a dynamic comment header `//nohistorysuffix=...` in target queries. Excluded fields are parsed on-the-fly and passed to the Gson rewriter, removing hardcoded metric strings from the Java codebase.

---

## Session 29 July 2026: Flow & Resource Utilization History Routing Fixes

## 1. Moved History Query Rewriter Order (`QwUtil.java`)
- **Problem:** History index query rewriter (`rewriteQueryForHistory`) was run on the raw query text before Javascript execution. If the query contained Javascript evaluations like `eval(unescape(...))`, Gson would fail to parse the invalid JSON syntax, causing the suffix rewriting (`tx` -> `tx_min`) to be silently skipped.
- **Fix:** Moved `rewriteQueryForHistory` to execute immediately **after** Rhino Javascript engine has resolved the timestamp variables. The rewriter now always works on clean, valid JSON.

## 2. Fixed Integer to Float Serialization Conversion (`QwQueryRewriter.java`)
- **Problem:** When Gson parsed the JSON query using a generic `Map.class`, all numeric integer fields (like `1` or timestamps) were converted to Java Doubles. Serializing the map back to JSON caused integers to have `.0` suffixes (e.g. `1.0`), which Quickwit rejected with `invalid type: floating point 1.0, expected u64`.
- **Fix:** Rewrote `rewriteQueryForHistory` and `rewriteAggsForHistory` to parse the JSON string as a `JsonElement` tree using `JsonParser` and work directly on `JsonObject`/`JsonArray`. Gson now preserves the original integer formats.

## 3. Simplified Database Views to Use Standard `Math`
- **Problem:** Previously, the views in PostgreSQL `maya_global_settings` used the `eval(unescape("%4d%61%74%68"))` hack to bypass Rhino classloader/conflict errors where `Math` was not defined.
- **Fix:** Since the classloader issues have been resolved, and Grafana Base32-encodes the queries before sending them to Trino (preventing Trino from lowercasing standard SQL identifiers), standard `Math.ceil` and `Math.floor` work perfectly inside Rhino! We updated 9 view configurations in the PostgreSQL database to use standard `Math` directly.

## 4. Successful End-to-End Execution
- Tested the history rollup query against Trino HTTP endpoint: finished successfully and returned **419 rows** of history rollup data.
- Tested Base32 encoded queries using clean `Math` expressions: finished successfully and returned **156 rows**.

## 5. Quickwit Response Flattener Fix (`QwUtil.java`)
- **Problem:** When JFlat flattened the nested Quickwit aggregations, sibling metric nodes on different paths were flattened into separate rows, causing key metrics to return as null.
- **Fix:** Replaced the legacy `flattenMap` with a recursive `flatten` method that computes the Cartesian product of nested buckets. This correctly merges sibling keys onto the same output rows.

## 6. Tuned No-Cache TTL (`ConnectorBaseUtil.java`)
- **Problem:** Queries with `cache=false` were cached in Redis with a 5-second TTL (`NONE_CACHE_TTL_IN_SECONDS`) to share data between Trino planning and execution. Under coordinator load or queue wait times exceeding 5s, the cache would expire before execution, causing a duplicate live query execution to Quickwit.
- **Fix:** Increased `NONE_CACHE_TTL_IN_SECONDS` to 10 seconds. This guarantees the execution phase always finds the cached metadata/data under typical planning and queue delays.

## 7. Helm Catalog Timeout Configurations (`helm_repo1`)
- **Problem:** Heavy Quickwit nested aggregation queries (like panel 36) take >10 seconds on cold runs, causing client read timeouts in Trino.
- **Fix:** Added default `connect-timeout=60`, `read-timeout=60`, and `write-timeout=60` to `quickwit.properties` in both `trino-single` and `trino-multi` helm charts. Patched the ConfigMap and rollout-restarted the Trino coordinator on the OGM cluster.

## 8. Fixed Duplicate Fallback Columns (`UlakQuickwitMetadata.java`)
- **Problem:** When live query returns no data (due to empty time range), the Quickwit connector falls back to the declared `columns` in the query comment. If a column is listed multiple times, it results in duplicate metadata properties, causing Trino planning phase to fail with `AMBIGUOUS_NAME` and `Multiple entries with same value`.
- **Fix:** Added deduplication logic in `getTableMetadata` and `getColumnsInternal` fallback paths using `list.stream().noneMatch(...)` based on case-insensitive names.

## 9. Auto-Corrected Dashboard Columns Typos (`QueryParameters.java`)
- **Problem:** In fallback mode (no live data), dashboard copy-paste typos like `/value1/9/key` (missing comma between `/value` and `1/9/key`) or `1/15/keym_overlay` (missing comma between `1/15/key` and `m_overlay`) omit referenced columns from the schema, throwing `COLUMN_NOT_FOUND` on empty panels.
- **Fix:** Added regex auto-correction rules inside `QueryParameters.java` TEXT_COLUMNS parsing to insert missing commas between word/digit and word/letter boundaries automatically.

## 10. Researched History Index Integration in Table Queries
- **Findings:**
  - In JSON/Metadata-based table queries (table names containing `//`), the history mechanism is fully integrated using the `//historyenabled` and `//historyindex` parameters.
  - When the query range exceeds `HISTORY_TIME_THRESHOLD_SECONDS` (1 hour), the connector dynamically switches the target index to the history index in `QwUtil.select` and rewrites the query aggregate fields in `QwUtil.executeOneQuery`.
  - In the new Plain Table Query Mode, the history index mechanism is not active by default as the parameters are not provided.

## 11. Traffic Statistics Sankey Panel src_ip Rollup Fix
- **Problem:** When `enable_history` was toggled on the **Traffic Statistics** dashboard, the Sankey panel at the bottom showed `N/A` for all source and destination IPs. This was because the query was routed to the history index `rollup_15m_site_app`, which only aggregates by `site_uuid` and `app`, omitting the `src_ip` and `dst_ip` columns.
- **Fix:** 
  1. Created a new Quickwit rollup index `rollup_15m_site_src_dst_ip` with custom schema mappings.
  2. Defined and registered a new rollup task `flow_rollup_15m_site_src_dst_ip` inside `tasks.json` that aggregates by `site_uuid`, `src_ip`, and `dst_ip` concurrently.
  3. Deployed the updated configuration via ConfigMap and rollout-restarted the Quickwit rollup engine.
  4. Updated the dashboard's Sankey panel target query in PostgreSQL to route history queries to `rollup_15m_site_src_dst_ip`.
- **Verification:** Ran test query via Trino and verified that it successfully returns correct source and destination IP pairs instead of `N/A`.

## 12. Hub Resource Utilization Dashboard Trino Migration
- **Problem:** The **Hub Resource Utilization** dashboard (`defsvlkcamuwwe`) queried metrics (CPU, RAM, DISK) using the native Quickwit datasource plugin (Lucene query syntax) rather than Trino SQL. This prevented it from utilizing the `enable_history` rollup variables.
- **Fix:**
  1. Created a complete dashboard backup at `scratch/dashboard_hub_res_util_backup.json`.
  2. Enabled the `enable_history` template variable visibility (`hide: 0`) in the UI.
  3. Converted all 5 resource panels to run direct SQL queries in Trino:
     - **CPU Utilization:** Moved the join with PostgreSQL `site` table directly into Trino SQL, calculating core values and returning timeseries columns. Cleared transformations and removed Target B. Set `hasjs=false` to bypass Rhino parsing.
     - **RAM Utilization:** Grouped directly in Trino SQL, using `CASE WHEN` to title-case component names (`Free`, `Used`, etc.) to match color overrides. Cleared transformations and set `hasjs=false`.
     - **DISK Usage:** Wrapped with an outer `group by component` to return exactly 3 clean rows (`Free`, `Used`, `Reserved`) without duplicates. Cleared transformations and set `hasjs=false`.
     - **Total RAM / Total DISK:** Performed SQL `SUM` aggregation over component averages in Trino SQL, returning `Total` bytes. Cleared transformations and set `hasjs=false`.
  4. Fixed a missing closing brace in the `Total DISK` query JSON block.
  5. Saved the clean SQL-grouped dashboard to PostgreSQL.
- **Verification:** Verified that `enable_history` successfully switches query routing between live `metrics3` and history `metrics3_15` indexes, returning correct and distinct CPU/RAM/Disk metrics.

## 13. Excluded Flow Suffixes from History Renaming
- **Problem:** When querying history indexes for flows, the query rewriter was renaming metrics like `u`, `ac`, etc. to `u_sum`, `ac_avg`, which did not match the actual schemas of the flow rollup indexes (e.g. `rollup_15m_site_src_ip`).
- **Fix:** Modified `QwQueryRewriter.java` to check for and exclude flow metrics (`u`, `ac`, `ab`, `t`, `u_ac`, `t_ab`) from being renamed with history suffixes.

## 14. Support history_index and enable_history in raw_query
- **Problem:** Previously, the `quickwit.system.raw_query` table function did not dynamically route queries to history rollup indexes when historical time ranges were queried.
- **Fix:** Added `enable_history` and `history_index` parameters directly into `RawQuery.java` and `RawQuickwitQueryTableHandle.java`, implementing the conditional routing and schema resolution logic inside `QwUtil.java`.

## 15. Excluded Default Value Metric Field from History Suffix Renaming
- **Problem:** Suffix renaming (`value` -> `value_avg`) broke historical resource utilization graphs because older rolled-up documents (before today at 08:00) stored the average directly in `value` and did not contain the `value_avg` field, returning null/empty graphs.
- **Fix:** Excluded `span_attributes.value` from suffix renaming in `QwQueryRewriter.java` so that both older and newer rollups resolve correctly using the common `value` field.



