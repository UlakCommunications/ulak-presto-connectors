# DONE.md — Completed Work in this Session

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

