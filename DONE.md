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
