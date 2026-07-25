# TODO.md — Next Steps and Open Actions

## Current State
- The Quickwit connector has been fully updated to resolve `AMBIGUOUS_NAME` and `COLUMN_NOT_FOUND` issues when queries yield no matching documents (fallback mode).
- Duplicate column handles are deduplicated in metadata resolving (`getTableMetadata`, `getColumnsInternal`).
- The `//columns` CSV parser in `QueryParameters` now auto-corrects typos (missing commas) between key/value and adjacent columns.
- The new connector image (`0.0.1-develop-latest`) has been compiled, packaged, pushed, and Trino coordinator restarted.
- End-to-end verification via statement query scripts succeeded.

## Next Steps for the Next Session
1. **User/Grafana Verification**:
   - Ask the user to open the Quality of Service dashboard at https://192.168.109.206/grafana/d/cez1fkculu1hca/quality-of-service?orgId=1&refresh=1m and verify that all panels load successfully (displaying data or 0 rows/empty panels without errors).
2. **Monitor Logs**:
   - Monitor Trino coordinator logs during user verification:
     `KUBECONFIG=/home/fatihyuce/.kube/config-ogm-demo kubectl logs -n maya3 -l app=trino,component=coordinator --tail=100 -f`
3. **Database Settings Check**:
   - In case any other views show syntax or runtime evaluation errors, check `public.maya_global_settings` table in PostgreSQL. A complete backup of the views prior to the `Math` conversion is saved at:
     `/home/fatihyuce/.gemini/antigravity/brain/4837bf22-a4cb-4684-8f23-011b35ab0895/scratch/original_views_backup.json`
     To restore original views, run:
     `python3 /home/fatihyuce/.gemini/antigravity/brain/4837bf22-a4cb-4684-8f23-011b35ab0895/scratch/restore_views_from_backup.py`

4. **Resource Utilization History Rollups**:
   - Currently, CPU, RAM (`memory`), and Disk (`df`) metrics are successfully rolled up into the `metrics3_15` index.
   - However, the **Hub Resource Utilization** dashboard (`defsvlkcamuwwe`) queries these metrics directly using the Grafana Quickwit plugin (Lucene query syntax) rather than Trino SQL. This prevents it from supporting the `enable_history` / `history_index` variables.
   - **Action Item:** Convert these panel targets to Trino SQL queries (using views or direct SQL on `quickwit.otlp_metric`) so they can utilize the `enable_history` variable and pull from `metrics3_15` when history is enabled.
