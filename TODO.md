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
