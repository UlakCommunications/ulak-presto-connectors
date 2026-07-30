# TODO.md — Next Steps and Open Actions

## Current State
- The Quickwit connector has been updated to dynamically parse a `//nohistorysuffix=` comment to exclude fields from metric suffix renaming during history routing.
- The `netlink_${ROLLUP_INTERVAL}m` task in `tasks.json` has been updated with `m_site_id`, `m_name`, and `m_site_name` dimensions. Rollup data has been backfilled from today 00:00.
- Dashboard panel typos (`columns1` -> `columns`) were fixed on the QoS dashboard while preserving the parent folder structure.
- Build #374 on Jenkins compiled and pushed the updated connector image successfully. The Trino coordinator deployment has been restarted.

## Next Steps
1. **Verify Grafana Quality of Service Dashboard**:
   - Ask the user to load the QoS dashboard with `enable_history` set to true (time range `now-6h` to `now`).
   - Verify that data renders successfully on the graphs, and no query errors are thrown.
2. **Monitor Trino Logs**:
   - Monitor Trino coordinator logs for any unexpected query rewriter failures:
     `kubectl --kubeconfig /tmp/vc-yucemonitoring.config -n yucemonitoring logs -l app=trino,component=coordinator --tail=100 -f`
3. **Convention for Future Dashboards**:
   - For any future dashboard utilizing rollup metrics that should *not* have history suffixes appended (such as flow metrics), ensure `//nohistorysuffix=metric1,metric2` is declared in the panel's target SQL comments.
