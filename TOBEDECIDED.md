# TOBEDECIDED — Open Decisions and Questions

There are no critical open decisions or pending questions left from this session.

## Notes for Future Reference
- **Resource Groups Concurrency Limits:** Keep an eye on Trino coordinator's queue/waiting time under high load. If concurrency becomes a bottleneck, a custom Resource Group schema can be configured in `resource-groups.properties` on the Trino coordinator.
- **Rollup Index Migration:** If query execution latency on raw `flows3` remains high (3-4s) despite cache hits, migrate the dashboard panels to use pre-aggregated rollup indexes.
- **Plain Table Query Mode vs. History Index:** Currently, plain table queries (e.g. `SELECT * FROM quickwit.public.metrics3`) do not auto-enable `historyenabled` or switch to `historyindex`. If this behavior is required, we need to design a way to pass these configurations in plain table mode (e.g., via session properties, table functions, or catalog defaults).

