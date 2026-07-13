# TOBEDECIDED — Open Decisions and Questions

There are no critical open decisions or pending questions left from this session.

## Notes for Future Reference
- **Resource Groups Concurrency Limits:** Keep an eye on Trino coordinator's queue/waiting time under high load. If concurrency becomes a bottleneck, a custom Resource Group schema can be configured in `resource-groups.properties` on the Trino coordinator.
- **Rollup Index Migration:** If query execution latency on raw `flows3` remains high (3-4s) despite cache hits, migrate the dashboard panels to use pre-aggregated rollup indexes.
