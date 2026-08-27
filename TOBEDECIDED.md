# TOBEDECIDED — Open Decisions and Architectural Questions

## Open Decisions

- **OGM Redis-pool-exhaustion fix priority.** Root cause has two independent
  contributors (see CLAUDE.md/TODO.md, diagnosed 2026-08-26): Quickwit's
  node-pinning/memory-pressure on `ssb-sdwan-master`, and
  `ConnectorBaseUtil.select()` holding its Jedis connection across the
  whole downstream fetch instead of just the cache calls. Needs a decision
  on whether to do both, and which first — the connector-side fix is a
  shared-code change (affects every catalog using `ConnectorBaseUtil`, not
  just OGM), while the Quickwit node-affinity fix is OGM-infrastructure-only.
- **`qw-rollup-engine` backward backfill — deploy now or wait?** Implemented
  and live-tested 2026-08-26 (see DONE.md), committed and pushed, but not
  built into a Docker image or rolled out. Needs a decision on which
  cluster(s) (OGM, yucemonitoring, or both) and when — unlike most fixes in
  this repo, this one only changes behavior for *new* tasks (checkpoint
  still zero), so it's low-risk to already-running tasks, but the actual
  Docker build/push/rollout step still needs sign-off.
- **`ROLLUP_RETENTION` duration vs. `qwdata` capacity (90GB).** Measured
  2026-08-26: all 6 rollup indexes combined cost ~3.6-4GB/day at real,
  full-753-site density. The live `ROLLUP_RETENTION=1 month` setting
  would need ~107-120GB once the indexes actually hold a full month at
  that density — more than the 90GB volume has. Options: shorten the
  retention window, expand the volume again, or accept it self-limits via
  DiskPressure (not recommended — see DONE.md's earlier 30Gi→100Gi
  incident). Same finding kills the old "~14.4GB for a 2-year backfill"
  estimate (that was low-density synthetic filler, not real density) —
  see TODO.md's 2-year-backfill item. Not urgent yet (live accumulation
  is nowhere near a month deep), but needs a real answer before it is.

## Architecture Notes for Future Reference
- **Resource Groups Concurrency Limits:** Keep an eye on Trino coordinator queue/waiting time under high query concurrency. If needed, customize `resource-groups.properties` on the Trino coordinator.
- **Plain Table Query Mode vs. History Index:** Currently, plain table queries (e.g. `SELECT * FROM quickwit.default_schema.metrics3`) do not auto-enable `historyenabled` or switch to `historyindex`. If required, dynamic history routing can be enabled via session properties or table functions (`raw_query`).
