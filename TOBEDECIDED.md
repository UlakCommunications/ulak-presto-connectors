# TOBEDECIDED — Open Decisions and Architectural Questions

## Open Decisions

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
