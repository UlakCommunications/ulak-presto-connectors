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
- **`ROLLUP_RETENTION` duration vs. `qwdata` capacity (90GB), and whether it
  should differ per tier.** Measured 2026-08-26: all 6 15m rollup indexes
  combined cost ~3.6-4GB/day at real, full-753-site density. The live
  `ROLLUP_RETENTION=1 month` setting would need ~107-120GB once the
  indexes actually hold a full month at that density — more than the 90GB
  volume has. Options: shorten the retention window, expand the volume
  again, or accept it self-limits via DiskPressure (not recommended — see
  DONE.md's earlier 30Gi→100Gi incident). Same finding kills the old
  "~14.4GB for a 2-year backfill" estimate (that was low-density synthetic
  filler, not real density) — see TODO.md's 2-year-backfill item.
  **New 2026-08-27 wrinkle:** user separately asked for 60m rollups to have
  their *own*, shorter retention (1 week) — `pg_works.py` has no per-tier
  differentiation mechanism at all today (one blanket value for every
  matched index, see TODO.md). 60m's per-day cost is much smaller than
  15m's (the 7-day OGM demo backfill put all 6 60m indexes combined at
  ~83M docs / roughly comparable order of magnitude to a couple of days of
  15m data, not a capacity-mover on its own) — so the per-tier ask is
  really about *intent* (60m is meant as a short-lived demo/recent-trend
  tier, not long-term storage) more than about the capacity math above.
  Worth deciding both together: what should 15m's real retention be (not
  just what it happens to be set to), and does 60m getting its own value
  change that number at all. Not urgent — live accumulation is nowhere
  near a month deep on either cluster — but needs a real answer before it
  is, and before someone builds the per-tier code without a target number.

## Architecture Notes for Future Reference
- **Resource Groups Concurrency Limits:** Keep an eye on Trino coordinator queue/waiting time under high query concurrency. If needed, customize `resource-groups.properties` on the Trino coordinator.
- **Plain Table Query Mode vs. History Index:** Currently, plain table queries (e.g. `SELECT * FROM quickwit.default_schema.metrics3`) do not auto-enable `historyenabled` or switch to `historyindex`. If required, dynamic history routing can be enabled via session properties or table functions (`raw_query`).
