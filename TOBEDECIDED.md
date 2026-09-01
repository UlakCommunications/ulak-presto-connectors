# TOBEDECIDED — Open Decisions and Architectural Questions

## Open Decisions

- **qw-rollup-engine bucket-limit-exceeded fix: `host_prefix_chars` vs.
  cascading rollups vs. raising Quickwit's limit?** 2026-09-01:
  `maya_ifstatus_15m`/`_60m` and `flow_rollup_60m_site_ip_proto` fail
  backward-backfill windows on Quickwit's `aggregation_bucket_limit`
  (default 65000, ~77-88k actual) — confirmed baked into OGM's Quickwit
  image, not ConfigMap-adjustable. Three options, not compared against each
  other in detail: (a) turn on `host_prefix_chars` for the 2-3 failing
  tasks — cheapest, already-built/tested mechanism (and, as of today,
  correctly bound by `max_concurrent_tasks` — see DONE.md), pure
  `tasks.json` change; (b) cascade 60m (and other coarse intervals) from an
  already-rolled-up finer index instead of querying raw each time — real
  complication: 19 `avg` metrics in `tasks.json` don't cascade via simple
  re-averaging (avg-of-avg is wrong unless sample counts match across the
  sub-windows), which is exactly why the original 60m implementation
  (`1b79c85`) deliberately stayed raw-only; correct cascading needs either a
  write-schema change (store `sum`+`count`, divide at read time — has
  downstream blast radius on whatever reads the `_avg` fields today) or an
  accepted-approximation via bucket `doc_count` weighting; (c) raise
  `aggregation_bucket_limit` itself — fixes every task at the root, but
  needs a Quickwit image rebuild (not a config patch) and a considered
  value against the paired `aggregation_memory_limit` (OOM risk, same class
  of problem the 60m-disable mitigation below was responding to).
  **Session ended mid-discussion, before any of these were chosen or ruled
  out.** In the meantime, all 60m tasks were disabled entirely
  (`tasks.json`, not yet deployed) as a blunter, faster mitigation for the
  OOM risk — see TODO.md 🔴 and DONE.md 2026-09-01.
- **OGM containerd/docker `docker.io`/`registry.k8s.io` mirror — add it, or
  keep mirroring per-image via skopeo?** Confirmed 2026-08-28 (twice, on a
  fresh incident): no OGM node has real internet at all (not just master
  as previously thought), and none has a mirror for any bare-registry
  image. The per-image workaround is now cheaper than it used to be —
  `skopeo copy` straight from Claude's own sandbox (has internet) to
  `maya-nexus:35000`, no OGM node involved — but it's still reactive,
  one image at a time as each one breaks. Worth a real decision on whether
  a proactive `registry.mirrors` config entry is worth doing once things
  are calmer, vs. continuing to firefight per-image with the now-easier
  skopeo path. Also still unresolved: whether Nexus's `admin`/`nexus2025!`
  basic-auth credential (still in active use) needs rotating.
- **`metrics3_60` backfill scope — full 7-day/~231M-doc run, or a smaller
  template window?** 2026-08-28: user wants `metrics3_60` (only ~7h of
  real data) backfilled from a `metrics3_15` template (has ~34h), tile-
  shifted backward site-by-site sequentially (deliberately no concurrency,
  learned from the incident this exact class of job caused earlier the
  same day — see DONE.md). Measured before running anything: a 1-hour
  template window = 1,378,156 docs; a full 7-day/168-shift run at that
  rate = ~231M docs, ~60x `metrics3_60`'s current size, likely many hours
  fully sequential. Options: (a) run the full scope anyway, slow but
  matches the original ask, or (b) shrink the template window (e.g. 15min
  instead of 1h) to cut total volume ~4x, faster but less representative
  per synthetic hour. **Conversation ended before this was decided** —
  first thing to resolve next session, see TODO.md 🔴.

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
