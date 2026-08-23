# TODO.md — Next Steps and Open Actions

Completed work moved to [`DONE.md`](DONE.md). Connector-base cache/
architecture items tracked in
[`ulak-presto-connector-base/TODO.md`](ulak-presto-connector-base/TODO.md).

## Open — infra (OGM, `192.168.109.203`)

- [ ] **OGM control plane / `maya-quickwit` down since 2026-08-21 ~17:39,
      still down as of 2026-08-23.** Root cause: Longhorn PVC
      `pvc-b2109990-cf13-44dc-a997-d8d4c0c2f5d8` (quickwit's data volume)
      stuck exclusively attached to `ssb-sdwan-master` with no pod using
      it there; `maya-quickwit` pod rescheduled to `ssb-sdwan-worker1`
      and can never mount it (`Multi-Attach error`). SSH + kubectl to
      `ssb-sdwan-master` are themselves intermittently/persistently
      unreachable (banner-exchange timeouts, then outright connection
      refused on :6443) — this looks like more than just the stuck PVC;
      needs someone with console/physical access to the node. Once
      reachable again: check the Longhorn volume CR
      (`kubectl get volumes.longhorn.io -n longhorn-system`, or the
      Longhorn UI) for the real attach state, clear the stale attachment
      to `ssb-sdwan-master`, then let the quickwit pod reschedule/mount
      on `ssb-sdwan-worker1`.
- [ ] **`.gitignore` gap for `trino/etc/catalog/maya_*.properties` +
      `mayapostgres.properties`.** These newer catalog config files
      (contain live pg-connection-password) aren't covered by the
      `.gitignore` rule that only lists the older `quickwit.properties`/
      `tenant.properties` names — one already got committed with a real
      password (commit `eea9592`, already pushed to `gitlab_mfyuce`).
      User confirmed the leaked password is a temporary/throwaway
      credential, no rotation needed right now — but the gap itself
      should still be closed (extend `.gitignore`, `git rm --cached`,
      add `.example` templates, matching the R21 pattern) before the
      *next* real secret lands there.

## Open — feature parity

- [ ] **`yucemonitoring`'s `data-gen` was not migrated** to the
      multiprocessing/deadlock-fixed cyclic script (see DONE.md,
      2026-08-21 session). It still runs an older, different (single-
      process, non-cyclic) script with only 4 real sites — no known
      throughput problem there today, so left untouched. Revisit if/when
      `yucemonitoring` needs the same NORMAL/ANOMALY cyclic behavior as
      OGM.
- [ ] **`UlakQuickwitMetadata` empty-result → `COLUMN_NOT_FOUND` instead
      of "no data".** When a sample/schema-inference query legitimately
      returns 0 rows (e.g. a filtered dropdown value with no historical
      match) and no `//columns=` is declared, Trino throws a hard SQL
      error instead of rendering an empty panel. Proposed fix (fall back
      to a static known column list) was discussed but never
      implemented/approved.
- [ ] **`maya_ifstatus` Redis cache footprint (~80MB observed)** — see
      connector-base TODO C03. Idle-eviction (C02) should shrink this
      over time for abandoned entries; root cause of the size itself is
      still unexplained.

## Open — dashboards (carried over, not reverified this session)

- [ ] **`_rustrino` Grafana macros missing `//historyenabled=`.**
      Discovered 2026-08-20: macros ending in `_rustrino` (e.g.
      `view_interface_with_site_filter_rustrino`, used by the Hub Network
      Throughput dashboard) call `qw_agg('metrics3', ...)` via the
      `rustrino-pg` Postgres datasource without a `//historyenabled=`/
      `//historyindex=` header, so long time-range queries never route to
      the history index and can time out. Not touched since discovery —
      needs the `maya_global_settings` macro definitions updated, or the
      `qw_agg` plugin changed to append the header automatically.
