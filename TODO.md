# TODO.md — Next Steps and Open Actions

Completed work moved to [`DONE.md`](DONE.md). Connector-base cache/
architecture items tracked in
[`ulak-presto-connector-base/TODO.md`](ulak-presto-connector-base/TODO.md).

## Open — infra (OGM, `192.168.109.203`)

- [ ] **`ssb-sdwan-master` pod containers run ~6.5 minutes behind the
      node's real clock.** Found 2026-08-25 while debugging why a freshly
      redeployed `qw-rollup-engine` pod appeared to hang with zero
      progress for minutes (`kubectl exec ... date -u` vs the node's own
      `date -u` showed the gap directly; confirmed on two separate fresh
      pods on that node). Not a `qw-rollup-engine` bug — just adds a real
      ~6.5min lag to anything on that node computing "is this due yet"
      off its own clock. yucemonitoring's nodes have no such skew. Worth
      an NTP check on `ssb-sdwan-master` specifically; not blocking
      anything today.
- [ ] **`qw-rollup-engine`'s `flow_rollup_15m_site_src_dst_ip`
      `host_prefix_chars` fix (2026-08-25, see DONE.md) is deployed and
      verified on both OGM and yucemonitoring, but no other flow_rollup/
      metric tasks were audited for the same class of bucket-limit-
      overflow risk** — only netlink (known, ~35x over) and this one
      (found by accident once it started erroring loudly instead of
      silently, ~1.17x over) got the generic fix. Worth a deliberate
      sweep of all `qw-rollup-engine` tasks' top-level `terms(size:9999)`
      cardinality against the 65,000 bucket cap rather than waiting for
      each one to fail into visibility one at a time.
- [ ] **`ssb-sdwan-master` has no outbound internet path.** `ping 8.8.8.8`
      and `docker.io`/`gcr.io` DNS resolution both fail past the node's
      own default gateway (`192.168.109.254`), which itself responds
      fine — this is a network/firewall-level restriction (source-IP
      block on the gateway, most likely), not something fixable from the
      node itself. Root-caused 2026-08-23/24 while chasing a wave of
      `ImagePullBackOff` pods (Longhorn CSI sidecars, `cattle-cluster-agent`,
      `kubegres-controller-manager`) — worked around by mirroring the
      specific needed images into the local Nexus registry
      (`maya-nexus:35000`, see DONE.md), but that's per-image, not a fix.
      Needs someone with access to the `.254` gateway/firewall to check
      why master's source IP can't reach the internet while the worker
      nodes can.
- [ ] **Possible dev-vs-OGM gap: `anomaly-events-*` alert rules paused,
      root cause (missing Quickwit indexes) never actually fixed.** Found
      2026-08-24 while answering a user question about OGM-only dashboard
      fixes — `machine_ops/OGM_DEMO_INSTALL_DONE.md` section 11-G
      (an earlier, already-"Tamamlandı" session, not this one) documents
      that OGM never had the `anomaly-events-metrics`/`-gateway`/`-cpe`/
      `anomaly-events` Quickwit indexes, so Grafana's alert scheduler hit
      404s every 10s with 3x retries, loading Trino's connection pool and
      Grafana's worker threads. Rather than creating the indexes, the
      matching Grafana alert rules were paused via the provisioning API
      (`isPaused: true`) as a stopgap "until the indexes are created" —
      that follow-up apparently never happened. Unknown: whether `dev`
      has these indexes (if not, it may have the same unpaused alert
      noise OGM had) or whether this is purely an OGM synthetic-data gap.
      Needs someone with dev-environment access to check index existence
      + alert-rule pause state there before deciding whether to create
      the indexes on OGM, unpause anything, or leave as-is.
      **Update 2026-08-24 (later same session), root-caused while
      diagnosing "why is the alert-map dashboard slow":** checked live —
      all 5 `anomaly|cpe|gateway` rules (`critical_flow_anomaly`,
      `sustained_flow_anomaly`, `critical_metrics_anomaly` @ 1m;
      `pipeline_silent_gateway`, `pipeline_silent_cpe` @ 5m) currently
      show `isPaused=false` via `/api/v1/provisioning/alert-rules` — the
      stopgap pause from the earlier session is **not** in effect now
      (unpaused since, or never actually applied to these UIDs). They're
      no longer hitting the old 404-missing-index error either — now
      failing with `quickwit.javaclient.ApiException: Quickwit 500:
      tantivy error: Aborting aggregation because bucket limit was
      exceeded. Limit: 65000, Current: ~155000` from
      `UlakQuickwitMetadata.getColumnsInternal` against
      `quickwit.metrics3`, at a sustained ~1/sec (335 of 335+91 queries
      in a 5-min `system.runtime.queries` sample were `FAILED
      GENERIC_INTERNAL_ERROR`, each burning ~2-3.5s of coordinator
      dispatcher time). Almost certainly triggered by today's 1-month/
      753-site synthetic rollup backfill (see the 2-year-backfill item
      below) pushing whatever field these rules group by (host/iface/
      datasource-shaped, matching `view_alerts_trino`'s label set) past
      Quickwit's default 65k aggregation bucket cap. This churn is
      loading the shared Trino coordinator enough to add ~1s of
      queueing delay to otherwise-fast queries (`FINISHED` queries in
      the same sample showed ~1000ms `waiting`/`scheduling` vs ~20-40ms
      actual `running`) — a direct, currently-live contributor to the
      alert-map (and likely other dashboards') sluggishness. Fix options
      (none applied yet, needs a decision): re-pause these 5 rules again
      via the provisioning API (fast stopgap, same as before); raise
      Quickwit's aggregation bucket limit
      (`aggregation.max_terms_aggregation_buckets`-style Quickwit
      config, needs checking exact key for this version); or narrow the
      rules' queries so they don't need a 155k-bucket terms aggregation.
      Separately, while investigating this, confirmed the RedisCacheWorker
      multi-catalog bug from `CLAUDE.md`/commit `eea9592` ("cache fixes")
      is **still live on OGM** — `RedisCacheWorkerItem` background thread
      is repeatedly failing on the "Data Plane Status Sites And Overlays"
      cached query (`view_tenant_host_overlay_iface` in
      `maya_global_settings`, which feeds the alert-map dashboard's
      "Alarm Status Map" panel directly) with the exact documented
      symptom — `relation "public.site_temp_version_state" does not
      exist`. Since that fix already exists in `develop` (commit
      `eea9592`), this means **OGM's Trino image predates that commit
      and hasn't been rebuilt/redeployed since** — a separate action
      item from the alert-rule issue above. (Confirmed, while there: the
      session-scoped `WITH tv_json AS MATERIALIZED (...)` CTE the user
      recalled adding is present and correct in
      `view_tenant_host_overlay_iface` — that part is not the problem.)

      **Resolution 2026-08-24 (same session):** the actual dominant
      failure source turned out to be **`tx_maya_link_utilization` /
      `rx_maya_link_utilization`**, not the anomaly/cpe/gateway family —
      pausing those 5 (then user paused 2 more, `pipeline_silent_flow`/
      `pipeline_silent_metrics`, via UI) barely moved the FAILED rate
      (still 241/5min after). Cross-checked all 13 alert rules using
      `raw_query(...)` against any Quickwit index (not just `metrics3`):
      10 already paused (the whole `anomaly-events*`/`anomaly-model-stats`
      family), 3 live — `ntp_out_of_sync` (no `size=` terms aggs, not a
      risk), and `tx_`/`rx_maya_link_utilization` (4-5x `size=9999`
      nested `terms()` inside a `histogram()` each — confirmed via full
      query text capture from `system.runtime.queries`, this is the
      bucket-explosion source). Separately, the `alert_rule`/
      `alert_rule_version` desync wasn't just the one rule the user
      first hit (`cf910a6d-...`) — **26 rules** had `alert_rule.version`
      behind `alert_rule_version`'s max (gaps of 1-48), from the
      `REFRESH_DASHBOARD=1` bulk SQL reimport earlier in the session.
      User ran a targeted `UPDATE alert_rule ... SET version = mv.max_version`
      (synced to history max per `rule_uid`/`rule_org_id`) — confirmed
      0 rules behind afterward, and pause/edit started working again via
      UI (was 403 for my service-account token via the provisioning API
      the whole time — that permission gap is still unresolved, only
      worked around by the user doing it via UI). User then applied some
      fix for `tx_`/`rx_maya_link_utilization` — **mechanism unconfirmed**,
      `alert_rule.data` still shows `size=9999` and `is_paused=false` for
      both, so it wasn't a query edit or a pause; verified empirically
      instead: 0 `FAILED` queries and 0 matching error log lines over a
      90s window post-fix (was ~1/sec sustained before). Declared fixed
      by user call, but since the rule definitions are unchanged the
      same bucket-explosion could resurface (e.g. after any process
      restart that resets whatever was actually changed) — worth a
      real fix (lower the `size=9999` values, or raise Quickwit's
      aggregation bucket limit) rather than relying on whatever this was.
      **Resolved 2026-08-25:** the RedisCacheWorker `site_temp_version_state`
      failure was root-caused (a `services.public.` vs `public.` prefix bug
      in a `maya_global_settings` SQL-template row, nothing to do with
      `eea9592`/`connectionId`) and fixed on both OGM and yucemonitoring —
      full detail in DONE.md ("Session 25 August 2026", item 2). The
      Redis-keyspace audit done alongside it found nothing else actionable
      (32 keys, all legitimate infra, no long-lived stray cache entries).
- [ ] **Plaintext Grafana service-account bearer token in `machine_ops/
      OGM_DEMO_INSTALL_DONE.md`** (section 11-G, `glsa_...` prefix — real
      token format). Found 2026-08-24 while reviewing that doc, not
      rotated or redacted — flagged to user, no action taken yet. Same
      pattern as the `eea9592` Postgres password leak already tracked
      below: confirm whether it's still live/needs rotation, then redact
      from the doc regardless.
- [ ] **`cattle-cluster-agent` (Rancher connection) may drift back to the
      broken unmirrored image.** Rancher's own reconciliation reverted the
      Nexus-mirrored image back to `rancher/rancher-agent:v2.12.1`
      (unreachable, see above) about 5 minutes after it was first fixed
      2026-08-24 — had to re-patch once already. If the cluster shows as
      disconnected in Rancher UI again, re-run:
      `kubectl set image deployment -n cattle-system cattle-cluster-agent
      cluster-register=maya-nexus:35000/rancher/rancher-agent:v2.12.1`.
      A durable fix would be a transparent containerd registry mirror for
      `docker.io` (and `gcr.io`) itself, not just `maya-nexus:35000` —
      not done this session.
- [ ] **Master disk at 48% (71GB free of 142GB) as of 2026-08-24 post-
      recovery — no headroom strategy still exists.** Nexus's own data
      (`/mnt/nexus-data`) has no cleanup/retention policy and will keep
      growing with every future image push. More importantly: this
      session had **two** separate DiskPressure incidents from disk-usage
      surprises (first from Nexus image mirroring, second much more
      severe from a 30Gi→100Gi Longhorn PVC expansion — see DONE.md #18,
      which ended in deleting and rebuilding `qwdata-pv` from scratch at
      a safer 30Gi). Worth either configuring Nexus blob-store cleanup
      policies or watching disk usage before the next round of image
      mirroring, **and treating any future Longhorn volume resize on this
      node as high-risk** — go up in small increments only, since
      `storageScheduled` reserves the full nominal size immediately (not
      actual usage) and resize auto-creates a snapshot, both with an
      immediate real disk cost.
- [ ] **Any future Longhorn DaemonSet image mirroring must update the
      matching `--<component>-image` CLI flag, not just `image:`.**
      Root-caused 2026-08-24 (DONE.md #20): `longhorn-manager`'s
      `command:` list carries its own `--manager-image`,
      `--engine-image`, `--instance-manager-image`,
      `--share-manager-image`, `--backing-image-manager-image`, and
      `--support-bundle-manager-image` flags, separate from the
      container's `image:` field. Longhorn's internal upgrade-path check
      (`isOldManagerPod` in `upgrade/upgrade.go`) compares the running
      pod's `image:` against `--manager-image` specifically — mirroring
      `longhorn-manager`'s own image to Nexus via `kubectl set image`
      without also patching `--manager-image` caused every replica to
      treat all manager pods (including itself) as permanently "old",
      deadlocking instance-manager creation cluster-wide for hours. Fixed
      that one instance via `kubectl patch daemonset longhorn-manager
      --type=json` on the specific `command` array index. If
      `--engine-image` etc. are ever similarly redirected to Nexus
      without also updating the flag, expect the same class of failure
      (unconfirmed whether the other five flags feed a similar
      self-check — not hit yet, but treat as a live risk).
- [ ] **`otelcontribcol` has no `memory_limiter` processor.** The collector
      config (`/app/otel/collector-config.yaml`, ConfigMap-backed) has no
      `memory_limiter` in any pipeline — the only guard against runaway
      memory is the container's own resource limit (see DONE.md #3, #10),
      which just gets the pod OOM-killed rather than gracefully shedding
      load. `sending_queue.queue_size: 100000` per exporter is set on
      `otlp`/`otlp/flow`/`otlp/metric` but not on `otlp/apigw`, and even
      100000 is high enough it likely never binds before OOM does. Add
      `memory_limiter` (e.g. `limit_mib` a few GB under the container's
      current 15000Mi cap) as the first processor in every pipeline, and
      add a `sending_queue` block to `otlp/apigw` for consistency.
      Deprioritized 2026-08-24 once throttling `data-gen` at the source
      (see DONE.md #9) proved sufficient — revisit if OOMs recur.
- [ ] **No dashboard/alerting on otel's own dropped/failed data.** The
      collector already exposes self-telemetry on `:8888/metrics`
      (`otelcol_exporter_send_failed_spans`, `otelcol_exporter_enqueue_failed_*`,
      `otelcol_exporter_queue_size` vs `_capacity`) but nothing scrapes or
      graphs it — found by `kubectl exec ... curl localhost:8888/metrics`
      one-off during the 2026-08-24 session (see DONE.md #8), which is how
      the ~49%/77% metrics/flow failure rate during the outage was
      quantified after the fact. Wire a ServiceMonitor/scrape config (the
      cluster already runs `rancher-monitoring-prometheus`) plus a Grafana
      panel so this is visible going forward instead of only discoverable
      after the fact.
- [ ] **`monitoring-jobs` image needs a rebuild+push to ship the
      `ROLLUP_RETENTION` fix.** `pg_works.py`'s rollup-index retention
      (see DONE.md #15) is code-complete and the env var is wired through
      Helm (`helm_repo1/postgres-single`) and already set live on
      `maya-postgres-single-cronjob`, but the code itself won't run until
      a new `monitoring-jobs` image ships — it's baked into the image, not
      ConfigMap-mounted like `data-gen`. `monitoring_temp/.../grafana_init/
      push.sh` defaults to `192.168.57.202:35000`, a *different* registry
      than the one OGM actually pulls from (`maya-nexus:35000` →
      `192.168.109.203:35000`) — confirm the right registry/invocation
      (or find the Jenkins job used for the `3.1.4-20260810-OGM`-tagged
      builds referenced in `machine_ops/OGM_DEMO_INSTALL_DONE.md`) before
      building, rather than guessing and possibly pushing somewhere OGM
      never pulls from.
- [ ] **Flow/metric ratio still well short of the ~3:1 target.** User
      wants flow generation ≈ 3× metric generation in steady state;
      current live setting (`FLOW_MULTIPLIER=15`, `INTERVAL=20`, see
      DONE.md #13) measures ~0.06 (metric still ~17× flow). Reaching 3:1
      needs `FLOW_MULTIPLIER≈735` from today's per-site interface-pair
      math — user deliberately chose to step up gradually and watch
      system load rather than jump straight there. Next step: once the
      current setting is confirmed stable for a while, raise
      `FLOW_MULTIPLIER` further (re-measure via two `:8888/metrics`
      snapshots on `maya-monitoring-config-management` each time, same
      method used this session) and watch quickwit/otel CPU, memory, and
      restart counts as it climbs toward ~735.
- [ ] **2-year rollup backfill — parked at 1 month for now, by user
      choice, not a technical blocker.** Re-ran the backfill 2026-08-24
      after fixing an unrelated metastore desync (DONE.md #22) — this
      time cleanly for all 753 real sites × 30 days: 6.5M docs, ~599MB
      total on `qwdata` (see DONE.md #23). User explicitly decided 1
      month across all sites is sufficient for now and stopped there,
      rather than continuing to the full 2-year scope (extrapolated
      ~14.4GB for `metrics3_15` + `rollup_15m_site_app` combined — safe
      headroom-wise against the 30Gi volume, and the user separately
      confirmed `qwdata` can go to 40Gi if needed, but a meaningful
      enough chunk that it wasn't worth doing speculatively). If 2 years
      is wanted later: the same script
      (`/tmp/claude-1000/.../scratchpad/rollup_backfill_pilot.py` this
      session — still only in a scratchpad, not committed anywhere
      durable) already fetches the live 753-site list from Postgres and
      just needs `--days 730` instead of `--days 30`; consider committing
      it into the repo first given it's now been run successfully twice.
      `ROLLUP_RETENTION=2 years` (see above item) only matters once the
      indexes actually *hold* close to 2 years of data — at 1 month of
      backfill plus whatever the live rollup engine accumulates going
      forward, that's still a long way off.
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
- [x] **`IllegalStateException: Current transaction already committed`
      in OGM coordinator logs (2026-08-25) — investigated, benign,
      Trino-core, not actionable here.** `io.trino.execution.
      QueryStateMachine` logs `Error collecting query catalog metadata
      metrics: <queryId>` with this exception from
      `io.trino.transaction.InMemoryTransactionManager$TransactionMetadata
      .checkOpenTransaction` — a race in Trino 479's own post-completion
      metrics collection running after the transaction's already been
      committed/removed. Entirely `io.trino.*`/`io.trino.$gen.Trino_479...`
      stack, nothing in this repo's connectors. Confirmed harmless:
      cross-checked 3 affected query IDs against
      `system.runtime.queries` — all `FINISHED`, `error_type` empty, so
      clients get correct results; only the internal telemetry step
      fails. Frequency ~5% of queries (23/464 in a 15-minute sample) —
      log noise, not a functional issue. No fix available without a
      Trino version upgrade (out of scope); not worth chasing further
      unless it starts actually failing queries.

## Open — dashboards (carried over, not reverified this session)

- [ ] **`view_interface`'s stale `//columns=` fix (Hub Network Throughput
      panel, see DONE.md item 3) only applied to OGM.** Same
      `maya_global_settings` row/content on yucemonitoring almost
      certainly has the identical bug — not yet checked or fixed there.
- [ ] **A `general-purpose` agent's yucemonitoring stale-`//columns=`
      audit (18 candidate SQL fixes + 5 dashboard fixes, prepared
      2026-08-25) is sitting unreviewed and unapplied.** Classifier-
      blocked from running directly; output left on disk in the agent's
      own scratchpad (`yucemonitoring_columns_fixes.sql`,
      `apply_dashboard_fixes.sh`) but not yet reviewed for correctness or
      handed to the user to run. Needs a review pass before trusting it
      wholesale — it was generated, not verified against live data the
      way the OGM `view_interface`/`view_tenant_host_overlay_iface` fixes
      were.
- [ ] **`_rustrino` Grafana macros missing `//historyenabled=`.**
      Discovered 2026-08-20: macros ending in `_rustrino` (e.g.
      `view_interface_with_site_filter_rustrino`, used by the Hub Network
      Throughput dashboard) call `qw_agg('metrics3', ...)` via the
      `rustrino-pg` Postgres datasource without a `//historyenabled=`/
      `//historyindex=` header, so long time-range queries never route to
      the history index and can time out. Not touched since discovery —
      needs the `maya_global_settings` macro definitions updated, or the
      `qw_agg` plugin changed to append the header automatically.
