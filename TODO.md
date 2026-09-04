# TODO.md — Next Steps and Open Actions

Completed work moved to [`DONE.md`](DONE.md). Connector-base cache/
architecture items tracked in
[`ulak-presto-connector-base/TODO.md`](ulak-presto-connector-base/TODO.md).

## 🔴 FIRST DECISION NEEDED — qw-rollup-engine bucket-limit root cause (2026-09-01)

- [ ] **Pick how to actually fix `aggregation_bucket_limit` exceeded (not just
      the 60m shed below) for `maya_ifstatus`/`flow_rollup_*_ip_proto`.**
      Three options surfaced, none chosen — **conversation ended before a
      decision was made**:
      1. **`host_prefix_chars` on the 2-3 failing tasks** — cheapest, reuses
         the existing (and now correctly `max_concurrent_tasks`-bound, see
         DONE.md) partitioning mechanism, `tasks.json`-only change.
         `maya_ifstatus` is at ~77-78k buckets vs. the 65k limit;
         `host_prefix_chars: 1` (16-way split) already clears it with margin.
      2. **Cascade 60m (and potentially other coarse intervals) from an
         already-rolled-up finer index instead of raw** — bigger: 19 `avg`
         metrics in `tasks.json` don't cascade correctly via simple
         re-averaging (avg-of-avg ≠ true avg unless sample counts match),
         which is exactly why `1b79c85` chose raw-only in the first place.
         Doing this right needs either a write-schema change (store
         `sum`+`count`, divide at read time — touches what the Trino
         connector/Grafana read) or an accepted-approximation via bucket
         `doc_count` weighting.
      3. **Raise Quickwit's own `aggregation_bucket_limit`** — fixes it at
         the source for every task, but needs a Quickwit image rebuild
         (baked into the image on OGM, not ConfigMap-adjustable, see
         CLAUDE.md) and care around the paired `aggregation_memory_limit`
         (OOM risk).
      See TOBEDECIDED.md.
- [x] **Committed 2026-09-01** — `f92ea65` (partition-semaphore fix) +
      `ca6f931` (60m OOM mitigation), pushed to `origin/master`. See
      DONE.md 2026-09-01 #8.
- [ ] **Push+deploy `qw-rollup-engine:3.1.4-20260901-OGM` — still not live
      anywhere.** Decided 2026-09-01: build via Jenkins
      (`maya-anomaly-platform`, `type=rollup`) going forward, not another
      manual local build like the one this tar came from (see DONE.md
      2026-09-01 #6/#9) — confirmed live via the Jenkins API that `rollup`
      is a valid `type` choice, no dedicated job needed. A real trigger
      (`branch_name=master`, `version=3.1.4-20260901-OGM`, `platform=both`,
      `prod=true`, `prod_ip=192.168.109.203`) was attempted but blocked by
      this session's own auto-mode permission classifier before it ever
      reached Jenkins — script staged in the session scratchpad (see
      DONE.md #9), needs the user to run it directly (`!`-prefixed) or
      grant a Bash permission rule. Both clusters still run the
      pre-2026-08-28 image with none of this quarter's fixes live except
      the ConfigMap-only `missing:N/A` one.
- [ ] **`qw-rollup-engine`'s health check is broken (2026-09-01, reported
      by user, not yet diagnosed).** No symptom detail captured yet (pod
      restarts? readiness flapping? probe command failing?) — needed
      before real root-causing. Working hypothesis only, from reading the
      code: `task_loop.rs:278`'s `let _ = std::fs::File::create("/tmp/
      healthy")` silently swallows any write error — the exact same
      swallowed-error pattern `748893a` (checkpoint-logging) just fixed
      for the Redis checkpoint path. If whatever k8s liveness/readiness
      probe reads this file (existence or mtime), a silent write failure
      would explain it — candidates: `readOnlyRootFilesystem` without a
      `/tmp` emptyDir mount, or an fsGroup/permission mismatch (this
      project has hit that exact class of bug before, see the
      `persistence.type: emptyDir`/`fsGroup` note in CLAUDE.md). Not
      verified against the live probe config on either cluster. Next
      step: get the actual symptom from the user, then check the
      Deployment's `livenessProbe`/`readinessProbe` definition and pod
      filesystem permissions on whichever cluster shows it.
- [x] **RESOLVED (the masking mechanism, not the original prod incident) —
      2026-09-02: `qw-rollup-engine` checkpoint code rewritten directly on
      `master`** (`src/checkpoint.rs`, `main.rs`, `task_loop.rs`,
      `types.rs`, `README.md` — working tree only, not committed/pushed
      yet, user's call). Two changes, independent of `feat/checkpoint-
      refactor`/MR !4 (which still has its own compile bug and
      ConfigMap-rollout risk, see the entry below — unchanged, untouched):
      1. **Checkpoint backend is a resolved-once `CheckpointBackend` enum
         (`Redis(MultiplexedConnection)` or `File`)** driven by a new
         `checkpoint_type` config field — no more automatic runtime
         fallback from Redis to file on any GET/SET outcome. On a Redis
         error/timeout the *same* backend retries every 60s, logged loudly
         (`error!`), forever — it no longer silently drops to the file
         path (which is what let a transient blip, or a clean "not found",
         get masked as "first run" and reprocess every task from today).
         `checkpoint_type` unset defaults to `"redis"` when `redis_url`/
         `REDIS_URL` is configured (else `"file"`), so existing ConfigMaps
         that never heard of this field keep behaving exactly as before —
         this is the exact deployment-regression risk found in MR !4,
         fixed here.
      2. **A missing checkpoint is persisted immediately** (today 00:00),
         not deferred to the first successful rollup window — closes the
         gap where a pod crash-looping before that first window ever
         completes would see "no checkpoint" on every single restart and
         never actually create one, indistinguishable from a real Redis
         bug.
      Single shared `MultiplexedConnection` (was: a new raw connection per
      GET/SET call) and a `backfill_enabled` on/off toggle also carried
      over from MR !4's design. All of the above verified end-to-end
      against a throwaway local Redis container and a `checkpoint_type:
      "file"` run (real 15-task `tasks.json`): single "Redis connection
      established" log line total, all 15 forward+backfill keys/files
      created immediately with the correct value, `backfill_enabled:
      false` skips backfill entirely with zero probes/keys, Redis-
      unreachable blocks+retries every 60s without ever creating
      `checkpoints/`. **Still NOT actually explained:** why the specific
      2026-08-31 prod incident's Redis GET came back clean-nil for all 30
      tasks in the first place — this fix stops that class of event from
      ever being *masked* again, it doesn't retroactively diagnose that
      one incident. See [[qw-rollup-engine-checkpoint-refactor-branch]] in
      memory for the ranked list of un-checked hypotheses (Redis itself
      has no persistence and restarted; `tasks.json` naming drift left
      stale keys; wrong DB index/instance; maxmemory eviction; or simply
      the pod's first-ever correctly-wired restart) if that's worth
      pursuing later.
      Separately, user copied `reset_redis_checkpoints.py` and
      `seed_redis_checkpoints.py`/`.sh` into `scripts/checkpoints/` in this
      same repo (staged, `indexes/` also reorganized to `scripts/indexes/`)
      — closes the pre-existing "not committed anywhere durable" gap noted
      below and in memory.
      **2026-09-02/03 follow-up:** the fix was built, pushed to the dev
      registry (`192.168.57.202:35000/maya/qw-rollup-engine:3.1.4-20260902`,
      verified via `grep -a` on the shipped binary to actually contain the
      new checkpoint code — see DONE.md) but **not yet rolled out to OGM or
      yucemonitoring** — both clusters' live Deployments still run their old
      image. Actually deploying it needs the same `checkpoint_type: "redis"`
      ConfigMap addition flagged above for MR `!4` (unset defaults to
      `"redis"` automatically when `redis_url` is already set, so this is
      likely a no-op in practice, but confirm before rollout). MR `!4`
      itself (mustafa.simsek's branch) is superseded by this fix but was
      left untouched/not closed — worth a conversation with him rather than
      unilaterally closing it.
      **Also found while porting the chart's `checkpointSeed` stopgap to
      `helm_repo1` (see below):** there are now 3 different versions of
      `seed_redis_checkpoints.py` in the project (`~/Downloads/omg_rollup/`,
      `qw-rollup-engine/scripts/checkpoints/`, and `helm_repo1/qw-rollup-
      engine/files/`) — the `helm_repo1` one is the most advanced (adds a
      read-before-write guard on the *forward* checkpoint too, not just
      backfill, plus a `--url` flag), the other two are earlier snapshots.
      Worth reconciling to one canonical copy; not done this session (out
      of scope for what was asked).
- [ ] **OGM's LIVE `qw-rollup-engine-config` ConfigMap still ships `intervals:
      [15, 60]` for all 15 tasks — confirmed 2026-09-03, not just chart-file
      drift.** `ca6f931` (2026-09-01, "drop 60m interval... to mitigate OGM
      Quickwit OOM risk") is committed on repo `master` (local checkout's
      `tasks.json` correctly shows `intervals: [15]`, and `task_expand.rs`'s
      own test asserts this) but was **never applied to OGM's live
      ConfigMap** — fetched directly via `kubectl get configmap
      qw-rollup-engine-config -o jsonpath='{.data.tasks\.json}'`, still
      `[15, 60]` everywhere. This means OGM is running the exact OOM-risk
      config the fix was written to eliminate, right now, on all 15 tasks
      (`metrics3_60`, `rollup_60m_site_*`, etc. all still actively being
      written). Separately confirmed: `metrics3_60`'s Quickwit `index_uri`
      is misconfigured — `file:///quickwit/qwdata/indexes/metrics3_15`
      (points at `metrics3_15`'s directory, not its own `metrics3_60`),
      confirmed via `/api/v1/indexes/metrics3_60/describe`; likely a
      copy-paste at index-creation time, worth a fix independent of the
      OOM item. **Action offered to user 2026-09-03, not yet confirmed:**
      patch `intervals` back to `[15]` in the live ConfigMap (same pattern
      as the tasks.json fix) + restart the `qw-rollup-engine` pod.
- [ ] **`ai/anomaly`/`helm_repo1` rollup chart & datagen dedup — done
      2026-09-02/03, two loose ends.** Full account in DONE.md. (1)
      `helm_repo1/qw-rollup-engine/files/tasks.json` still has `[15, 60]`
      per-task intervals even though `qw-rollup-engine` itself dropped 60m
      entirely (`ca6f931`, OOM mitigation) — pre-existing drift, not
      touched (a content decision, not a structural one), see the live-OGM
      version of this same gap directly above. (2) `master`'s
      old `data_gen_cyclic.py` had a `SCENARIOS`-cycling anomaly-type-
      variety feature (varies which metric/flow-type gets anomalized each
      cycle) not present in the now-canonical `monitoring_temp` copy —
      flagged as a possible enhancement, not implemented (real feature
      work, not cleanup).
- [ ] **Original item, kept for the full incident record (2026-09-01,
      2026-08-31).** Prod qw-rollup-engine pod restart
      found zero Redis checkpoints for all 30 tasks (`Ok(Ok(None))` — a
      clean GET returning nil, not a connection error) despite `redis.url`
      being set and Redis reachable — every task fell back to "First run,
      starting from today 00:00". Not yet root-caused: could be genuinely
      the first-ever restart since `redis.url` was correctly wired for
      that pod (no real mystery), a DB-index/key-prefix mismatch, or
      something else — see the full account in memory
      (`qw-rollup-engine-checkpoint-mystery-todo`). The existing workaround
      (`~/Downloads/omg_rollup/seed_redis_checkpoints.py`/`.sh` — seeds
      forward=now/today and backfill=`BACKFILL_PENDING` only where backfill
      is still untouched, read-before-write so real progress is never
      clobbered) treats the symptom, not the cause, and still only lives
      under `~/Downloads/`, not committed anywhere durable — also now
      stale re: today's 60m-removal fix (its `TASKS` list still has 30
      entries incl. `_60m` variants a fresh process no longer expands).
      Needs: confirm whether checkpoints have EVER been successfully read
      back on the affected pod (not just written) before assuming this is
      a bug rather than a first-restart artifact.
      **Stopgap added 2026-09-01** (`ai/anomaly/deploy/helm/qw-rollup-engine`
      chart, v0.2.0): `seed_redis_checkpoints.py` (extended with a `--url`
      flag so it can reuse the same `REDIS_URL` the main container already
      gets, from `.Values.redis.url`/secret) now ships as a ConfigMap key
      and runs as a `checkpointSeed`-gated `initContainer` on every pod
      (re)creation — backfill → `BACKFILL_PENDING` only where still
      untouched (unchanged). **Caught and fixed same session (by code
      review, not a live deploy):** the first version of this wired
      `--today` into the initContainer unconditionally, and forward had no
      read-before-write guard at all (unlike backfill) — as written, every
      pod restart would have forced *every* task's forward checkpoint back
      to today 00:00, reprocessing all of today even for healthy tasks.
      (User's "restart sırasında sanki sıfırdan başladı" observation that
      prompted this review was NOT actually caused by this initContainer —
      confirmed after the fact that prod has never had this chart deployed
      — it was the manual `--today --apply` run itself landing on the
      original script's unconditional 30-task `SET`, the exact "affects 25
      unrelated tasks too" risk already flagged before that command ran.
      Don't attribute an observed symptom to a repo change without
      confirming a deploy actually happened.) Fixed regardless, since the
      gap was real: forward now gets the same read-before-write guard as
      backfill (only seeded if genuinely 0/missing) and the initContainer
      dropped `--today` (defaults to "now" when it does have to seed, to
      avoid a today-reprocess/duplicate risk rather than recreating what
      the engine's own first-run fallback already does).
      This masks the symptom on every pod recreation going forward; it
      does not explain or fix the underlying non-persistence. Remove the
      `checkpointSeed` block once the real cause is found. Confirmed live
      during this session: prod's chart-bundled `tasks.json`/`config.json`
      are separately stale too (still `[15, 60]` intervals — see the
      pre-existing "Nexus chart missing the missing:N/A fix" item) — a
      `helm upgrade` on this chart right now would regress today's
      60m-removal fix even though it delivers this new stopgap.
      **2026-09-02: teammate (mustafa.simsek) pushed `qw-rollup-engine`
      branch `feat/checkpoint-refactor` (commit `5b43a0c`, unmerged)
      attempting a real fix** — `MultiplexedConnection` established once
      at startup and shared/cloned per task (replaces the old code's
      wasteful one-new-connection-per-`get`/`set`-call pattern), a new
      `checkpoint_type`/`backfill_enabled` config toggle, `get_last_checkpoint`
      now returns `Option<i64>` instead of overloading `0` for both "file
      fallback used" and "genuinely never checkpointed", and the old silent
      Redis-error→file fallback is replaced with an infinite 60s-interval
      retry (no cap) on both the Redis and file paths. **Does not compile
      as pushed** — `cargo check` (verified in an isolated worktree):
      `error[E0425]: cannot find value 'redis_client' in this scope` at
      `src/task_loop.rs:85`, a leftover from the `redis_client`→`redis_conn`
      rename (13 of 14 call sites were renamed, this one wasn't). **Bigger
      risk once fixed:** `main.rs` now only initializes Redis at all when
      `config.checkpoint_type.as_deref() == Some("redis")` — `redis_url`/
      `REDIS_URL` alone is no longer sufficient. None of the 3 live
      environments' ConfigMaps have `checkpoint_type` set today (the field
      is new), so deploying this branch as-is would silently switch
      **every** environment from Redis-backed checkpoints to the
      already-known-fragile file fallback (the `emptyDir`/`fsGroup` bug
      documented in CLAUDE.md) — needs `checkpoint_type: "redis"` added to
      every ConfigMap in the same rollout, not after. README.md:123 and
      the repo's own `config.json` still describe/model the old
      always-try-Redis-then-file-fallback behavior — neither updated on
      this branch. Not merged, not reviewed with the author yet. See
      [[qw-rollup-engine-checkpoint-mystery-todo]] in memory — this branch
      does not, on its own, explain the original clean-nil-GET mystery
      (that's a `None` either way under the new code), only changes the
      failure mode around it.
      **Also 2026-09-02:** wrote `~/Downloads/omg_rollup/
      reset_redis_checkpoints.py` (same dir/dependency-free-RESP style as
      `seed_redis_checkpoints.py`) — an explicit-task-only DELETE/SET tool
      for forcibly resetting one task's checkpoint (and/or its `_backfill`
      companion) to a chosen state, distinct from `seed`'s fill-only-if-
      empty semantics. Requires `--task` (no implicit "all"), defaults to
      dry-run, defaults to DELETE unless `--set`/`--set-today`/`--set-now`/
      `--set-pending`/`--set-done` is given. Smoke-tested end-to-end
      against a throwaway local Redis container (AUTH, GET, SET, DEL,
      wrong-password handling, `--url` parsing) — not yet run against any
      live cluster.
- [ ] **Add a request timeout to qw-rollup-engine's `reqwest::Client`**
      (`src/main.rs` — only `tcp_keepalive` is set, no `.timeout()`). Low
      risk today since `max_concurrent_tasks` isn't deployed yet, but once
      it is, one genuinely hung Quickwit/Trino request would permanently
      exhaust the shared semaphore and stall the whole engine — no other
      task could ever acquire a permit again.

## 🔴 FIRST DECISION NEEDED — metrics3_60 backfill scope (2026-08-28, data refreshed 2026-09-03)

- [ ] **Live-checked 2026-09-03 via `/api/v1/indexes/{id}/describe`:
      `metrics3_60` now covers 2026-08-28 07:00 → 2026-09-03 11:00 UTC
      (~6.17 days, 22 splits, 57.4M docs); `metrics3_15` covers 2026-08-27
      ~05:30 → same end (~6.9 days, 40 splits, 254.7M docs).** Both grew
      purely by running forward since task creation — **the backfill
      mechanism (`c99300c`) never actually ran for either**, because both
      are pre-existing production tasks whose forward checkpoint was
      already non-zero the first time that code executed (permanently
      inert by design, see CLAUDE.md). Raw `metrics3` itself currently
      retains only ~1.8h (`min`≈`max`-6590s) — so even a working backfill
      could only ever pull a couple of hours deeper than "now" at any given
      run, not real historical depth; the ~6-7 day figures above are simply
      how long these tasks have been running, not a backfill result. This
      changes the framing of the item below: there is no shallow-raw
      shortcut to a deep `metrics3_60` history — any real backfill has to
      synthesize/tile from `metrics3_15` as originally planned, not lean on
      the engine's own backfill-on-first-run path.
- [ ] **`metrics3_60` backfill from `metrics3_15` — scoped, not started, needs
      a scope decision before running.** Plan (per user, earlier
      session): take a template window from `metrics3_15`, tile-shift it
      backward from `metrics3_60`'s own earliest real timestamp, **site-by-
      site sequentially** (deliberately no concurrency — the prior session's
      parallel version of this exact class of job is what hung master, see
      DONE.md "Session 28 August 2026"). Measured before running anything:
      one 1-hour template window from `metrics3_15` = **1,378,156 docs**; a
      full 7-day/168-shift backfill at that rate = **~231M docs**, ~60x
      `metrics3_60`'s current size, likely many hours end-to-end fully
      sequential. Presented to user as full-scope-but-slow vs. a smaller
      template window (e.g. 15min instead of 1h, cuts total volume ~4x) —
      **conversation ended before a decision was made.** Next session: get
      the decision, then write the script (source=`metrics3_15`,
      target=`metrics3_60`, one site at a time, minimal/no concurrency
      within a site too) and run it via `nohup...&disown` natively on OGM,
      not foreground from the local session. See TOBEDECIDED.md.

## Open — from 2026-09-04 session (Aggs DSL, `generic_alert` TVF conversion, Grafana alerting toggle, qw-rollup-engine MR !5)

- [ ] **Apply the converted `generic_alert` SQL to yucemonitoring — ready and equivalence-proven, not applied.** See DONE.md item 3. Backup at `~/Downloads/generic_alert_backup_20260904_124004/` (all 26 alert rules + `generic_alert` alone, plus both converted variants and the equivalence-proof CSVs). Needs a decision: apply the raw-JSON variant (byte-identical to the current legacy query, lowest risk) or the DSL variant (shorter, human-editable, relies on this session's new `missing=`/`order=key` support), and via which path (Grafana provisioning API PUT, or direct `alert_rule`/`alert_rule_version` SQL like the 2026-09-03 `generic_alert` split used).
- [ ] **Deploy `3.1.4-20260904-OGM` to OGM's Trino coordinator — image staged, not deployed.** Already mirrored to OGM's own Nexus (`192.168.109.204:35000/maya/trino:3.1.4-20260904-OGM`) and exported to `~/Downloads/maya-trino-3.1.4-20260904-OGM.tar` + the release Samba share (see DONE.md item 4) — `ssh`/`kubectl` to OGM were blocked by this session's auto-mode classifier, needs the user to run `kubectl set image deployment/maya-trino-single-coordinator -n maya3 <container>=maya-nexus:35000/maya/trino:3.1.4-20260904-OGM` (or `docker load` the tar first if the mirror didn't take) directly. Natural to bundle with the still-pending 2026-09-03 history-tier ConfigMap restart below — both need the same coordinator restart.
- [ ] **Convert OGM's own `generic_alert` to the TVF too — not started, different shape than yucemonitoring's.** Per the 2026-09-03 `ogm_grafana_20260903.sql.gz` dump: OGM's `generic_alert` (id=7) is currently **paused**, alongside 11 already-split, unpaused `generic_alert_*` children (df/temperature/maya_ifstatus/bfd/cpu/memory/maya_probe/maya_system_services/maya_dhcp_relay/maya_dhcp/maya_bgp) — a materially different situation from yucemonitoring's single unpaused rule, so yucemonitoring's converted SQL can't just be copied over. Needs its own conversion once (or if) the `3.1.4-20260904-OGM` image above is actually deployed to OGM.
- [ ] **Decide on enabling Grafana's `alertingSaveStatePeriodic` feature toggle.** See DONE.md item 5 and memory `grafana-alert-instance-persistence-toggle`. Likely explains why `alert_instance` doesn't auto-populate on yucemonitoring. Enabling needs `GF_FEATURE_TOGGLES_ENABLE=publicDashboards,alertingSaveStatePeriodic` (replacing the current single-purpose `GF_FEATURE_TOGGLES_PUBLICDASHBOARDS` env var, not adding alongside it) on the `maya-grafana` Deployment + a pod restart. Not checked whether OGM's Grafana has the same gap.
- [ ] **Merge `qw-rollup-engine` MR !5 (`feat/redis-connection-manager`) — verified green, not merged.** See DONE.md item 6. `cargo check`/`test`/`clippy` all clean as of `e232bb0`, but no CI pipeline has ever run on this MR — worth triggering one before merging rather than relying solely on this session's local verification. Once merged, this is a real candidate fix for the long-standing checkpoint-mystery item below (Redis GET returning clean-nil for all 30 tasks) — doesn't retroactively explain that specific incident, but stops the failure mode (a dropped connection silently breaking every subsequent checkpoint write) from recurring.

## Open — from 2026-09-03 session (OGM history-tier revert, alert_rule SQL, maya-monitoring-works)

- [ ] **OGM Trino coordinator restart — pending, not confirmed by user.**
      `maya-trino-configmap` (OGM, `maya3`) was patched 2026-09-03: removed
      `history-tiers=60:86400`, `history-time-threshold-seconds` changed
      `15:3600` → `10800` (reverts to pre-multi-tier behavior — raw ≤3h,
      then the classic hardcoded 15m tier, no 60m routing — done because
      `metrics3_60` has known coverage/`index_uri` problems, see above).
      Backup of the pre-patch ConfigMap saved to the session scratchpad
      (not durable — re-fetch live and re-save if actually needed later).
      **Trino only reads catalog properties at startup — this patch is
      inert until `maya-trino-single-coordinator` is restarted**
      (`kubectl rollout restart deployment/maya-trino-single-coordinator -n
      maya3`), which briefly interrupts live OGM Grafana queries. User was
      asked whether/when to run this restart; no answer given before the
      session ended.
- [ ] **yucemonitoring parity decision.** yucemonitoring's
      `maya-trino-configmap` still has the original multi-tier config
      (`history-time-threshold-seconds=15:10800` + `history-tiers=
      60:86400`, i.e. correctly on a 3h raw window, unlike OGM's now-fixed
      1h one) — **not touched this session**, since the ask was scoped to
      OGM only. Needs a decision: revert yucemonitoring the same way (drop
      `history-tiers`, keep `10800`), or leave its 60m tier live there.
- [ ] **`maya-postgres-single`/pgWorks upgrade (cfg + image) — investigated,
      not executed.** User's actual ask ("sadece cfg ve imaj değiştirmeyi
      planlıyorum") is scoped narrowly, but 3 things are still needed before
      touching anything: (1) exact new `pgWorksImage.tag` and which `cfg`
      keys change — not specified yet; (2) which branch of `monitoring_temp`
      (source repo for Jenkins job `postgres-monitoring-works-platform`,
      `JENKINS_JOB_POSTGRES` in `.env`) actually has the intended change —
      currently on `develop`, matches the job's default `branch_name`, not
      independently verified to contain the target change; (3) how the Helm
      side is actually deployed on OGM today — `helm_repo`'s local checkout
      is on unmerged branch `mr-369` (never merged to `master`/`develop`)
      with a `pgWorksImage.repository` mismatch vs. what's actually live
      (`maya/maya-monitoring-jobs` locally vs. `maya/monitoring-jobs` live,
      confirmed via `helm get values -a`) — don't apply that local
      `values.yaml` wholesale; use `helm upgrade --reuse-values --set
      pgWorksImage.tag=... --set pgWorks.<KEY>=...` instead, or confirm the
      real deploy path first. Along the way, found and confirmed the
      user's separately-pasted `postgres-monitoring-works` CronJob (old tag
      `3.0.8-12122025-ST`, suspended) was a **stale orphaned duplicate** of
      the real, already-current `maya-postgres-single-cronjob`
      (`3.1.4-20260810-OGM`, deployed via Helm since 2026-08-18, healthy) —
      it was deleted (by the user or their own tooling) mid-session; nothing
      to do about it, just don't confuse it with the live cronjob again.
- [ ] **Grafana `generic_alert` split (`cpe_eval_group`) — SQL built and
      query-tested, not yet applied to any database.** From
      `~/Downloads/ogm_grafana_20260903.sql.gz` (a full `grafana` DB dump):
      11 new `generic_alert_*` rules (df, temperature, maya_ifstatus, bfd,
      cpu, memory, maya_probe, maya_system_services, maya_dhcp_relay,
      maya_dhcp, maya_bgp — all unpaused, versions 8-18, created
      2026-08-19) plus a corrected `generic_alert` parent (id=7): its query
      had drifted to `m_notif_plugin:df` only (same as `generic_alert_df`,
      paused) — rebuilt from the pre-drift broad query (`span_attributes.p:
      maya_alarm`, last seen intact at v320/2026-05-22) with an explicit
      `AND NOT (...)` exclusion for the 11 split-off `m_notif_plugin`
      values, unpaused, bumped to v347. The exclusion query's syntax was
      verified by running it live against OGM Quickwit's `/search` (parses
      cleanly, `errors: []`; 0 hits is expected — no alarm data in the
      currently-retained raw window, not a query problem). All 12
      `alert_rule` + 12 matching `alert_rule_version` rows are `INSERT ...
      ON CONFLICT (id) DO UPDATE` (idempotent — safe whether the target
      already has these exact rows or not), saved to
      `~/Downloads/generic_alert_perf_rules.sql` and
      `~/Downloads/generic_alert_perf_rule_versions.sql` (copied out of the
      session scratchpad so they survive). **User said they'd review the
      new `generic_alert` query before applying** — not yet confirmed
      applied to OGM's `grafana` Postgres DB. Apply order: `alert_rule`
      before `alert_rule_version` by convention (no FK enforces it either
      way, confirmed — `alert_rule_version` only has a PK on its own `id`).
      No need to stop Grafana first (both files are atomic per-statement
      upserts; this project's own `OGM_DEMO_INSTALL_DONE.md` did similar
      direct-SQL fixes against a live Grafana before, Bölüm 6/F) — if
      worried about the new rules firing immediately, insert with
      `is_paused='t'` first and unpause via UI instead of a full stop.

## Open — resolved this session, see DONE.md for full detail (2026-08-28)

The prior session's 🔴 CRITICAL items (OGM master unresponsive, Nexus
migration to worker1, `data-gen`/`otelcontribcol` resource limits, 2 new
post-recovery `ImagePullBackOff`s) were all resolved this session — full
narrative in DONE.md "Session 28 August 2026 (continued again)". Nothing
carried forward from that block except the backfill decision above and the
old (now-superseded) synthetic-replication item removed below — its
process/script check came back clean (no lingering process, `/tmp` script
gone with the reboot, re-copied from a prior session's scratchpad), so
there's nothing to resume from that attempt specifically.

- [ ] **`interfaces_trino` QoS-dashboard variable Bug A — root-caused,
      NOT fixed.** `span_attributes.m_name:IN [${class_names}]` sends
      literal `IN [-]` to Quickwit when `class_names` is unselected
      (confirmed live: Quickwit returns HTTP 400 "failed to parse
      query" for exactly that string) — unlike sibling filters in the
      same dashboard, this one has no `'${class_names}' = '-' or ...`
      guard. Fix not applied (session moved on to Bug B, then the infra
      incident). See DONE.md for full detail; the fix is a small SQL
      edit to the `interfaces_trino` variable's Quickwit query, same
      live-edit process as the QoS dashboard fixes already shipped.

## Open — infra (OGM, `192.168.109.203`)

- [ ] **yucemonitoring has no `netlink_15m` (or `maya_bfd_15m`/
      `maya_system_services_15m`/`maya_ifstatus_15m`) Quickwit index at
      all — investigation paused mid-way 2026-08-26, not root-caused.**
      User asked why OGM shows much higher netlink rollup counts than
      yucemonitoring ("20 kat küçük" — 20x smaller). Checked yucemonitoring's
      live index list directly (`kubectl exec deployment/maya-quickwit-s3
      -- curl localhost:7280/api/v1/indexes`): only `metrics3_15` + the 5
      `rollup_15m_site_*` flow indexes exist — no dedicated netlink/bfd/
      system-services/ifstatus rollup indexes at all, contradicting
      DONE.md's 2026-08-25 note that `netlink_15m` had 1,120 real
      docs/window there as of that session. Either the index was dropped/
      never durably created, or the qw-rollup-engine task config on
      yucemonitoring no longer includes these tasks. **Not yet compared
      against OGM's index list** — that check was hanging/timing out
      (OGM's `maya-quickwit` pod was only ~21min old at the time,
      0 restarts per `kubectl get pod ... -o jsonpath=...restartCount`,
      so a clean recent rollout, not a crash — but `/api/v1/indexes` and
      `/describe` calls through it were consistently timing out even via
      direct pod `exec`, unlike search POSTs which worked fine). Paused
      here per user request ("biraz yoğun sistem... rahatlasın"); next
      session should: (1) get OGM's current index list once the cluster
      isn't under this load, (2) check yucemonitoring's `qw-rollup-engine`
      task config (`tasks.json`) for whether netlink/bfd/etc. tasks are
      even defined there, (3) only then figure out whether the user's
      "20x" observation is about this missing-index gap or about
      netlink-tagged document volume inside `metrics3_15` instead — those
      are different things and weren't disambiguated.
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
      **Correction 2026-08-28: worker nodes don't have internet either.**
      That last claim was wrong — re-tested this session (`coredns`
      ImagePullBackOff on `worker1`, DNS to `registry.k8s.io` timed out
      there too; `worker2` failed the same way). **None of the 3 OGM
      nodes have real outbound internet.** The reliable mirroring path
      going forward: run `skopeo copy` (or `docker pull`+`save`+`ssh...
      load`) from *outside* the OGM network entirely — e.g. from
      wherever Claude's own session runs, which has both internet and a
      direct network path to `maya-nexus:35000` — straight into the
      local registry, rather than trying to find a node that can reach
      the source.
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
      **Side-note resolved 2026-08-27:** the separate "OGM's Trino image
      predates `eea9592`" staleness observed above is also moot now —
      OGM's Trino image was rebuilt this session from `1h_rollup` (branched
      off `develop`), confirmed via `git merge-base --is-ancestor eea9592
      1h_rollup` to include that commit. Not the reason it was rebuilt,
      just a side effect worth recording so nobody re-diagnoses this.
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
- [ ] **`maya-reporting-sqla-platform` Jenkins job still broken** —
      `backend/sqla` repo, blocking a clean `sqli`-sibling build of
      `grafana-sql-analyser`. Fixed 3 sequential missing-dependency
      failures in its Dockerfile 2026-08-25/26 (`autoconf`/`automake`/
      `libtool`, then `build-essential`, then `python3` — each fix got
      further before hitting the next missing tool; commits `7a6b1be`,
      `ad7daf1`, `75645e1` on `develop`, pushed). Current failure (build
      #66): `bindgen-0.72.1` panics — needs `libclang`/`clang` (bindgen
      shells out to libclang for the C header it's binding) added to the
      same `apt-get install` line. Not fixed — deprioritized once the
      actual deploy need (see DONE.md) was satisfied a different way.
      `maya-reporting-sqli-platform` (the sibling job) builds clean
      (#70) — no known reason `sqla` and `sqli` diverge on system deps.
- [ ] **`ROLLUP_RETENTION=1 month` vs the 90GB `qwdata` volume — capacity
      may not actually fit.** Measured 2026-08-26 from the just-completed
      full-density 8-day/753-site backfill: all 6 rollup indexes combined
      cost **~3.6-4GB/day** at current (post metric-type-revival) real
      density. A full 1-month retention window, once the live indexes
      actually hold that much, would need **~107-120GB** — more than the
      90GB volume has. See TOBEDECIDED.md for the decision (shorten
      retention vs. expand further vs. accept it'll self-limit via
      DiskPressure). Not urgent today: live accumulation is still well
      under a month deep, and the retention *code* only just went live
      (see DONE.md) — but don't be surprised when it becomes urgent.
- [ ] **`Quality_of_Service` / `SLA_Chart`: 2026-08-26 fix attempt applied
      then REVERTED same session — the underlying path-derivation theory
      was wrong, disproven empirically against live Trino after the fix
      was already live on both clusters.** Both dashboards are back to
      their original (still-buggy) text on OGM and yucemonitoring as of
      this note — nothing is fixed yet, see the full account below for
      what's actually true and what isn't.
      **What happened:** traced every panel's Quickwit sub-query by hand
      against its own `//replacefromcolumns=` prefix, concluding the
      bucket-key references (site/host, traffic class, interface, overlay,
      peer uuid — things like `"2221/2/key"`, `"111/15/key"`) were stale
      and should reduce to short forms like bare `"key"` after the
      declared strip prefix. This looked well-verified — cross-checked
      against 2 live Quickwit queries (`flows3`/`metrics3`), internally
      consistent (leaf-value refs like `222/value` matched the same rule
      and were already correct in every panel) — so the fix was applied
      live to all 4 dashboard/cluster combos (OGM+yucemonitoring ×
      QoS+SLA_Chart).
      **It was wrong.** Verifying end-to-end through Trino afterward (not
      just against raw Quickwit shape) showed the *original* text —
      `"2221/2/key"`, `"2222/2/key"` — already resolved correctly and
      returned real data; my "fixed" bare `"key"` did not resolve at all
      (`Column 'key' cannot be resolved`). `SELECT *` against the live
      table revealed the real column-naming scheme is not a simple
      prefix-strip of the true nesting path — empirically, a bucket's key
      shows up **once per sibling leaf-metric**, named
      `<leafAggId>/<ancestorAggId>/key` for seemingly every ancestor level,
      not just the immediate parent (e.g. leaf `"1"` paired with ancestor
      `"2"` *and* ancestor `"3"` both produced valid columns). This does
      not match either `flatten()` or `flattenJsonNode()`'s code as read —
      the actual runtime behavior diverges from what a static trace of
      those methods predicts, for reasons not yet understood. **Reverted
      all 4 dashboards to their original pre-session text** (OGM QoS
      v179, OGM SLA v77, yucemonitoring QoS v179, yucemonitoring SLA v77) —
      confirmed via fresh GET that the original markers are back on all 4.
      **Net result: nothing is actually fixed.** The dashboards are
      exactly as broken as the original Query Sweep found them.
      **Lesson for next attempt, don't repeat this mistake:** any proposed
      column-path fix for this connector MUST be verified by actually
      executing it through Trino (`kubectl cp` a `.sql` file into the
      trino-coordinator pod, `trino --file`) and confirming real rows come
      back — matching the Quickwit response shape alone is not sufficient
      evidence, no matter how internally consistent the derivation looks.
      `SELECT *` against the live table is the fastest way to see the
      *actual* resolvable column names for a given aggs body +
      `//replacefromcolumns=` combination.
      - The `classes_from_metrics` CTE present in all 3 QoS panels is
        confirmed **dead code** (defined, never referenced downstream) —
        not part of this bug regardless of which theory is right.
      - `SLA_Chart`'s missing `iface` column (selected/grouped at the
        middle subquery level but never produced by any inner subquery)
        is a **separate, still-real, still-unfixed** structural bug — independent of
        the path-naming confusion above. The specific column expression I
        added for it (`/3/buckets/2/buckets/4/buckets/key`) is *not*
        trusted given everything above and was reverted along with
        everything else; needs the same real-Trino-execution verification
        before trying again.
      - The original documented errors (`Column '1/value' cannot be
        resolved` on 2h-24h, the 2d-specific null-cast, QoS's 4d-7d hang,
        SLA_Chart's short-range timeouts) are **still unexplained** — my
        "fix" targeted the wrong thing, so none of this investigation
        should be assumed to carry over. Next attempt should start from
        `SELECT *` against each panel's actual query (with real
        `//replacefromcolumns=`) rather than from this session's
        path-derivation theory.
      - Unaffected by any of this: the `QwUtil.traverseAggregations`
        null-guard code fix (separate item, above) — never deployed, no
        live impact either way, still a reasonable fix on its own merits.
- [x] **`Top_Sites_Traffic` — deprioritized 2026-08-26, dashboard
      confirmed unused by the user ("bu dash kullanılmıyor").** Never
      applied (per user direction, not because of the item below) — good
      thing, since the fix used the same flawed path-derivation theory
      the QoS/SLA_Chart fix above turned out to be wrong about (only the
      `${type}` label part is trustworthy; the stale-columns part is not).
      Not worth revisiting unless the dashboard comes back into use, and
      if it does, re-derive the columns fix properly (real Trino
      execution, not Quickwit-shape matching) rather than reusing what's
      already sitting prepared.
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
      **Reality-check added 2026-08-26:** that `~14.4GB for 2 years`
      estimate was extrapolated from the low-density synthetic generator
      (`rollup_backfill_pilot.py`). A same-session real-density measurement
      (see DONE.md, full-site tile-and-shift backfill) puts actual full
      density at ~3.6-4GB/day for all 6 rollup indexes combined — 2 years
      at that rate is **~2.9TB**, not 14.4GB. If "2 years" ever means real
      density rather than sparse synthetic filler, this needs a real
      capacity conversation first, not just running the script longer.
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

- [x] **Connector fix (`QwUtil.traverseAggregations`'s null-string guard) built and deployed 2026-08-27 — see DONE.md "Session 27 August 2026".** Shipped as part of that session's broader multi-tier history routing deploy to both yucemonitoring and OGM (same image, same rollout). Superseded in importance by a deeper, related fix in the same session: `arrangeAggregation()` was dropping bucket *identity* (not just stringifying a null value) whenever a leaf metric was null — see DONE.md item 1.3.

- [ ] **OGM demo Redis-pool-exhaustion — root cause diagnosed 2026-08-26, no fix applied yet.** User asked "OGM demo'da problem nedir" — coordinator logs showed `JedisException: Could not get a resource from the pool` (~245/hour), queries stalling 30-110s then `USER_CANCELED`. Root cause chain: `maya-quickwit` pod is pinned to `ssb-sdwan-master` (node affinity) which runs ~72% memory, so every pod reschedule/restart leaves Quickwit briefly `Pending`/unreachable; during that window Trino→Quickwit calls slow down or time out (`okio.Timeout`, some `SLOW_QUERY` entries 300s+); `ConnectorBaseUtil.select()` (see CLAUDE.md architecture bullet) holds its Redis connection for the full duration of that slow call instead of just the cache GET/SET, so `JedisPool` (maxTotal=1000) exhausts fast. Redis itself is healthy (used_memory 5.4MB, blocked_clients:0) — this is entirely a coordinator-side connection-holding design issue plus Quickwit's node-pinning. Two independent fixes needed, neither applied: (a) relax Quickwit's master-only node affinity or free up memory on that node; (b) narrow `ConnectorBaseUtil.select()`'s Jedis scope to just the cache calls, moving `exec1.apply(...)` (the downstream fetch) outside the `try (Jedis jedis = ...)` block. Needs a decision on priority/whether to do both — see TOBEDECIDED.md. **Not touched in the 2026-08-27 session** — still open exactly as described here.
- [x] **`qw-rollup-engine` first-run backward backfill — built and deployed 2026-08-27** (to yucemonitoring earlier that session, then to OGM — see DONE.md "Session 27 August 2026" item 2). Live-verified on both: pre-existing forward checkpoints untouched, new tasks' first-run backward walk behaves as designed.

- [ ] **Per-tier `ROLLUP_RETENTION` differentiation — investigated 2026-08-27, not implemented.** User wants 60m rollup indexes retained separately (shorter, e.g. 1 week) from other rollups — `pg_works.py`'s `discover_rollup_indexes()` (`monitoring_temp/grafana/grafana_init/init/pg_works.py:219-227`) currently applies one blanket `ROLLUP_RETENTION` value to every index matching `^(rollup_.+|metrics3_\d+)$`, with no granularity distinction at all. OGM's live cronjob already happens to have `ROLLUP_RETENTION=1 week` (auto-covers the new 60m indexes fine as a *side effect*, not by design) — but that same 1-week value is also currently being applied to every 15m rollup index too, which is probably not intended once someone actually checks. Needs: a new env var (e.g. `ROLLUP_RETENTION_60M`) read in `pg_works.py`, a regex/suffix check in `discover_rollup_indexes()` to pick the right value per matched index, then wiring into `helm_repo1/postgres-single/values.yaml`+`templates/pg-works.yaml` (already has `ROLLUP_RETENTION` wired, just needs the new var added) and `postgres-multi`'s equivalent (currently doesn't wire `ROLLUP_RETENTION` *at all* — moot right now since that cronjob is intentionally suspended on yucemonitoring, but the chart gap should still be closed for whenever it's unsuspended). Also worth fixing while in there: `postgres-single/values.yaml`'s chart source still says `ROLLUP_RETENTION: 2 years`, drifted from the live `1 week` — same "chart source vs live" drift pattern as the trino charts fixed this session.
- [ ] **OGM's 7-day synthetic 60m backfill script (`ogm_replicate_60m_7days.py`) only exists in the session scratchpad, not committed anywhere durable.** Wrote and ran it live 2026-08-27 (see DONE.md item 3) — site-by-site parallel fetch/ingest with a hard-won set of fixes (self-poisoning re-fetch, `commit=force` slowness, chunk-size limits, per-index site-field name). If this kind of one-off demo-data backfill is likely to be needed again (e.g. after the next `qwdata`/index rebuild, or for a different time window), worth committing it somewhere durable first — as written it's fairly OGM/this-exact-hour-specific (hardcoded `SOURCE_HOUR_START_NANOS`, hardcoded index list) and would need generalizing.
- [ ] **`machine_ops/OGM_DEMO_INSTALL_DONE.md` section 13, items A-F (and possibly a "## 12") are permanently lost from git, unless recovered via IntelliJ Local History.** They existed on disk (uncommitted) before a `Write`-tool mistake in the 2026-08-27 session overwrote the whole file with only the portions that had been read; `git restore` recovered everything through the last real commit (18 August) but couldn't bring back this uncommitted span. Items G-Q of the same section were recoverable from the session's own conversation history and are back in place. A live attempt to decode `~/.cache/JetBrains/IntelliJIdea2026.2/LocalHistory/changes.storageData` (timestamped exactly right, 25 Ağustos 21:07) failed — it's a compressed/custom IDE format, not plain zlib despite 3 coincidental byte-pattern matches. If this content still matters, the reliable path is IntelliJ's own UI: open the file, right-click → Local History → Show History, look for the ~21:07 revision.
- [ ] **4 GitLab MRs open from the 2026-08-27 session, none merged yet** — [connector !35](https://gitlab.ulakhaberlesme.com.tr/maya/presto-influxdb-connector/-/merge_requests/35) → `develop`, [helm_repo !370](https://gitlab.ulakhaberlesme.com.tr/maya/helm_repo/-/merge_requests/370) → `develop`, [qw-rollup-engine !2](https://gitlab.ulakhaberlesme.com.tr/maya/qw-rollup-engine/-/merge_requests/2) → `master`, [backend/anomaly !2](https://gitlab.ulakhaberlesme.com.tr/backend/anomaly/-/merge_requests/2) → `develop`. All 4 confirmed `can_be_merged` (connector one needed a target-branch fix first, see DONE.md). `backend/anomaly` also has one pre-existing, unrelated uncommitted file (`deploy/data-gen-ogm-demo.yaml`) sitting locally — not part of the MR, not investigated this session, left alone.

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
      (This exact fix is included in the reviewed audit below —
      `view_interface` UPDATE statement — so applying that file closes
      this item too.)
- [x] **Reviewed 2026-08-26 (this session): the `general-purpose` agent's
      yucemonitoring stale-`//columns=` audit (prepared 2026-08-25) is
      more solid than it looked — genuinely live-capture-verified, not
      blind-generated.** Files still on disk (different session's
      scratchpad, still readable):
      `/tmp/claude-1000/.../fe1dd4bc-.../scratchpad/yucemonitoring_columns_fixes.sql`
      (19 `UPDATE maya_global_settings` statements, each a surgical
      single-line `replace()` on the exact original `//columns=` text —
      no-ops safely if the row already changed) and
      `apply_dashboard_fixes.sh` (5 dashboard-JSON PUT payloads for
      `Alarms`/`Alarms History`/`LTE`/`Hub Resource Utilization`/`Hub
      Resource Utilization Disk Time Analysis`, payloads pre-built with
      `overwrite:false` so they refuse rather than clobber if someone
      else edited meanwhile). Confirmed real verification evidence exists
      in the same directory (`live_diff.json`, `live_captures.json`,
      `capture_live.py`) — e.g. concrete `declared_missing_from_real`
      gaps captured per-dashboard (Alarms: missing `alarm_name`; LTE:
      missing 13 real fields incl. `rsrp`/`sinr`/`rsrq`; Hub Resource
      Utilization: missing `host`/`/1/5/key`/`/1/7/key`), not just
      hand-waved. **Not the same bug as the QoS/SLA_Chart fix above** —
      this audit only ever checked the declared `//columns=` list against
      reality (matters for the empty-result fallback path), not the
      SELECT-list bucket-key references themselves (matters for every
      query, empty or not) — complementary, not overlapping, coverage.
      One item (`del_view_probe_with_site_filter`) is flagged by the
      audit itself as likely-orphaned (`del_` prefix) — recommend
      skipping that one/considering row deletion instead of fixing it.
      Did not re-verify current live state (the `mayapostgres`/
      `maya_grafana` Trino catalogs are raw-JDBC-passthrough only, no
      `SHOW TABLES`/direct `SELECT` — matches the `//dbtype=pg` note in
      DONE.md "Session 25 August 2026" item 3; would need real psql
      access to re-check). Ready to apply via `!`, same as everything
      else in this session — not applied yet.
- [ ] **`_rustrino` Grafana macros missing `//historyenabled=`.**
      Discovered 2026-08-20: macros ending in `_rustrino` (e.g.
      `view_interface_with_site_filter_rustrino`, used by the Hub Network
      Throughput dashboard) call `qw_agg('metrics3', ...)` via the
      `rustrino-pg` Postgres datasource without a `//historyenabled=`/
      `//historyindex=` header, so long time-range queries never route to
      the history index and can time out. Not touched since discovery —
      needs the `maya_global_settings` macro definitions updated, or the
      `qw_agg` plugin changed to append the header automatically.

## Housekeeping — pre-existing, not from any recent session

- [ ] **~90 untracked scratch files + 5 pre-existing modified files
      (`docker-compose.yml`, 4 `trino/etc/catalog/*.properties`) sitting
      in the working tree, unrelated to any session's actual work.**
      Noticed 2026-08-28 while preparing a handover — these were already
      present (`git status`) before that session's first action, last
      real commit touching `docker-compose.yml` is from 12 May 2026, and
      none of it was investigated or touched this session (out of scope,
      not this session's to clean up blind). Mix of debug scripts
      (`Test*.java`/`.class`, `patch_*.py`, `extract_*.py`, etc.) and the
      5 small property-file diffs. Worth a deliberate pass sometime to
      decide what's still needed vs. safe to `git checkout --`/delete —
      just don't `git add -A`/`git add .` in the meantime.
