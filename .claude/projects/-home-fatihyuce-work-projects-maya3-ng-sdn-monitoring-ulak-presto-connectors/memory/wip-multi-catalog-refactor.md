---
name: develop-branch-state-2026-05-14
description: QW8+QW9 fixes; QW9-03 WIP (written, NOT deployed); next session — refactor+test+deploy QW9-03
metadata:
  type: project
---

**Status**: QW9-01 + QW9-02 deployed Jenkins #355+#356. QW9-03 written (commit `b1ffb10`) but NOT built/deployed.

**Why:** Multi-session COLUMN_NOT_FOUND fix series for Grafana dashboard panels.

**How to apply:** Next session: (1) refactor QW9-03 to Guava + PlainTableQuery, (2) add tests, (3) build + Jenkins deploy.

## Open items (priority order)

| ID | Konu | Durum |
|---|---|---|
| **QW9-03** | base32 decode in `getTableHandle()` — J56 regression fix | WRITTEN, NOT BUILT. Pending: Guava refactor + tests + deploy |
| **view_interface_with_site_filter** | sqlversion=0 JFlat path: 1 NULL row for eth* | OPEN (after QW9-03 deploy, re-check) |
| J56 | QW connector — base32 regression root cause documented | DONE (documented) |

## QW9 tamamlanan (2026-05-14)

| ID | Fix | Commit / version |
|---|---|---|
| QW9-01 | traverseAggregations: expose /aggId/key alongside aggId/key for 0.1 | d147c18, Jenkins #355 |
| QW9-02 | analyze() + parseResponse(): fallback to columns param when traverseAggregations empty | b129765, Jenkins #356 |
| QW9-03 (WIP) | getTableHandle(): decode base32 before storing — J56 regression fix | b1ffb10 (NOT DEPLOYED) |

## QW9-03 tam kök nedeni (2026-05-14 keşif)

**Ne bozuldu:** J56 (`isPlainMode()`) query-in-table-name stilini bozdu.

**Mekanizma:**
1. Grafana Trino plugin, tablo adını (multi-line `//columns=...` içerenler dahil) **base32'ye encode eder** — case normalizasyonundan kaçmak için. Bu davranış eski koddan beri vardı.
2. `QueryParameters.getQueryParameters()` Guava `BaseEncoding.base32()` ile decode ediyordu — J56 öncesi çalışıyordu.
3. J56, `isPlainMode()` check'ini `QueryParameters`'dan **ÖNCE** koydu. Base32 string'de `//` yok → `isPlainMode()=true` → `getColumnsFromDocMapping(base32_string)` → Quickwit'te böyle index yok → boş schema → `COLUMN_NOT_FOUND`.
4. Fix: `getTableHandle()`'da decode → handle + downstream hepsi orijinal string'i görür.

**Pending refactor:** Mevcut fix custom decoder kullanıyor. `QueryParameters` ile tutarlı olması için:
- Custom `base32Decode()` + `decodeBase32IfNeeded()` → `PlainTableQuery.decodeIfBase32Encoded()` (static, Guava)
- `getTableHandle()` → `PlainTableQuery.decodeIfBase32Encoded()` çağrısı
- `PlainTableModeTest.java`'ya test: base32 encode → decode round-trip; `isPlainMode()` false döner

## Repo / cluster state (2026-05-14 end of session)

- Connector develop tip: `b1ffb10` (QW9-03 WIP)
- java-client develop tip: `56224e6` (0.0.1.43-SNAPSHOT)
- Cluster `yucemonitoring`: Jenkins #356 (QW9-02); Trino pod `7c7b9c7666-b4xsh`; QW9-03 NOT deployed
- Rollback image: `maya-nexus:35000/maya/trino:0.0.1-develop-merge` (Jenkins #340)
- `origin-github` (Chasingdreams6 fork) — DO NOT TOUCH

## Next session checklist

1. `cd /home/fatihyuce/work/projects/maya3/ng_sdn/monitoring/ulak-presto-connectors && git switch develop && git pull --ff-only`
2. Refactor `decodeBase32IfNeeded` → `PlainTableQuery.decodeIfBase32Encoded()` (Guava)
3. Add tests to `PlainTableModeTest.java` (round-trip: encode with Guava → decodeIfBase32Encoded → same string; isPlainMode false)
4. Build: `./mvnw -pl ulak-presto-quickwit-connector -am -DskipTests clean package`
5. Commit QW9-03 final + Jenkins deploy + verify `/buckets/2/a/key` errors stopped
