# Changelog — ulak-presto-quickwit-connector

Items move here from [`TODO.md`](TODO.md) when finished.

## 2026-05-13 — Quickwit 0.8 compatibility (QW8 series)

All fixes are in `monitoring_temp/maya-quickwit/quickwit-java-client` (develop branch, commits b9b5076–fc052c4). Deployed via Jenkins builds #341–#349 to `0.0.1-develop-latest` image on yucemonitoring.

- **QW8-01** — 81 leaf model classes: strict unknown-field validation removed. QW 0.8 added `coerce`, `fast`, `indexed`, `output_format`, `stored` etc. to `FieldMappingEntry`; old validator threw `Failed deserialization for VersionedIndexMetadata`. Fix: lenient deserialization for non-discriminator classes only.
- **QW8-02** — `MergePolicyConfig` oneOf discriminator: strict validation restored for all 34 `*OneOf*` classes. Removing validation from all 81 classes (QW8-01 scope) also broke oneOf selection — all 3 `MergePolicyConfig` variants matched, expected 1. Fix: restore strict validation only for `*OneOf*` discriminator classes.
- **QW8-03** — `VersionedSourceConfigOneOf`: added `num_pipelines` to `openapiFields`; removed `params` from `openapiRequiredFields` (ingest/ingest-cli/ingest-api sources carry no params in QW 0.8).
- **QW8-04** — `VersionEnum` in 5 `Versioned*OneOf` classes (`VersionedIndexConfigOneOf`, `VersionedIndexConfigOneOfAllOf`, `VersionedIndexMetadataOneOf`, `VersionedSourceConfigOneOf`, `VersionedSplitMetadataOneOf`): added `_0_8("0.8")`. QW 0.8 returns `"version":"0.8"`; old enum only knew `"0.7"` → `Unexpected value '0.8'`.
- **QW8-05** — `FastFieldOptions` read adapter rewritten. QW 0.8 `dynamic_mapping.fast` can be `{"normalizer":"raw"}` (object). Old adapter: (1) called `getAsJsonObject()` on any element — would fail on boolean/string; (2) auto-matched `String` schema for objects; (3) never tried `FastFieldOptionsOneOfEnabledWithNormalizer`. New adapter: handles primitives directly as String, tries `WithNormalizer` first for objects, removes String auto-match.
- **QW8-06** — `FastFieldOptions` schema registry completed. Added `FastFieldOptionsOneOfEnabledWithNormalizer` to `schemas` static map, `setActualInstance()` type guard, constructor, getter, and `validateJsonObject()`. Fixes `Invalid instance type. Must be FastFieldOptionsOneOf, String` error. java-client commit `56224e6`, `0.0.1.42-SNAPSHOT`, deployed Jenkins #350.

## 2026-05-12 — Code review + architectural fixes (R-series, J-series, L-series)

### Security fixes

- **R01** — Rhino ClassShutter + `initSafeStandardObjects()` + 100k instruction limit. Blocks all Java stdlib access from hasjs scripts; prevents remote code execution via `java.lang.Runtime`. Logic extracted to `RhinoExecutor.java` (SPI-free, unit-testable).
- **R02** — SSRF via `//qwurl=` mitigated. `UlakQuickwitMetadata.validateQwUrl()` enforces a catalog-level URL allowlist. Default: only `qw-connection-url`. Set `qw-allowed-urls=*` to allow any URL (opt-in).

### High-priority bugfixes

- **R03** — `defaultClients` `LinkedHashMap` → `ConcurrentHashMap` + `computeIfAbsent` (thread-safe lazy init).
- **R04** — `//name=` header was reading `h.isCache()` instead of `h.getName()`. Fixed.
- **R05** — All three timeouts in `RawQuery.analyze()` used `connectTimeout`. `readTimeout`/`writeTimeout` restored.
- **R06** — `Optional` compared with `==` in `RawQuickwitQueryTableHandle.equals()`. Fixed with `.equals()`.
- **R07** — NPE in `getSchemas()`/`getTableNames()` when client is null. Null guard added.

### Medium-priority bugfixes

- **R08** — `buildSearchRequestJson` called twice in `QuickwitSplitManager` producing diverging timestamps. Single call.
- **R09** — `noDataIndex` lambda capture never incremented → duplicate `no-data-0` column names. Fixed with `AtomicInteger`.
- **R10** — `prefix.getTable().get()` without `isPresent()` guard. Fixed.
- **R11** — `trimTimeEdges` deleted all rows when all timestamps equal (minTime == maxTime). Fixed.
- **R12** — `ClassCastException` when Gson returns `Integer` instead of `Double` in agg values. Fixed with instanceof chain.
- **R13** — `c[j]` without `c.length` guard → `ArrayIndexOutOfBoundsException` on short rows. Fixed.
- **R14** — `"null"` string treated as absent for `qwindex`. Fixed.

### Low-priority fixes

- **R15** — `QwUtil.main()` with hardcoded `10.20.4.53` internal IP removed.
- **R16** — DSL parser bracket nesting (`[`/`]`) inside comma-split. Fixed.
- **R17** — `order=id:X:dir` split unbounded → `split(":", 3)`. Fixed.
- **R18** — Inconsistent `@JsonProperty` on Optional getters in `RawQuickwitQueryTableHandle`. All 5 getters annotated.
- **R19** — Logger fields not `static final`. Fixed across all QW connector classes.

### New features

- **J47/J55** — `sqlversion` deprecation plan. ADR at `docs/adr/0003-sqlversion-deprecation.md`. WARN log for `0` and `0.1` at parse time. Target mode: `0.2`.
- **J48** — `AggsDslCompiler.IdAllocator.claim()` logs WARN on auto-ID collision with preferred id, assigned id, and guidance.
- **J49** — `CAPABILITIES.md` written: agg types, auto-id rules, `order=` modifier, `replacefromcolumns` scope, sqlversion column naming table, all inline params, Quickwit 0.8 limitations.
- **J50** — `QuickwitIntegrationTest.java`: 5 sentinels (S1 HTTP, S2 aggs v0.1, S3 aggs v0.2, S4 raw hits, S5 SSRF guard). Skipped unless `QUICKWIT_TEST_URL` set.
- **J51** — Injection audit: `InjectionAuditTest.java` documents and tests attack surfaces (qwurl injection, JSON field injection, Rhino injection).
- **J52** — Multi-tenant isolation audit: `docs/adr/0004-multi-tenant-isolation.md`. Threat model with mitigations.
- **J53** — Slow-query WARN: `ConnectorBaseUtil.select()` logs `WARN SLOW_QUERY` with elapsed ms/name/index/rows when >5 s.
- **J54** — Quickwit version-compatibility matrix: `docs/quickwit-compatibility.md`.
- **J56** — Plain Table Query Mode (Phases 1 + 2). `FROM quickwit.public."<index-name>"` works without `//params`. Schema from `DocMapping.fieldMappings` via `IndexesApi`. Filter pushdown (`applyFilter()`): equality + single-range → Quickwit Lucene syntax. Limit pushdown (`applyLimit()`): `max_hits` updated, `limitGuaranteed=true`. New classes: `PlainTableQuery.java` (SPI-free helpers), schema field type mapping in `QwUtil.getColumnsFromDocMapping()`.

### Architectural fixes

- **L14** — TVF schema uyuşmazlığı ("returned table does not match the node's output") fix. `RawQuickwitQueryTableHandle` artık `computedColumns` (Optional<String>) alanı taşıyor. `analyze()` tek bir live search yapıp sonucu handle'a dondurur; `applyTableFunction()`, `getTableMetadata()`, `getColumnHandles()` bu frozen listeyi kullanır — ikinci search yapmaz.
- **L15** — Rhino Math.floor Trino plugin classloader altında fail oluyordu. İki root cause bulundu ve düzeltildi:
  1. `rhino-engine:1.8.1` ve `rhino-runtime:1.7.15.1` `rhino:1.8.1` (standalone, self-contained) ile birlikte declare edilmişti → classpath'te eski `NativeMath.class` çakışması. İkisi `pom.xml`'den kaldırıldı.
  2. Thread Context Classloader (TCL) Trino plugin classloader olarak set edilmemişti. `RhinoExecutor.executeScript()` artık `Context.enter()` öncesinde TCL'yi kaydedip `RhinoExecutor.class.getClassLoader()` ile değiştiriyor, finally'de geri alıyor.
  Smoke test (2026-05-12 Trino 479 container): `Math.floor(1777551625/1000)` → `1777551`, hata yok.
