# Quickwit version compatibility matrix

## Current lock

| Component | Version |
|-----------|---------|
| Quickwit server | **0.8.2** |
| quickwit-java-client | `1.0.3-SNAPSHOT` (local, generated from Quickwit 0.8.2 OpenAPI spec) |
| Trino | 479 |
| Connector | see `pom.xml` |

The Java client (`org.quickwit:quickwit-java-client`) is generated from
Quickwit's OpenAPI spec and is **not published to Maven Central** — it lives
in the local/internal Maven repo only.

---

## Compatibility history

| Connector version | Quickwit server | Notes |
|-------------------|-----------------|-------|
| ≤ 3.0.x | 0.7.x | `SearchResponseRest` shape differs; `aggregations` field absent in some 0.7 responses |
| 3.1.x (current) | **0.8.2** | Tested. Production deployment on `yucemonitoring` cluster |

---

## Upgrade risk list: 0.8 → 0.9 / 1.0

Based on Quickwit changelog and API diff analysis:

| Area | Risk | Details |
|------|------|---------|
| **OpenAPI spec changes** | High | Java client must be regenerated from new spec. Any renamed/removed field in `SearchResponseRest`, `VersionedIndexMetadata`, or `FieldMappingEntryForSerialization` will break at runtime (NPE or silent empty result). |
| **`aggregations` field** | Medium | Quickwit 0.8 returns `aggregations` as `Object` (untyped map). If 0.9/1.0 changes the shape of bucket/metric nodes, `traverseAggregations()` will silently produce wrong columns. |
| **`getIndexesMetadatas()` response** | Medium | `VersionedIndexMetadata.getVersionedIndexMetadataOneOf()` chain is deep. Any restructuring breaks `getSchemas()` / `getTableNames()`. |
| **`IN [v1 v2]` query syntax** | Low | Quickwit 0.9 may change multi-value filter syntax. `replaceTrinoQWVars()` handles current normalisation; verify after upgrade. |
| **Timestamp field precision** | Low | Current dashboards use nanosecond fields (`span_start_timestamp_nanos`). If 0.9 normalises to milliseconds, `from_unixtime(cast(... as double)/1000000000)` expressions break. |
| **`top_hits` support** | Opportunity | Quickwit 0.9+ may add `top_hits`. Can replace the current workaround of using `max_by(field, timestamp)` in Trino. |

---

## Upgrade procedure

1. Pull new Quickwit OpenAPI spec from the 0.9/1.0 release tag
2. Regenerate `quickwit-java-client` using the same generator config
3. Fix compilation errors (renamed fields, changed return types)
4. Run `./mvnw test` — existing unit tests catch shape regressions in `parseResponse` / `traverseAggregations`
5. Deploy to staging, run the integration test suite (J50) against real Quickwit 0.9
6. Verify the BFD / Throughput / Anomaly Grafana dashboards end-to-end

---

## Notes

- The client is generated, not hand-written. Breaking changes in the OpenAPI
  spec produce compile errors (good) rather than silent runtime failures.
- `SearchResponseRest.getAggregations()` returns `Object` — this is the
  one untyped surface where a shape change would be a runtime failure, not
  a compile error. Add a regression test for the aggregation shape after
  each upgrade.
