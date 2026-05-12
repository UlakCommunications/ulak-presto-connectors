# ADR 0003 — sqlversion deprecation plan

**Date:** 2026-05-12  
**Status:** Accepted  
**Deciders:** Fatih Yuce

---

## Context

The Quickwit connector has three parallel aggregation-parsing code paths, selected by the `//sqlversion=` query parameter:

| Mode | Column naming | Implementation |
|------|--------------|----------------|
| `0` (legacy) | `/3/buckets/2/key` — full JFlat path | `parseResponseHits()` + JFlat + `replacefromcolumns` strip |
| `0.1` | `aggId/key`, `aggId/value`, `aggId/key_as_string` | `traverseAggregations(stripSuffixes=false)` |
| `0.2` | `aggId`, `aggId_str` | `traverseAggregations(stripSuffixes=true)` |

Three parallel paths mean:
- Every bug fix must be applied (or consciously skipped) in all three paths
- Dashboard authors must know which mode they're targeting
- New features (e.g., Plain Table Query Mode J56) only need to be implemented once if there is a single target

## Decision

**Target mode: `0.2`** (bare aggId column names).

Reasons:
1. Shortest, most readable column names in Grafana field overrides
2. `traverseAggregations` is the newer, cleaner code path — already handles integer key formatting, doc_count, sum_other_doc_count correctly
3. `0.1` is an intermediate step with no production dashboards known to use it exclusively
4. `0` (JFlat) carries a hard dependency on the `jflat` library and `replacefromcolumns` path magic; removing it eliminates ~200 lines of code and one transitive dependency

**Deprecation timeline:**

| Phase | Version | Action |
|-------|---------|--------|
| Now | current | Log `WARN` when `sqlversion=0` or `sqlversion=0.1` is used |
| Next minor | +1 | Print deprecation notice in connector startup if any cached query uses `0`/`0.1` |
| Future major | +2 | Remove `sqlversion=0` and `sqlversion=0.1` code paths; `0.2` becomes default |

## Migration guide

### From `sqlversion=0` (JFlat + replacefromcolumns)

Old query uses:
```
//sqlversion=0
//replacefromcolumns=/3/buckets/2/buckets/1
```
And references columns like `/key`, `/value`, `/key_as_string`.

New query:
```
//sqlversion=0.2
```
Remove `//replacefromcolumns=` entirely. Column references change:

| Old (`0`) | New (`0.2`) |
|-----------|-------------|
| `"/3/key"` | `"3"` |
| `"/3/key_as_string"` | `"3_str"` |
| `"/1/value"` | `"1"` |
| `"/value"` (root metric) | `"1"` (metric aggId) |

### From `sqlversion=0.1`

Old column references:

| Old (`0.1`) | New (`0.2`) |
|-------------|-------------|
| `"3/key"` | `"3"` |
| `"3/key_as_string"` | `"3_str"` |
| `"1/value"` | `"1"` |

Remove the `/key`, `/value`, `/key_as_string` suffixes from all column references.

## Consequences

- All new dashboards should use `sqlversion=0.2`
- Existing dashboards on `0` require column-reference updates in Grafana field overrides and SQL aliases
- `0.1` dashboards need minor suffix removal
- JFlat dependency (`com.github.opendevl:jflat`) can be removed in the future major release
