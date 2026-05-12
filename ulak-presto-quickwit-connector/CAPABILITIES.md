# Quickwit Connector — Capabilities Reference

Quickwit version tested: **0.8.2**  
Connector version: see `pom.xml`

---

## Aggs DSL — supported aggregation types

| Type | Syntax | Notes |
|------|--------|-------|
| `histogram` | `histogram(field, interval=N)` | Auto-id: `"date"` |
| `date_histogram` | `histogram(field, interval=Nd/h/m)` | interval suffix: `d`, `h`, `m`, `s` |
| `terms` | `terms(field, size=N)` | Auto-id: last segment of field name after last `.` |
| `max` | `max(field)` | Metric leaf |
| `min` | `min(field)` | Metric leaf |
| `sum` | `sum(field)` | Metric leaf |
| `avg` | `avg(field)` | Metric leaf |
| `count` | `count(field)` | Metric leaf; Quickwit returns as `doc_count` |
| `top_hits` | — | **NOT supported** in Quickwit 0.8 — removed from all dashboards |

---

## Auto-ID rules

- `histogram` → always gets id `"date"`
- `terms` / metric → id = last segment after last `.` in field name  
  (e.g. `span_attributes.m_src` → id `m_src`)
- Collision: numeric fallback (`1`, `2`, …) with WARN log — see J48
- Explicit id override: prefix with `id=<name>:` in the DSL token

---

## `order=` modifier

Only valid inside `terms()`. Format:

```
terms(field, size=N, order=<metricId>:<direction>)
```

- `<metricId>` must be a metric aggregation id defined in the **same** terms bucket
- `<direction>`: `asc` or `desc`
- Shorthand `order=_key:asc` sorts by bucket key

The compiler splits `order=id:X:dir` with `split(":", 3)` — id may contain `:` characters.

---

## `replacefromcolumns` — sqlversion `0` only

```
//replacefromcolumns=/3/buckets/2/buckets/1
```

Strips the given path prefix from all column names returned by JFlat.  
Only applies when `//sqlversion=0` (legacy). **Deprecated** — migrate to `sqlversion=0.2`.

---

## sqlversion column naming

| sqlversion | Key column | Key-as-string | Metric value |
|-----------|-----------|---------------|-------------|
| `0` (deprecated) | `/aggId/key` (after replacefromcolumns strip) | `/aggId/key_as_string` | `/aggId/value` |
| `0.1` (deprecated) | `aggId/key` | `aggId/key_as_string` | `aggId/value` |
| `0.2` (**target**) | `aggId` | `aggId_str` | `aggId` |

See `docs/adr/0003-sqlversion-deprecation.md` for migration guide.

---

## Inline query parameters

| Parameter | Effect |
|-----------|--------|
| `//qwindex=<name>` | Override catalog default index |
| `//qwurl=<url>` | Override catalog URL (must be in `qw-allowed-urls` allowlist — see R02) |
| `//sqlversion=<v>` | `0`, `0.1`, `0.2` (default: `0`) |
| `//cache=true` | Enable Redis result cache |
| `//ttl=<sec>` | Redis TTL (default: 10 s) |
| `//refresh=<sec>` | Background refresh interval |
| `//name=<text>` | Query label (shown in Trino query log) |
| `//columns=<csv>` | Fallback column list when no data returned |
| `//replacefromcolumns=<path>` | Path prefix strip (sqlversion=0 only) |
| `//hasjs=true` | Enable Rhino JS pre-processing of query string |
| `//from=<unix>` | Start timestamp (seconds) for time-range cache key |
| `//to=<unix>` | End timestamp (seconds) for time-range cache key |

---

## Known Quickwit 0.8 limitations

- No `top_hits` aggregation
- No `percentiles` / `extended_stats` aggregations
- `max_hits=0` required when using aggs-only queries (no raw hits needed)
- Timestamp fields must be in nanoseconds for `span_start_timestamp_nanos`-style indexes
- `IN [v1 v2 v3]` syntax: values space-separated, not comma-separated
