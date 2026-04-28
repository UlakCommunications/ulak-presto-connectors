# Changelog — ulak-presto-connector-base

Items move here from [`TODO.md`](TODO.md) when finished. Keep entries
concrete enough that a future reader can locate the change without
chasing commits.

## 2026-04-28 — GeoIP cleanup

After the repo-wide history rewrite (`git filter-repo` over all 28
branches + 53 tags) that purged license-restricted binary geolocation
data, the connector-base GeoIP layer was made functional and safe to
ship without baked-in `.mmdb` files. See
[`CLAUDE.md`](CLAUDE.md#history-rewrite-event-2026-04-28) for the
history-rewrite event details.

### Done

- **K01 — Dockerfile: drop binary GeoIP `COPY` lines.** The image is
  now data-less; the customer mounts MMDB at runtime per
  [`geolocation/README.md`](../geolocation/README.md).
- **K02 — `IPToCountry.java`: configurable MMDB path.** New env vars
  `ULAK_GEOIP_COUNTRY_DB` and `ULAK_GEOIP_CITY_DB`; defaults still
  `/usr/lib/trino/plugin/GeoLite2-{Country,City}.mmdb` so existing
  deployments keep working without config change.
- **K03 — `IPToCountry.java`: graceful fallback.** `openReader` logs
  a `WARN` and returns `null` when the file is missing or fails to
  open; lookups short-circuit on `null` reader; UDFs return `""`. The
  connector loads cleanly without MMDB instead of throwing
  `RuntimeException` at class-load.
- **K04 — `IPToCountry.java`: real lookup logic re-enabled.** All UDFs
  (`ip_to_country`, `ip_to_latitude`, `ip_to_longitude`) now hit
  `DatabaseReader.country()` / `.city()` and surface real values.
  `IOException` and `GeoIp2Exception` (incl. `AddressNotFoundException`,
  `InvalidDatabaseException`) caught at debug level → empty result, no
  crash. **Runtime test against real MMDB still outstanding** — tracked
  inline on the K04 row in TODO.md as a follow-up note.

### Pending follow-up (still in TODO)

- **K05** — license attribution audit on Grafana panels in
  `backend/anomaly`.
- **K06** — dependabot vulnerability triage on
  `UlakCommunications/ulak-presto-connectors` GitHub mirror.
- **K07** — migrate off `geoip2:5.x` deprecated APIs before bumping to
  6.x.
