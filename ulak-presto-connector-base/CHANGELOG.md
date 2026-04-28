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

- **K07 — `IPToCountry.java`: off `geoip2:5.x` deprecated APIs.**
  Switched from `getCountry()` / `getName()` / `getLocation()` /
  `getLatitude()` / `getLongitude()` to the record-style accessors
  `country()`, `name()`, `location()`, `latitude()`, `longitude()`.
  Compile is now warning-free (`@Deprecated(forRemoval=true)` notices
  gone), unblocks future bump to geoip2 6.x.
- **K04-followup — smoke test + Trino E2E against real MMDB.**
  Standalone Java test (`/tmp/GeoTest.java`, not committed) loaded each
  MMDB and queried `8.8.8.8` / `1.1.1.1` / `212.156.4.5`. Then a full
  Trino E2E via `docker-compose.yml` (uncommenting the GeoIP mount +
  env-var lines) confirmed the UDFs over SQL:
  `SELECT ip_to_country('8.8.8.8')` → `"United States"`, lat 37.751,
  lon -97.822. `ip_to_country('212.156.4.5')` → `"Türkiye"`, lat
  41.0145, lon 28.9533. `ip_to_country('not-an-ip')` → `""`
  (graceful). Standalone results:
  - `GeoLite2-Country.mmdb` (database_type `GeoLite2-Country`):
    ✓ correctly returns "United States" / "Türkiye" (1.1.1.1 unmapped).
  - `GeoLite2-City.mmdb` (database_type `GeoLite2-City`):
    ✓ correct country + lat/lon for the same IPs.
  - `IP2LOCATION-LITE-DB11.CSV.MMDB` (database_type `IP2LITE-City`,
    produced by `geolocation/ip2location/convert.py`):
    ✓ **accepted** by `geoip2:5.0.0` despite the non-`GeoIP2-*`
    `database_type` header — both `country()` and `city()` calls
    succeed. Country names follow IP2Location's spelling
    ("United States of America", "Turkiye"). 1.1.1.1 (Cloudflare)
    actually resolves to Australia here, where MaxMind's free LITE
    returned null.
  Conclusion: the K04 risk note about `IP2LITE-*` rejection was
  unfounded; **`convert.py` does not need a header fix**.

- **K06 — Dependabot triage on UlakCommunications GitHub mirror.**
  Pulled the alert list via `gh api`: 11 total alerts, 7 already
  fixed historically. The 4 still-open alerts are all **the same
  CVE-2025-66453 (`GHSA-3w8q-xq97-5j7x`, severity LOW)** —
  Rhino DoS via `toFixed()` on attacker-controlled floats — flagged
  once per `pom.xml` (base + 3 connectors). Bumped:
  - `org.mozilla:rhino` 1.8.0 → 1.8.1
  - `org.mozilla:rhino-engine` 1.8.0 → 1.8.1
  - `org.mozilla:rhino-runtime` 1.7.15 → 1.7.15.1 *(no 1.8.x line
    exists for this artifact on Maven Central; 1.7.15.1 is the
    upstream patch)*
  Build still passes (`mvn compile` over base + 3 connectors).
  Dependabot will close the 4 alerts automatically when the new
  versions land in the default branch.

### Pending follow-up (still in TODO)

- **K05** — license attribution audit on Grafana panels in
  `backend/anomaly`.
- **K06** — dependabot vulnerability triage on
  `UlakCommunications/ulak-presto-connectors` GitHub mirror.
