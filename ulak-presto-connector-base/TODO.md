# TODO — ulak-presto-connector-base

## GeoIP cleanup (2026-04-28) — connector-base scope

After the master history rewrite (binary geolocation purge) and the
`geolocation/` workspace overhaul. Context in
[`CLAUDE.md`](CLAUDE.md).

- [ ] **K01 [P1, 0.3d] Dockerfile — remove binary `COPY` lines** —
      [`Dockerfile`](../Dockerfile) lines 28-31 still copy
      `./geolocation/maxmind/...mmdb` and
      `./geolocation/ip2location/IP2LOCATION-LITE-DB11.CSV/IP2LOCATION-LITE-DB11.CSV.MMDB`
      into the image. Source files are now `.gitignore`'d so
      `docker build` fails on those COPY steps. Drop the three lines
      (leave the `*.jar` COPYs). Image becomes data-less; customer mounts
      MMDB at runtime per `geolocation/README.md`.

- [ ] **K02 [P1, 0.5d] `IPToCountry.java` — make MMDB path configurable** —
      hardcoded `/usr/lib/trino/plugin/GeoLite2-{Country,City}.mmdb` forces
      customers to mount over the plugin path. Accept env vars
      (`ULAK_GEOIP_COUNTRY_DB`, `ULAK_GEOIP_CITY_DB`) or Trino catalog
      properties so the K8s manifest can mount to `/data/...mmdb` cleanly.

- [ ] **K03 [P1, 0.3d] `IPToCountry.java` — graceful fallback when MMDB
      missing** — static block currently throws `RuntimeException` if the
      file does not exist → entire connector fails to load. Make
      `DatabaseReader` initialisation lazy (or catch `IOException` and
      keep the readers `null`); UDFs return `""` / `null` instead of
      crashing the plugin.

- [ ] **K04 [P1, 0.5d] `IPToCountry.java` — uncomment real lookup logic** —
      bodies of `getCountryName`, `getLocation`, `ipToLatitude`,
      `ipToLongitude`, and the substantive part of `ipToCountry` are
      entirely in comments (returning `null`/`""`). Re-enable; verify
      against IP2Location-converted MMDB. Risk: `convert.py` writes
      `database_type` `IP2LITE-Country` / `IP2LITE-City` — if
      `geoip2:5.0.0` rejects non-`GeoIP2-*` types in `country()`/`city()`,
      either fix `convert.py` to write `GeoIP2-Country` / `GeoIP2-City`
      headers, or drop to low-level `com.maxmind.db.Reader.get()`.

- [ ] **K05 [P2, 0.3d] License attribution audit** — Grafana panels and
      reports surfacing GeoIP data must include the EULA-mandated strings:
      *"This product uses IP2Location LITE data
      ([https://lite.ip2location.com](https://lite.ip2location.com))."*
      and *"This product includes GeoLite2 data created by MaxMind
      ([https://www.maxmind.com](https://www.maxmind.com))."* Audit
      downstream observability dashboards in `backend/anomaly` for
      compliance.

- [ ] **K06 [P1, 1d] Dependabot vulnerability triage** — GitHub flagged
      7 vulnerabilities (2 moderate, 5 low) on the
      `UlakCommunications/ulak-presto-connectors` default branch:
      <https://github.com/UlakCommunications/ulak-presto-connectors/security/dependabot>.
      Read each, triage transitive vs direct, bump the affected
      `pom.xml` versions across `ulak-presto-connector-base` and the four
      connectors as needed.
