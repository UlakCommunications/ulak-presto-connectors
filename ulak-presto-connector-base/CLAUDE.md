# ulak-presto-connector-base — CLAUDE.md

Shared base library for the Trino plugin family in this repo
(`ulak-presto-influxdb-connector`, `ulak-presto-postgres-connector`,
`ulak-presto-quickwit-connector` all depend on it). Hosts shared utilities
+ the GeoIP UDFs.

## GeoIP UDFs

[`IPToCountry.java`](src/main/java/com/facebook/presto/ulak/geolocation/IPToCountry.java)
exposes three Trino SQL UDFs backed by MaxMind `com.maxmind.geoip2:geoip2:5.0.0`
(Apache-2.0):

- `ip_to_country(varchar) → varchar`
- `ip_to_latitude(varchar) → varchar`
- `ip_to_longitude(varchar) → varchar`

Reads MMDB from hardcoded `/usr/lib/trino/plugin/GeoLite2-Country.mmdb` and
`/usr/lib/trino/plugin/GeoLite2-City.mmdb`. **Status today (2026-04-28):**

- Static block opens the readers and throws `RuntimeException` on missing
  file → entire connector fails to load. **TODO K03.**
- All lookup method bodies (`getCountryName`, `getLocation`, `ipToLatitude`,
  `ipToLongitude`, the substantive part of `ipToCountry`) are entirely
  commented out — UDFs return `null` / `""` even when MMDB is present.
  **TODO K04.**
- Path is hardcoded — customer must mount MMDB over the plugin path.
  **TODO K02.**

See [`TODO.md`](TODO.md) category K for open items.

## GeoIP data policy — never commit

MaxMind GeoLite2 EULA and IP2Location LITE EULA both **forbid
redistribution**. No `.mmdb` / IP2Location LITE `.csv` / `.zip` is
committed to any git remote. `.gitignore` (repo root) enforces this:

```
geolocation/*
!geolocation/README.md
!geolocation/ip2location/
geolocation/ip2location/IP2LOCATION-LITE-DB11.CSV/
geolocation/ip2location/IP2LOCATION-LITE-DB11.CSV.zip
*.mmdb
*.MMDB
```

Customer mounts the MMDB at runtime — see
[`geolocation/README.md`](../geolocation/README.md) for the K8s
init-container + `emptyDir` pattern (recommended) and the PVC + CronJob
alternative.

## History rewrite event (2026-04-28)

The `master` branch was rewritten with `git filter-repo` to drop binary
geolocation blobs that had leaked into one stale commit (`bbde25e Merge
branch 'develop' into 'master'`). All four push-able remotes were
force-updated:

- `origin` → gitlab.ulakhaberlesme.com.tr/maya/presto-influxdb-connector
- `github` → github.com/mfyuce/presto-influxdb-connector
- `github_ulakcom` → github.com/UlakCommunications/ulak-presto-connectors
- `gitlab_mfyuce` → gitlab.com/mfyuce/presto-influxdb-connector

`develop` and the other 22 branches + 53 tags were already clean and
were not touched. **Anyone with an old clone of `master`** must run
`git fetch origin && git reset --hard origin/master` (or re-clone).
In-progress local work on master needs manual rebase.

Pre-rewrite backup bundle: `/tmp/ulak-presto-connectors-pre-purge-20260428-100923.bundle`
on the machine where the rewrite was performed (180 MB).
