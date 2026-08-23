# ulak-presto-connector-base — CLAUDE.md

Shared base library for the Trino plugin family in this repo
(`ulak-presto-influxdb-connector`, `ulak-presto-postgres-connector`,
`ulak-presto-quickwit-connector` all depend on it). Hosts shared utilities
+ the GeoIP UDFs. Full history of fixes: [`CHANGELOG.md`](CHANGELOG.md).
Open items: [`TODO.md`](TODO.md).

## GeoIP UDFs

[`IPToCountry.java`](src/main/java/com/facebook/presto/ulak/geolocation/IPToCountry.java)
exposes three Trino SQL UDFs backed by MaxMind `com.maxmind.geoip2:geoip2:5.0.0`
(Apache-2.0): `ip_to_country(varchar)`, `ip_to_latitude(varchar)`,
`ip_to_longitude(varchar)`.

Reads MMDB from `ULAK_GEOIP_COUNTRY_DB` / `ULAK_GEOIP_CITY_DB` (defaults
`/usr/lib/trino/plugin/GeoLite2-{Country,City}.mmdb`). Missing/unreadable
file → `WARN` + UDFs return `""` (no crash). Accepts MaxMind GeoLite2 and
`convert.py`-produced IP2Location MMDBs alike (`geoip2:5.0.0` client is
lenient about the header).

**Never commit `.mmdb` / IP2Location LITE data** — both EULAs forbid
redistribution. `.gitignore` (repo root) enforces this; customer mounts the
MMDB at runtime (K8s init-container + `emptyDir`, see
[`geolocation/README.md`](../geolocation/README.md)).

## Redis caching (`ConnectorBaseUtil` / `RedisCacheWorker`)

- Every query result is written to Redis regardless of `//cache=`; the
  `cache` flag only changes TTL (`NONE_CACHE_TTL_IN_SECONDS`=10s vs
  `//ttl=`/`DEFAULT_CACHE_TTL`=24h). This is what makes an uncached query
  still look "instant on the second hit" — it's a dedupe cache, not
  intentional long-term caching.
- `RedisCacheWorker` proactively refreshes `cache=true` entries in the
  background. On a failed refresh it deletes the key (does not resurrect
  a stale/erroring entry by extending its TTL). Entries idle longer than
  `QueryParameters.idleTtlInSeconds` (default 6h, `//idlettl=`) get
  evicted instead of refreshed.
- **Multi-catalog isolation**: several catalogs of the same `DBType` (e.g.
  three `mayapostgres` catalogs pointed at different Postgres databases)
  share one Redis keyspace. `QueryParameters.connectionId` (set to the
  catalog's own `pgUrl`/`qwUrl`/influx `url` at execution time) lets
  `RedisCacheWorker` skip entries that belong to a *different* catalog's
  connection — without it, one catalog's worker could refresh another
  catalog's cached query against the wrong database.

## Known architectural note

Multi-catalog registration bug (six classes captured the first catalog's
config via a static singleton) was fixed 2026-04-30 (`L04a`). Remaining
static state on `ConnectorBaseUtil` (Redis URL, keywords, worker id) is
shared across catalogs on purpose — only bites if two catalogs need
*different* Redis URLs, deferred until a customer needs it (`L04b`).

`master` branch was rewritten 2026-04-28 (`git filter-repo`) to drop
leaked GeoIP binaries; an old clone of `master` needs
`git fetch origin && git reset --hard origin/master`. `develop` was
never affected.
