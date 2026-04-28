# GeoIP databases — local workspace, **not committed**

The Trino plugin in
[`ulak-presto-connector-base/.../IPToCountry.java`](../ulak-presto-connector-base/src/main/java/com/facebook/presto/ulak/geolocation/IPToCountry.java)
exposes SQL UDFs (`ip_to_country()`, `ip_to_latitude()`, `ip_to_longitude()`)
backed by the MaxMind `com.maxmind.geoip2:geoip2:5.0.0` Java library. The
library reads MMDB files; two compatible data sources work:

1. **MaxMind GeoLite2** (`GeoLite2-Country.mmdb` + `GeoLite2-City.mmdb`) —
   the reference format the `geoip2` lib was built for.
2. **IP2Location LITE DB11** (free), converted to the GeoLite2 binary
   format with the local `convert.py` / `convert_v1.py` scripts. The
   converter writes a MaxMind-compatible header so the same
   `DatabaseReader` code path reads it.

> **Both data sources are license-restricted.** MaxMind GeoLite2 needs an
> accepted EULA + an account-level license key for download / auto-update
> (no public redistribution). IP2Location LITE explicitly forbids
> redistribution: *"You are not permitted to redistribute or resell this
> product."* Therefore **no `.mmdb` / `.csv` / `.zip` is committed to any
> git remote** — `.gitignore` enforces this. Only the conversion scripts
> (our code) and this README are tracked.

The data files belong in this folder on a developer workstation while
testing locally, and are mounted into the Trino container at runtime —
see [Runtime mount](#runtime-mount) below.

## Download

### MaxMind GeoLite2

1. Sign up (free) at https://www.maxmind.com/en/geolite2/signup, generate
   a license key.
2. Download `GeoLite2-Country` and `GeoLite2-City` `.tar.gz` packages.
3. Extract both `.mmdb` files into `geolocation/maxmind/<NAME_DATE>/`
   (gitignored).
4. Auto-update: use `geoipupdate` daemon with the license key — see
   [Pattern A](#pattern-a--emptydir--geoipupdate-init-container-recommended).

### IP2Location LITE DB11 (alternative)

1. Sign up (free) at
   https://lite.ip2location.com/database/db11-ip-country-region-city-latitude-longitude-zipcode-timezone
2. Download the CSV pack (`IP2LOCATION-LITE-DB11.CSV.zip`).
3. Drop it into `geolocation/ip2location/` (gitignored).
4. Convert to GeoLite2-format MMDB:

   ```bash
   cd geolocation/ip2location
   unzip IP2LOCATION-LITE-DB11.CSV.zip
   python3 convert.py IP2LOCATION-LITE-DB11.CSV/IP2LOCATION-LITE-DB11.CSV
   # → IP2LOCATION-LITE-DB11.CSV/IP2LOCATION-LITE-DB11.CSV.MMDB
   ```

   `convert_v1.py` is the older single-buffer variant; `convert.py` is
   the incremental writer (less RAM on big DBs). Output is identical.

5. Required attribution per IP2Location LITE terms — see
   [License attribution](#license-attribution).

## Runtime mount

`IPToCountry.java` reads MMDB from configurable paths:

| Env var | Default |
|---|---|
| `ULAK_GEOIP_COUNTRY_DB` | `/usr/lib/trino/plugin/GeoLite2-Country.mmdb` |
| `ULAK_GEOIP_CITY_DB`    | `/usr/lib/trino/plugin/GeoLite2-City.mmdb` |

If the file is missing or fails to open, the connector logs a `WARN`
and the GeoIP UDFs return `""` — the rest of the catalog keeps working
without GeoIP enrichment. No restart loop on a missing data file.

The MMDB files are 6–25 MB each — too large for a `ConfigMap` or a
`Secret` (both etcd-backed, 1 MB practical limit). Use one of:

| Mount style | When to pick it |
|---|---|
| **`emptyDir` + `geoipupdate` init/sidecar** | **Recommended.** Pod lifetime cache, refreshed on every pod start. Smallest moving parts; no PVC to provision. |
| **PVC + `geoipupdate` CronJob** | When many Trino workers share one updated DB and you do not want each pod to fetch its own copy on start. |
| **`hostPath`** | Single-node lab / dev. Operator drops the file once, every pod reads it. Not portable, not auto-updating. |

### Pattern A — `emptyDir` + MaxMind `geoipupdate` init-container (recommended)

`geoipupdate` is the MaxMind-supplied updater. License key (free, signup
required) goes in a Secret. Init-container downloads on pod start; Trino
reads from the shared `emptyDir`. No data lands on disk outside the pod,
no PVC to manage.

```yaml
# Secret with the MaxMind license key (kept off git)
apiVersion: v1
kind: Secret
metadata:
  name: maxmind-license
type: Opaque
stringData:
  GEOIPUPDATE_ACCOUNT_ID: "1234567"
  GEOIPUPDATE_LICENSE_KEY: "REPLACE_ME"
  GEOIPUPDATE_EDITION_IDS: "GeoLite2-Country GeoLite2-City"
---
# Trino coordinator (or worker) Deployment snippet
apiVersion: apps/v1
kind: Deployment
metadata:
  name: trino-coordinator
spec:
  template:
    spec:
      volumes:
        - name: geoip-data
          emptyDir:
            sizeLimit: 100Mi
      initContainers:
        - name: geoipupdate
          image: ghcr.io/maxmind/geoipupdate:v7
          envFrom:
            - secretRef: { name: maxmind-license }
          env:
            - name: GEOIPUPDATE_DB_DIR
              value: /data
          volumeMounts:
            - { name: geoip-data, mountPath: /data }
      containers:
        - name: trino
          image: <registry>/trinodb/trino:479
          env:
            - { name: ULAK_GEOIP_COUNTRY_DB, value: /data/GeoLite2-Country.mmdb }
            - { name: ULAK_GEOIP_CITY_DB,    value: /data/GeoLite2-City.mmdb }
          volumeMounts:
            - { name: geoip-data, mountPath: /data, readOnly: true }
```

Add a daily refresh by promoting the init-container to a sidecar with a
sleep loop, or — preferred — keep the init-container and rely on
`kubectl rollout restart` from a CronJob. The MMDB files are small;
re-downloading on every pod restart is fine (~7 MB country, ~25 MB city,
< 5 s on a normal link).

### Pattern B — PVC + `geoipupdate` CronJob (multi-pod / cold-start sensitive)

Use when many Trino workers read the same DB and you do not want each pod
to fetch its own copy on start.

```yaml
# 1. PVC (1Gi is plenty)
apiVersion: v1
kind: PersistentVolumeClaim
metadata: { name: geoip-data }
spec:
  accessModes: [ReadWriteMany]      # if storage class supports it; else ReadOnlyMany after first write
  resources: { requests: { storage: 1Gi } }
---
# 2. CronJob that runs geoipupdate daily
apiVersion: batch/v1
kind: CronJob
metadata: { name: geoipupdate }
spec:
  schedule: "0 4 * * *"              # 04:00 UTC every day
  concurrencyPolicy: Forbid
  jobTemplate:
    spec:
      template:
        spec:
          restartPolicy: OnFailure
          volumes:
            - name: geoip-data
              persistentVolumeClaim: { claimName: geoip-data }
          containers:
            - name: geoipupdate
              image: ghcr.io/maxmind/geoipupdate:v7
              envFrom:
                - secretRef: { name: maxmind-license }
              env:
                - { name: GEOIPUPDATE_DB_DIR, value: /data }
              volumeMounts:
                - { name: geoip-data, mountPath: /data }
---
# 3. Trino pod mounts the PVC read-only
spec:
  template:
    spec:
      volumes:
        - name: geoip-data
          persistentVolumeClaim:
            claimName: geoip-data
            readOnly: true
      containers:
        - name: trino
          env:
            - { name: ULAK_GEOIP_COUNTRY_DB, value: /data/GeoLite2-Country.mmdb }
            - { name: ULAK_GEOIP_CITY_DB,    value: /data/GeoLite2-City.mmdb }
          volumeMounts:
            - { name: geoip-data, mountPath: /data, readOnly: true }
```

### Pattern C — IP2Location LITE (no official auto-updater)

IP2Location LITE has no `geoipupdate` equivalent. Three options:

1. **Cron-driven internal mirror** — a CI runner downloads the LITE
   `.zip`, runs `geolocation/ip2location/convert.py` to produce a
   GeoLite2-format MMDB, uploads to private MinIO/Nexus/S3. An
   init-container in the Trino pod downloads from that internal URL,
   drops into the `emptyDir`. Same mount paths as MaxMind — `IPToCountry.java`
   does not care which source produced the MMDB.
2. **Manual swap** — operator runs the convert step on a workstation,
   `kubectl cp` into a PVC; rotate when stale.
3. **Skip** — leave the MMDB files absent. **Note:** today this prevents
   the connector from loading at all, see [Graceful degrade](#graceful-degrade)
   below.

If you do mirror IP2Location LITE inside the company, surface the
required attribution string in whatever UI displays country / lat / lon
data — see [License attribution](#license-attribution).

## Graceful degrade

If neither file is present, GeoIP enrichment is silently disabled —
`IPToCountry.openReader()` logs a `WARN`, the readers stay `null`, and
the three UDFs (`ip_to_country`, `ip_to_latitude`, `ip_to_longitude`)
return empty strings. The catalog keeps working.

## License attribution

Wherever this connector's data surfaces in downstream UIs (Grafana
dashboards, status pages, reports), include the following — both clauses
are mandatory per the respective LITE / GeoLite2 EULAs:

- IP2Location LITE: *"This product uses IP2Location LITE data
  ([https://lite.ip2location.com](https://lite.ip2location.com))."*
- MaxMind GeoLite2: *"This product includes GeoLite2 data created by
  MaxMind ([https://www.maxmind.com](https://www.maxmind.com))."*
