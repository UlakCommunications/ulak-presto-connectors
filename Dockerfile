#FROM 192.168.57.202:35000/trinodb/trino:432
FROM trinodb/trino:478

USER root

# Keep the image up-to-date for security scanners (RHEL/UBI style)
RUN set -eux; \
    if command -v dnf >/dev/null 2>&1; then \
        dnf -y upgrade --refresh && dnf clean all; \
    elif command -v microdnf >/dev/null 2>&1; then \
        microdnf update -y && microdnf clean all; \
    elif command -v yum >/dev/null 2>&1; then \
        yum -y update && yum clean all; \
    else \
        echo "No dnf/microdnf/yum found; skipping OS upgrade" >&2; \
    fi; \
    rm -rf /var/cache/dnf /var/cache/yum /var/cache/microdnf || true


RUN find /usr/lib/trino/plugin \
    -mindepth 1 -maxdepth 1 -type d \
    ! -name 'postgresql' \
    ! -name 'exchange-filesystem' \
    ! -name 'geospatial' \
    -exec rm -rf {} +


COPY ./geolocation/maxmind/GeoLite2-Country_20240917/GeoLite2-Country.mmdb /usr/lib/trino/plugin/
COPY ./geolocation/maxmind/GeoLite2-City_20240917/GeoLite2-City.mmdb /usr/lib/trino/plugin/

COPY ./geolocation/ip2location/IP2LOCATION-LITE-DB11.CSV/IP2LOCATION-LITE-DB11.CSV.MMDB /usr/lib/trino/plugin/
##COPY GeoLite2-City_20240917/GeoLite2-City.mmdb /usr/lib/trino/plugin/
COPY ulak-presto-influxdb-connector/target/ulak-presto-influxdb-connector-0.478-SNAPSHOT.jar /usr/lib/trino/plugin/UlakInfluxdbConnector/
COPY ulak-presto-postgres-connector/target/ulak-presto-postgres-connector-0.478-SNAPSHOT.jar /usr/lib/trino/plugin/UlakPostgresConnector/
COPY ulak-presto-quickwit-connector/target/ulak-presto-quickwit-connector-0.478-SNAPSHOT.jar /usr/lib/trino/plugin/QuickwitConnector/

