# Internal registry (default for local/cluster builds).
# Public/CI build: docker build --build-arg TRINO_BASE=trinodb/trino:479 .
ARG TRINO_BASE=192.168.57.202:35000/trinodb/trino:479
FROM ${TRINO_BASE}

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
    ! -name 'elasticsearch' \
    -exec rm -rf {} +


# GeoIP MMDB files are not baked into the image — license-restricted, mounted at
# runtime via the K8s manifest. See geolocation/README.md.

COPY ulak-presto-influxdb-connector/target/ulak-presto-influxdb-connector-0.479-SNAPSHOT.jar /usr/lib/trino/plugin/UlakInfluxdbConnector/
COPY ulak-presto-postgres-connector/target/ulak-presto-postgres-connector-0.479-SNAPSHOT.jar /usr/lib/trino/plugin/UlakPostgresConnector/
COPY ulak-presto-quickwit-connector/target/ulak-presto-quickwit-connector-0.479-SNAPSHOT.jar /usr/lib/trino/plugin/QuickwitConnector/

#COPY openapi/trino-openapi-1.86-SNAPSHOT.jar /usr/lib/trino/plugin/trino-openapi/
#COPY openapi/trino-openapi-1.86-SNAPSHOT.zip /usr/lib/trino/plugin/trino-openapi/
#COPY openapi/trino-openapi-1.86-SNAPSHOT-services.jar /usr/lib/trino/plugin/trino-openapi/
COPY openapi/plugin/trino-openapi-1.86-SNAPSHOT/ /usr/lib/trino/plugin/trino-openapi/
#COPY openapi/trino-openapi-1.86-SNAPSHOT-sources.jar /usr/lib/trino/plugin/trino-openapi/
#COPY openapi/trino-openapi-1.86-SNAPSHOT-test-sources.jar /usr/lib/trino/plugin/trino-openapi/
#COPY openapi/trino-openapi-1.86-SNAPSHOT-tests.jar /usr/lib/trino/plugin/trino-openapi/

USER trino

