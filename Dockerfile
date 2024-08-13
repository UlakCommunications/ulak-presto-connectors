FROM trinodb/trino:432

COPY ulak-presto-influxdb-connector/target/ulak-presto-influxdb-0.432-SNAPSHOT.jar /usr/lib/trino/plugin/UlakInfluxdbConnector/
COPY ulak-presto-postgres-connector/target/ulak-presto-postgres-0.432-SNAPSHOT.jar /usr/lib/trino/plugin/UlakPostgresConnector/
COPY ulak-presto-quickwit-connector/target/ulak-presto-quickwit-0.432-SNAPSHOT.jar /usr/lib/trino/plugin/QuickwitConnector/