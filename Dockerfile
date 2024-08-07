FROM trinodb/trino:432

COPY presto-ulak-influxdb-connector/target/presto-ulak-influxdb-0.432-SNAPSHOT.jar /usr/lib/trino/plugin/UlakInfluxdbConnector/
COPY presto-ulak-postgres-connector/target/presto-ulak-postgres-0.432-SNAPSHOT.jar /usr/lib/trino/plugin/UlakPostgresConnector/
COPY presto-ulak-quickwit-connector/target/presto-ulak-quickwit-0.432-SNAPSHOT.jar /usr/lib/trino/plugin/QuickwitConnector/