FROM 192.168.57.202:35000/trinodb/trino:432

COPY ./geolocation/maxmind/GeoLite2-Country_20240917/GeoLite2-Country.mmdb /usr/lib/trino/plugin/
COPY ./geolocation/maxmind/GeoLite2-City_20240917/GeoLite2-City.mmdb /usr/lib/trino/plugin/

COPY ./geolocation/ip2location/IP2LOCATION-LITE-DB11.CSV/IP2LOCATION-LITE-DB11.CSV.MMDB /usr/lib/trino/plugin/
##COPY GeoLite2-City_20240917/GeoLite2-City.mmdb /usr/lib/trino/plugin/
COPY ulak-presto-influxdb-connector/target/ulak-presto-influxdb-connector-0.432-SNAPSHOT.jar /usr/lib/trino/plugin/UlakInfluxdbConnector/
COPY ulak-presto-postgres-connector/target/ulak-presto-postgres-connector-0.432-SNAPSHOT.jar /usr/lib/trino/plugin/UlakPostgresConnector/
COPY ulak-presto-quickwit-connector/target/ulak-presto-quickwit-connector-0.432-SNAPSHOT.jar /usr/lib/trino/plugin/QuickwitConnector/