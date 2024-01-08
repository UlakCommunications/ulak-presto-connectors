# presto-influxdb-connector

It's an influxdb connector for presto, it's based on presto-0.279.

## build and run 
1. Execute "mvn package".
2. Create "influxdb" directory in ${presto-root}/plugin/
3. copy original-presto-influxdb-0.435-SNAPSHOT.jar and presto-influxdb-0.280-SNAPSHOT.jar to /{presto-root-dir}/plugin/influxdb
4. use "/bin/launcher start" to start server

## notes
1. It's only support DoubleType, IntType and TimeStampType;
