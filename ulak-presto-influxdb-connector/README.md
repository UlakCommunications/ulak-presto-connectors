# presto-influxdb-connector

It's an influxdb connector for presto, it's based on presto-0.432.

## build and run 
1. Execute "mvn package".
2. Create "influxdb" directory in ${presto-root}/plugin/
3. copy original-presto-influxdb-0.432-SNAPSHOT.jar and presto-influxdb-0.432-SNAPSHOT.jar to /{presto-root-dir}/plugin/influxdb
4. use "/bin/launcher start" to start server

## notes
1. It's only support StringType;


## Docker Local

```bash
./mvnw clean package
docker build -t 192.168.57.202:35000/trinodb/trino:432 .
docker push 192.168.57.202:35000/trinodb/trino:432
```

```bash
./mvnw clean package
docker build -t 192.168.57.202:35000/trinodb/trino:432-cache .
docker push 192.168.57.202:35000/trinodb/trino:432-cache
```

```bash
select cast(i._time as DOUBLE)*1000  as time, _measurement,cast(i._value as DOUBLE) as _value
from influxdb_monitoring.otlp_metric."
  //ttl=172800
  //refresh=3600
  //cache=true
  //eagercache=true
  //columns=_time,_measurement,_value
        
  import ""date""
  
  from(bucket: ""otlp_metric"")
  |> range(start: -24h) 
  |> aggregateWindow( every: 1h, fn: count, timeSrc: ""_start"")  
  |> group(columns: [""_time"",""_measurement""])
  |> sum() 
  |> group()
" as i
--left join postgres_simsek.public.site s on cast(s.id as varchar) = i.host 
--where s.name is null 
order by _value desc
 
```
```bash
select 0 as time, _measurement,cast(i._value as DOUBLE) as _value
from influxdb_monitoring.otlp_metric."
  //ttl=3600
  //refresh=120
  //cache=true
  //eagercache=true
  //columns=_time,_measurement,_value
        
  from(bucket: ""otlp_metric"")
  |> range(start: -1m) 
  |> aggregateWindow( every: 10m, fn: count, timeSrc: ""_start"")  
  |> group(columns: [""_time"",""_measurement""])
  |> sum() 
  |> group()
" as i
--left join postgres_simsek.public.site s on cast(s.id as varchar) = i.host 
--where s.name is null 
order by _value desc
 
```
