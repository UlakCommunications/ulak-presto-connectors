# presto-influxdb-connector

It's an influxdb connector for presto, it's based on presto-0.279.

## build and run 
1. Execute "mvn package".
2. Create "influxdb" directory in ${presto-root}/plugin/
3. copy original-presto-influxdb-0.432-SNAPSHOT.jar and presto-influxdb-0.280-SNAPSHOT.jar to /{presto-root-dir}/plugin/influxdb
4. use "/bin/launcher start" to start server

## notes
1. It's only support DoubleType, IntType and TimeStampType;


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


```
 keywords=application_id:APPLICATION_ID;proto:PROTO;total_packet_len:TOTAL_PACKET_LEN;total_packet:TOTAL_PACKET;src:SRC;dst:DST;application_tag:APPLICATION_TAG;dpt:DPT;dst:DST;end_reason:END_REASON;end_time:END_TIME;id:ID;list_id:LIST_ID;log_type:LOG_TYPE;out:OUT;prec:PREC;prefix:PREFIX;proto:PROTO;spt:SPT;
    start_time:START_TIME;
    tos:TOS;
    ttl:TTL;
    UUID:uuid; 
```



```

    maya_pg_grafana: >-
      connector.name=mayapg
      
      connection-url=jdbc:postgresql://postgres:5432/grafana

      connection-user=postgres

      connection-password=m1latDB
  
      redis-url=http://default:ulak@redis:6379
  
      keywords= 
  
      number_of_worker_threads=10
  
      run_in_coordinator_only=true
  
      worker_index_to_run_in=1
`````