# presto-postgres-connector

It's a Postgres connector for presto, it's based on trino-0.432.

## build and run 
1. Execute "mvn package".
2. Create "influxdb" directory in ${presto-root}/plugin/
3. copy original-presto-postgres-0.432-SNAPSHOT.jar and presto-postgres-0.280-SNAPSHOT.jar to /{presto-root-dir}/plugin/influxdb
4. use "/bin/launcher start" to start server

## notes
1. It's only support String Type;


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
        
  Select ""a"" as b
" as i
--left join postgres_simsek.public.site s on cast(s.id as varchar) = i.host 
--where s.name is null 
order by _value desc
 
```  

# KEYWORDS
Trino lower cases all query. Inside `keywords` will be reverted to original forms.
```
 keywords=application_id:APPLICATION_ID;proto:PROTO;
```



# Postgres Entry in Trino Catalog

```

    maya_pg_grafana: >-
      connector.name=mayapg
      
      connection-url=jdbc:postgresql://postgres:5432/grafana

      connection-user=postgres

      connection-password=*****
  
      redis-url=http://default:*****@redis:6379
  
      keywords= 
  
      number_of_worker_threads=10
  
      run_in_coordinator_only=true
  
      worker_index_to_run_in=1
`````