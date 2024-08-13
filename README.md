# ulak-presto-connectors

This repository contains three presto connectors;

* InfluxDB connector : Developed upon from [presto-influxdb-connector](https://github.com/Chasingdreams6/presto-influxdb-connector.git)
* Postgres inline connector
* Quickwit inline connector

All connectors exploit a known Trino feature (query in) to embed [non-]sql data source queries.

# Connectors
* [ulak-presto-influxdb-connector](ulak-presto-influxdb-connector)
* [ulak-presto-postgres-connector](ulak-presto-postgres-connector)
* [ulak-presto-quickwit-connector](ulak-presto-quickwit-connector)

# Installation
1. Execute "mvn package".
2. Create "quickwit" directory in ${presto-root}/plugin/
3. copy original-presto-quickwit-0.432-SNAPSHOT.jar and presto-quickwit-0.280-SNAPSHOT.jar to /{presto-root-dir}/plugin/quickwit
4. use "/bin/launcher start" to start server
5. 
## Docker Build and Push Commands
### Building and Publishing Trino
```bash
./mvnw clean package
docker build -t 192.168.57.202:35000/trinodb/trino:432 .
docker push 192.168.57.202:35000/trinodb/trino:432
```
### Building and Publishing Trino-Cache
```bash
./mvnw clean package
docker build -t 192.168.57.202:35000/trinodb/trino:432-cache .
docker push 192.168.57.202:35000/trinodb/trino:432-cache
```

# Sample Queries
## Sample Influxdb Query;

```sql

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

## Sample Quickwit Query

```bash
select 1 as r, status, count(*) as cnt
from (
         select ifstatus.iface,
                ifstatus.host,
                bfd.overlay,
                bfd.src,
                bfd.peer,
                bfd.ns_id,
                bfd.uuid,
                bfd.status bfdstatus,
                lower(ifstatus.status) ifstatus,
                lower(bfd.status) || '_' ||lower(coalesce(ifstatus.status, 'None')) status 
         from (
                  select
                      overlay,
                      src,
                      iface,
                      peer,
                      ns_id,
                      uuid,
                      host,
                      max_by(status_text, max_date)status
                  --date, date_str, status_text, status
                  from
                      (
                          select
                              "/5/key" overlay,
                              "/10/key" status_text,
                              "/4/key" peer,
                              "/9/key" src,
                              "/8/key" ns_id,
                              "/7/key" uuid,
                              from_unixtime(cast("1/value" as double)/ 1000000000) max_date,
                              --"/3/key_as_string" date_str,
                              "/6/key" iface,
                              "/2/key" host,
                              "/value" status
                          from
                              quickwit.metrics3." 
            //cache=false
            //name=Dataplane Status
            //columns=host,_time,_value
            //dbtype=qw
            //qwindex=metrics3
            //replacefromcolumns=/3/buckets/2/buckets/4/buckets/5/buckets/6/buckets/7/buckets/8/buckets/9/buckets/10/buckets/1
            
            {
            ""aggs"": {
              ""3"": {
                ""aggs"": {
                  ""2"": {
                    ""aggs"": {
                      ""4"": {
                        ""aggs"": {
                          ""5"": {
                            ""aggs"": {
                              ""6"": {
                                ""aggs"": {
                                  ""7"": {
                                    ""aggs"": {
                                      ""8"": {
                                        ""aggs"": {
                                          ""9"": {
                                            ""aggs"": {
                                              ""10"": {
                                                ""aggs"": {
                                                  ""1"": {
                                                    ""sum"": {
                                                      ""field"": ""span_attributes.status_ni""
                                                    }
                                                  },
                                                  ""11"": {
                                                    ""max"": {
                                                      ""field"": ""span_start_timestamp_nanos""
                                                    }
                                                  }
                                                },
                                                ""terms"": {
                                                  ""field"": ""span_attributes.m_status_text"", 
                                                  ""size"":1,
                                                  ""order"": {
                                                    ""11"": ""desc""
                                                  },
                                                  ""min_doc_count"": 1
                                                }
                                              }
                                            },
                                            ""terms"": {
                                              ""field"": ""span_attributes.m_peer"", 
                                              ""size"":9999,
                                              ""order"": {
                                                ""_key"": ""desc""
                                              },
                                              ""min_doc_count"": 1
                                            }
                                          }
                                        },
                                        ""terms"": {
                                          ""field"": ""span_attributes.m_ns_id"", 
                                          ""size"":9999,
                                          ""order"": {
                                            ""_key"": ""desc""
                                          },
                                          ""min_doc_count"": 1
                                        }
                                      }
                                    },
                                    ""terms"": {
                                      ""field"": ""span_attributes.m_uuid"", 
                                      ""size"":9999,
                                      ""order"": {
                                        ""_key"": ""desc""
                                      },
                                      ""min_doc_count"": 1
                                    }
                                  }
                                },
                                ""terms"": {
                                  ""field"": ""span_attributes.m_iface"", 
                                  ""size"":9999,
                                  ""order"": {
                                    ""_key"": ""desc""
                                  },
                                  ""min_doc_count"": 1
                                }
                              }
                            },
                            ""terms"": {
                              ""field"": ""span_attributes.m_overlay"", 
                              ""size"":9999,
                              ""order"": {
                                ""_key"": ""desc""
                              },
                              ""min_doc_count"": 1
                            }
                          }
                        },
                        ""terms"": {
                          ""field"": ""span_attributes.m_src"", 
                          ""size"":9999,
                          ""order"": {
                            ""_key"": ""desc""
                          },
                          ""min_doc_count"": 1
                        }
                      }
                    },
                    ""terms"": {
                      ""field"": ""span_attributes.h"", 
                      ""size"":9999,
                      ""order"": {
                        ""_key"": ""desc""
                      },
                      ""min_doc_count"": 1
                    }
                  }
                },
                ""date_histogram"": {
                  ""field"": ""span_start_timestamp_nanos"",
                  ""fixed_interval"": ""1d"",
                  ""min_doc_count"": 1
                }
              }
            },
            ""query"": ""span_attributes.p:maya_bfd  AND NOT span_attributes.h:IN [${hubs:pipe}]"",
            ""max_hits"": 0,
            ""start_timestamp"": ${__from:date:seconds},
            ""end_timestamp"": ${__to:date:seconds}
          }
          
            " as i
                      ) as j
                  -- where
                  --     overlay != 'None'
                  --   and iface != 'None'
                  group by
                      overlay,
                      src,
                      iface,
                      peer,
                      ns_id,
                      uuid,
                      host
              ) as bfd
              left join
             (

                 select iface,   host, max_by(status, max_date)status --date, date_str, status_text, status
                 from(
                         select "/2/key" host,
                                "/5/key" status,
                                --from_unixtime(cast("/3/key" as double)/1000) date,
                                --"/3/key_as_string" date_str,
                                "/4/key" iface,
                                from_unixtime(cast("1/value" as double)/1000000000) max_date
                         from quickwit.metrics3." 
        //cache=false
        //name=Dataplane Status
        //columns=host,_time,_value
        //dbtype=qw
        //qwindex=metrics3
        //replacefromcolumns=/3/buckets/2/buckets/4/buckets/5/buckets/1
        
        {
        ""aggs"": {
          ""3"": {
            ""aggs"": {
              ""2"": {
                ""aggs"": {
                  ""4"": {
                    ""aggs"": {
                      ""5"": {
                        ""aggs"": {
                          ""1"": {
                            ""sum"": {
                              ""field"": ""span_attributes.value""
                            }
                          },
                          ""11"": {
                            ""max"": {
                              ""field"": ""span_start_timestamp_nanos""
                            }
                          }
                        },
                        ""terms"": {
                          ""field"": ""span_attributes.ti"", 
                          ""size"":9999,
                          ""order"": {
                            ""11"": ""desc""
                          },
                          ""min_doc_count"": 1
                        }
                      }
                    },
                    ""terms"": {
                      ""field"": ""span_attributes.pi"", 
                      ""size"":9999,
                      ""order"": {
                        ""_key"": ""desc""
                      },
                      ""min_doc_count"": 1
                    }
                  }
                },
                ""terms"": {
                  ""field"": ""span_attributes.h"", 
                  ""size"":9999,
                  ""order"": {
                    ""_key"": ""desc""
                  },
                  ""min_doc_count"": 1
                }
              }
            },
            ""date_histogram"": {
              ""field"": ""span_start_timestamp_nanos"",
              ""fixed_interval"": ""1d"",
              ""min_doc_count"": 1
            }
          }
        },
        ""query"": ""span_attributes.p:maya_ifstatus AND span_attributes.pi:eth* AND NOT span_attributes.h:IN [${hubs:pipe}]"",
        ""max_hits"": 0,
        ""start_timestamp"": ${__from:date:seconds},
        ""end_timestamp"": ${__to:date:seconds}
      }
      
        " as i
                     ) as j
                 group by iface,  host

             ) as ifstatus
              on ifstatus.host = bfd.host and ifstatus.iface = bfd.iface
     ) as agg
group by status
 
```
## Sample Postgres Query
```sql

select cast(i._time as DOUBLE)*1000  as time, _measurement,cast(i._value as DOUBLE) as _value
from mayapostgres.public."
  //ttl=172800
  //refresh=3600
  //cache=true
  //eagercache=true

  //columns=_time,_measurement,_value
   select ""a"" as b

" as i
--left join postgres_simsek.public.site s on cast(s.id as varchar) = i.host 
--where s.name is null 
order by _value desc

```

# Notes
1. The connector only supports String type data.
2. The documentation is still improving by ourselves. We welcome any kind of help.

# Trino catalog sample


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
````