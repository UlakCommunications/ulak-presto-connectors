# ulak-presto-connectors

[![GitHub Container Registry](https://img.shields.io/badge/ghcr.io-ulakcommunications%2Ftrino--quickwit-blue)](https://github.com/UlakCommunications/ulak-presto-connectors/pkgs/container/trino-quickwit)

Three Trino connector plugins that embed non-SQL data source queries directly inside SQL using Trino's query-in-table-name feature:

* **Quickwit connector** — queries Quickwit search engine indexes; supports aggregations DSL, plain table mode, base32-encoded queries from Grafana Trino plugin
* **InfluxDB connector** — developed from [presto-influxdb-connector](https://github.com/Chasingdreams6/presto-influxdb-connector.git)
* **Postgres inline connector** — embeds PostgreSQL queries in Trino SQL

Redis is optional; required only when query caching (`//cache=true`) is used.

## Quick Start — Docker

```bash
docker pull ghcr.io/ulakcommunications/trino:479
docker run -p 8080:8080 ghcr.io/ulakcommunications/trino:479
```

## Connectors
* [ulak-presto-influxdb-connector](ulak-presto-influxdb-connector)
* [ulak-presto-postgres-connector](ulak-presto-postgres-connector)
* [ulak-presto-quickwit-connector](ulak-presto-quickwit-connector)

## Build from Source

```bash
mvn clean package -DskipTests
```

## Docker Build and Push

```bash
# Build (uses public trinodb/trino:479 base)
mvn clean package -DskipTests
docker build \
  --build-arg TRINO_BASE=trinodb/trino:479 \
  -t ghcr.io/ulakcommunications/trino:479 \
  -t ghcr.io/ulakcommunications/trino:latest \
  .

# Push to GitHub Container Registry
echo $GITHUB_TOKEN | docker login ghcr.io -u <github-username> --password-stdin
docker push ghcr.io/ulakcommunications/trino:479
docker push ghcr.io/ulakcommunications/trino:latest
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

> For full Quickwit connector documentation (TVF syntax, Aggs DSL, `sqlversion` modes, column naming): **[ulak-presto-quickwit-connector/README.md](ulak-presto-quickwit-connector/README.md)**

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
      connector.name=mayapostgres   
      pg-connection-url=jdbc:postgresql://postgres:5432/grafana
      pg-connection-user=*****
      pg-connection-password=*****
      redis-url=http://default:*****@redis:6379
      keywords= 
      number_of_worker_threads=10
      run_in_coordinator_only=true
      worker_index_to_run_in=1
````

# Quickwit Entry in Trino Catalog

```
  quickwit.properties: |
    connector.name=quickwit 
    redis-url=http://<user>:<*****>@redis:6379
    keywords= 
    number_of_worker_threads=5
    run_in_coordinator_only=true
    worker_index_to_run_in=1 
    qw-connection-url=http://<quickwit-host>:<port>
    qw-index=metrics3 
````

# Influxdb Entry in Trino Catalog

```
  influxdb.properties: >
    connector.name=influxdb
    connection-url=http://influxdb?readTimeout=600m&connectTimeout=100m&writeTimeout=200m
    connection-org=*****
    connection-token=<*****>
    redis-url=http://*****:*****@redis:6379
    keywords= 
    number_of_worker_threads=5
    run_in_coordinator_only=true
    worker_index_to_run_in=1 
````
# Query Parameters

```
//cache=false
//name=Dataplane Status
//columns=host,_time,_value
//dbtype=qw
//qwindex=metrics3
//qwurl=http://1.2.3.5:7280
//replacefromcolumns=/3/buckets/2/buckets/4/buckets/5/buckets/6/buckets/7/buckets/8/buckets/9/buckets/10/buckets/1
            
```

| Parameter            | Values               | Description                                                                                                                                                                                                                                                                                                                                                                                                                             |   |
|----------------------|----------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---|
| `cache`              | `true`/`false`       | Enable cache                                                                                                                                                                                                                                                                                                                                                                                                                            |   |
| `from`               | unixtime             | from date in seconds for caching. The values are replaced for relative time in execution.                                                                                                                                                                                                                                                                                                                                               |   |
| `to`                 | unixtime             | to date in seconds for caching. The values are replaced for relative time in execution.                                                                                                                                                                                                                                                                                                                                                 |   |
| `name`               | text                 | Name of the query                                                                                                                                                                                                                                                                                                                                                                                                                       |   |
| `columns`            | Comma separated names| If no data available, then only these headers will be returned                                                                                                                                                                                                                                                                                                                                                                          |   |
| `dbtype`             | `qw`/`pg`/`influxdb` | Database type                                                                                                                                                                                                                                                                                                                                                                                                                           |   |
| `qwindex`            | text                 | Quickwit index name (raw/live index)                                                                                                                                                                                                                                                                                                                                                                                                    |   |
| `qwurl`              | URL                  | Quickwit URL                                                                                                                                                                                                                                                                                                                                                                                                                            |   |
| `replacefromcolumns` | text                 | Text to replace from field names                                                                                                                                                                                                                                                                                                                                                                                                        |   |
| `enable_history`     | `true`/`false`       | Dynamically switch to rollup history index when query time range exceeds threshold (default: 3600s / 1 hour)                                                                                                                                                                                                                                                                                                                            |   |
| `history_index`      | text                 | Target rollup index name for historical time ranges (e.g., `metrics3_15`, `rollup_15m_site_app`)                                                                                                                                                                                                                                                                                                                                        |   |
| `ttl`                | seconds              | Time to live for redis cache                                                                                                                                                                                                                                                                                                                                                                                                            |   |
| `refresh`            | seconds              | Cache refresh period                                                                                                                                                                                                                                                                                                                                                                                                                    |   |
| `hasjs` (trino only) | `true`/`false`       | Ability to use javascript code in queries  <br/> Predefined javascripts variables: `now` (current unix time), `d` (day), `h` (hour), `m` (minute), `s` (second), `math` (`Math`). Standard `Math.floor`, `Math.ceil` are natively supported in Rhino runtime. |   |

---

# History & Rollup Query Routing Architecture

When queries span large time windows, scanning millions of raw documents in Quickwit causes high CPU, memory, and I/O load. To solve this, Quickwit indices are paired with pre-aggregated rollup tables produced continuously by `qw-rollup-engine`.

The Quickwit Connector automatically and transparently rewrites queries to use rollup indices whenever the requested time range exceeds `history-time-threshold-seconds` (configured in catalog properties or defaulting to 1 hour).

```mermaid
flowchart TD
    A["Grafana / Trino SQL Query"] --> B["Trino Engine: QwUtil.select()"]
    B --> C{"enable_history == true?"}
    
    C -- "No" --> D["Live Index (e.g. metrics3, flows3)"]
    C -- "Yes" --> E{"Time Range >= Threshold?<br/>(e.g., end_ts - start_ts >= 3600s)"}
    
    E -- "No (Recent data)" --> D
    E -- "Yes (Historical data)" --> F["Target Rollup Index<br/>(e.g. metrics3_15, rollup_15m_site_app)"]
    
    F --> G["QwQueryRewriter.rewriteQueryForHistory()"]
    G --> H["Deterministic Metric Suffix Mapping:<br/>• sum(field) → field_sum<br/>• avg(field) → field_avg<br/>• min(field) → field_min<br/>• max(field) → field_max<br/>• count(field) → field_count"]
    
    H --> I["Quickwit Search API Call<br/>(Reads pre-aggregated splits)"]
    D --> I
    I --> J["Response Parser & Flattener<br/>(JFlat / Tree Traversal)"]
    J --> K["SQL Tabular Result Set"]
```

### Standardized Metric Suffix Convention
In both `qw-rollup-engine` tasks and the Quickwit connector rewriter, metric names adhere to a strictly standardized naming model:
* All flow metrics (`u`, `ac`, `ab`, `t`, `u_ac`, `t_ab`) are pre-aggregated as sum in rollup tasks and stored with the `_sum` suffix (`u_sum`, `ac_sum`, `ab_sum`, `t_sum`, `u_ac_sum`, `t_ab_sum`).
* System metrics (`rx`, `tx`, `cpu`, etc.) are pre-aggregated as `_avg`, `_min`, `_max`, `_sum`, `_count`.
* The rewriter deterministically maps each metric aggregation without needing custom exclusions or query-level comments.

---

```bash
./push.sh 0.0.1-develop-latest 'linux/arm64,linux/amd64' false
```

