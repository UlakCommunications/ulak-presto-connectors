# presto-influxdb-connector

It's a Quickwit connector for presto, it's based on trino-0.432.

## build and run 
1. Execute "mvn package".
2. Create "quickwit" directory in ${presto-root}/plugin/
3. copy original-presto-quickwit-0.432-SNAPSHOT.jar and presto-quickwit-0.280-SNAPSHOT.jar to /{presto-root-dir}/plugin/quickwit
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
 