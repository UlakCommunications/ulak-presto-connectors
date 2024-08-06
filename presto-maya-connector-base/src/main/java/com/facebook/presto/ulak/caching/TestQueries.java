package com.facebook.presto.ulak.caching;

public class TestQueries {

    public static final String SAMPLE_QUERY = "tblBfd = from(bucket: \"otlp_metric\")\n" +
            "  |> range(start:  -5m) \n" +
            "  |> filter(fn: (r) => r[\"_measurement\"] == \"maya_bfd\") \n" +
            "  |> last()\n" +
            "  |> filter(fn: (r) => r[\"host\"] !~ /42cdeaa7-b7b6-402e-bb68-4cfde8fc7297/)\n" +
            "  |> group(columns: [\"host\"])\n" +
            "  |> sum(column: \"_value\")\n" +
            "  |> map(fn: (r)=>({r with connectivity:if r[\"_value\"] == 0 then \"DOWN\" else \"UP\"}))\n" +
            "  |> keep(columns: [\"host\", \"connectivity\"])\n" +
            "  |> group()\n" +
            "\n" +
            "tblIfStatus = from(bucket: \"otlp_metric\")\n" +
            "  |> range(start:  -5m) \n" +
            "  |> filter(fn: (r) => r[\"_measurement\"] == \"maya_ifstatus\")\n" +
            "  |> filter(fn: (r) => r[\"type\"] == \"maya_ifstatus\") \n" +
            "  |> filter(fn: (r) => r[\"plugin\"] == \"maya_ifstatus\")  \n" +
            "  |> last()\n" +
            "  |> filter(fn: (r) => r[\"host\"] !~ /42cdeaa7-b7b6-402e-bb68-4cfde8fc7297/)\n" +
            "  |> map(fn: (r)=>({r with link_status:if r[\"_value\"] == 6 then 1 else 0}))\n" +
            "  |> filter(fn: (r) => contains(value: r[\"link_status\"], set:[1,0,2]))\n" +
            "  |> group(columns: [\"host\"])\n" +
            "  |> sum(column: \"link_status\")  \n" +
            "  |> map(fn: (r)=>({r with link:if r[\"link_status\"] == 0 then \"DOWN\" else \"UP\"}))\n" +
            "  |> group()\n" +
            "\n" +
            "\n" +
            "\n" +
            "  join(tables: {sql: tblBfd, ts: tblIfStatus}, on: [\"host\"]) \n" +
            "  |> map(fn: (r)=>({r with g:r[\"connectivity\"] + \"_\" + r[\"link\"]}))\n" +
            "  |>group(columns: [ \"g\"])\n" +
            "  |>count(column: \"link\" )\n" +
            "  |>group()\n" +
            "  |> pivot(rowKey: [], columnKey: [\"g\"], valueColumn: \"link\")";
    public static final String SAMPLE_QUERY_WITH_CACHE= "//" + QueryParameters.TEXT_CACHE + "=true\n"
            +"//" +  QueryParameters.TEXT_TTL + "=60\n"
            +"//" +  QueryParameters.TEXT_REFRESH + "=10\n"
            + SAMPLE_QUERY;
    public static final String SAMPLE_QUERY_2 =
            "\n" +
                    "//import \"date\"\n" +
                    "import \"array\"\n" +
                    "//import \"experimental/array\"\n" +
                    "\n" +
                    "t1 = from(bucket: \"otlp_metric\")\n" +
                    "|> range(start:  -5m) \n" +
                    "|> filter(fn: (r) => r[\"_measurement\"] == \"maya_probe\" and r[\"dsname\"]==\"avg_delay\") \n" +
                    "|> group(columns: [\"host\"])\n" +
                    "|> mean(column: \"_value\") \n" +
                    "|> group()\n" +
                    "|> sort(columns:[\"_value\"], desc: true)\n" +
                    "|> limit(n:5)\n" +
                    "|> keep(columns: [\"host\"])\n" +
                    "\n" +
                    "t2= from(bucket: \"otlp_metric\")\n" +
                    "|> range(start:  -5m) \n" +
                    "|> filter(fn: (r) => r[\"_measurement\"] == \"maya_probe\" and r[\"dsname\"]==\"avg_delay\" ) \n" +
                    "\n" +
                    "\n" +
                    "join(tables: {t1: t1, t2: t2}, on: [\"host\"])\n" +
                    " |> aggregateWindow(every: 30s, fn: mean, createEmpty: true)\n" +
                    " |> group(columns: [\"host\",\"_time\"])\n" +
                    " |> mean(column: \"_value\") \n" +
                    " |> fill(usePrevious: true)\n" +
                    " |> group()\n" +
                    " //|> sort(columns:[\"_time\"], desc: false)";

    public static final String SAMPLE_QUERY_2_WITH_CACHE= "//" + QueryParameters.TEXT_CACHE + "=true\n"
            + "//" + QueryParameters.TEXT_TTL + "=60\n"
            + "//" + QueryParameters.TEXT_REFRESH + "=10\n"
            + SAMPLE_QUERY_2;
    public static final String SAMPLE_QUERY_3 =
            "from(bucket: \"otlp_metric\")\n" +
                    "  |> range(start:  -5m) \n" +
                    "  |> filter(fn: (r) => r[\"_measurement\"] == \"cpu\")\n" +
                    "  |> filter(fn: (r) => r[\"host\"] =~ /${hubs:regex}/)\n" +
                    "  |> aggregateWindow(every: 10s, fn: mean, createEmpty: false)\n" +
                    "  |> drop(columns: [\"idle\"])\n" +
                    "  |> group(columns: [\"host\" ])\n" +
                    "  |> mean(column: \"_value\")\n" +
                    "  |> drop(columns: [\"_start\",\"_stop\"])\n" +
                    "  |> group()\n" +
                    "  |> sort(columns: [ \"_value\"], desc: true)\n" +
                    "  |> limit(n:5)";

    public static final String SAMPLE_QUERY_3_WITH_CACHE= "//" + QueryParameters.TEXT_CACHE + "=true\n"
            + "//" + QueryParameters.TEXT_TTL + "=60\n"
            + "//" + QueryParameters.TEXT_REFRESH + "=10\n"
            + SAMPLE_QUERY_3;
    public static final String SAMPLE_QUERY_4 =
            "from(bucket: \"otlp_metric\")\n" +
                    "  |> range(start:  -5m)\n" +
                    "  |> filter(fn: (r) => r[\"_measurement\"] == \"maya_ifstatus\")\n" +
                    "  |> aggregateWindow(every: 1s, fn: mean, createEmpty: false)\n" +
                    "  |> last()\n" +
                    "  |> group()\n" +
                    "  |> map(fn: (r) => ({r with jn: r.host + \"-\" + r.plugin_instance , has_data: true  }))\n" +
                    "  |> yield(name: \"last\")";

    public static final String SAMPLE_QUERY_4_WITH_CACHE= "//" + QueryParameters.TEXT_CACHE + "=true\n"
            + "//" + QueryParameters.TEXT_TTL + "=60\n"
            + "//" + QueryParameters.TEXT_REFRESH + "=10\n"
            + SAMPLE_QUERY_4;
    public static final String SAMPLE_QUERY_5 =
            "\n" +
                    "\n" +
                    "tblUptime=from(bucket: \"otlp_metric\")\n" +
                    "  |> range(start:  -5m) \n" +
                    "  |> filter(fn: (r) => r[\"_measurement\"] == \"uptime\")\n" +
                    "  |> aggregateWindow(every: 200ms, fn: last, createEmpty: false)\n" +
                    "  |> last()\n" +
                    "  |> group()\n" +
                    "  |> keep(columns: [\"host\",\"_value\"])\n" +
                    "  |> map(fn: (r)=>({r with uptime:r[\"_value\"]}))\n" +
                    "  |> group()\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "  tblProbe= from(bucket: \"otlp_metric\")\n" +
                    "  |> range(start:  -5m) \n" +
                    "  |> filter(fn: (r) => r[\"_measurement\"] == \"maya_probe\"  ) \n" +
                    "  |> group(columns: [\"host\",\"dsname\",\"overlay\"])\n" +
                    "  |> mean( )\n" +
                    "  |> group()\n" +
                    "  |> pivot(columnKey:[\"dsname\"] , rowKey: [\"host\",\"overlay\"], valueColumn: \"_value\")\n" +
                    "  |> drop(columns: [\"localbo\",\"sent\",\"test_duration\"])\n" +
                    " \n" +
                    " tblJoin2 = join(tables: {sql: tblUptime, ts:tblProbe}, on: [\"host\"]) \n" +
                    "  |> keep(columns: [\"host\", \"uptime\", \"reset_count\",\"overlay\",\"avg_delay\",\"jitter\",\"loss_percentage\"])\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "\n" +
                    "tblIfStatus = from(bucket: \"otlp_metric\")\n" +
                    "  |> range(start:  -5m) \n" +
                    "  |> filter(fn: (r) => r[\"_measurement\"] == \"maya_ifstatus\")\n" +
                    "  |> filter(fn: (r) => r[\"type\"] == \"maya_ifstatus\") \n" +
                    "  |> filter(fn: (r) => r[\"plugin\"] == \"maya_ifstatus\")  \n" +
                    "  |> map(fn: (r)=>({r with type_instance : if r[\"type_instance\"]==\"UP\" then \"UP\" else if r[\"type_instance\"]==\"UNKNOWN\" then \"UNKOWN\" else \"DOWN\", link_status : if r[\"type_instance\"]==\"UP\" then 1 else if r[\"type_instance\"]==\"UNKNOWN\" then 2 else 0})) \n" +
                    "  |> last()\n" +
                    "  |> filter(fn: (r) => contains(value: r[\"link_status\"], set:[1,0,2]))\n" +
                    "  |> group(columns: [\"host\", \"plugin_instance\", \"link_status\"])\n" +
                    "  |> count(column: \"_field\")\n" +
                    "  |> group()\n" +
                    "\n" +
                    " \n" +
                    "\n" +
                    "  \n" +
                    " join(tables: {sql: tblIfStatus, ts:tblJoin2}, on: [\"host\"]) \n" +
                    "  |> map(fn: (r)=>({r with jn:r.host + \"-\" +  r.overlay + \"-\" + r.plugin_instance}))\n" +
                    "  //|> keep(columns: [\"host\", \"uptime\", \"reset_count\",\"avg_delay\",\"jitter\",\"loss_percentage\"])\n" +
                    " |> sort(columns: [\"host\"])";

    public static final String SAMPLE_QUERY_5_WITH_CACHE= "//" + QueryParameters.TEXT_CACHE + "=true\n"
            + "//" + QueryParameters.TEXT_TTL + "=60\n"
            + "//" + QueryParameters.TEXT_REFRESH + "=10\n"
            + SAMPLE_QUERY_5;


    public static final String SAMPLE_QUERY_6 =
            "\n\n" +
                    "with atmp as (\n" +
                    "select rule_uid,\n" +
                    "\t\tcurrent_state,\n" +
                    "\t\tlabels_hash,\n" +
                    "\t\ttrim(both '\"' from (jsonb_array_elements(ai.labels::jsonb)->0)::text) as k, \n" +
                    "\t\ttrim(both '\"' from (jsonb_array_elements(ai.labels::jsonb)->1)::text) as v\n" +
                    "from alert_instance ai\n" +
                    "),silence as (           \n" +
                    "select distinct *,\n" +
                    "\t\ttrim(both '\"' from ((ms.status::jsonb)->'state')::text) as state ,\n" +
                    "\t\ttrim(both '\"' from (a.value::text)) as silenced \n" +
                    "from maya_silence ms  \n" +
                    "left join jsonb_array_elements(ms.silenced_alerts::jsonb) a on true \n" +
                    "\n" +
                    ") ,silence_history as (           \n" +
                    "select distinct *,\n" +
                    "\t\ttrim(both '\"' from ((ms.status::jsonb)->'state')::text) as state ,\n" +
                    "\t\ttrim(both '\"' from (a.value::text)) as silenced \n" +
                    "from maya_silence_history ms  \n" +
                    "left join jsonb_array_elements(ms.silenced_alerts::jsonb) a on true \n" +
                    "\n" +
                    ") ,\n" +
                    "anno_tmp as (\n" +
                    "\tselect * \n" +
                    "\tfrom annotation a \n" +
                    ")\n" +
                    "\n" +
                    "select --sum(resolve_count) resolve_count,\n" +
                    "\t   -- sum(alarm_count) alarm_count ,\n" +
                    "\t   -- sum(nodata_count) nodata_count,\n" +
                    "\t   ((select count(prev_state) from anno_tmp a where  prev_state='Normal')) as resolve_count,\n" +
                    "\t\t((select count(prev_state) from anno_tmp a where  prev_state='Alerting')) as alarm_count,\n" +
                    "\t\t((select count(prev_state) from anno_tmp a where prev_state='NoData')) as nodata_count,\n" +
                    "\t\tcount(distinct host) as site_count,\n" +
                    "\t\tcount(case when silenced then 1 else 0 end) as silence_count,\n" +
                    "\t\tsum(case when resolve_count>0 then 1 else 0 end )resolve_host_count,\n" +
                    "\t\tsum(case when alarm_count>0 then 1 else 0 end )alarm_host_count,\n" +
                    "\t\tsum(case when nodata_count>0 then 1 else 0 end )nodata_host_count,\n" +
                    "\t\tsum(case when silenced_count then 1 else 0 end )silenced_host_count,\n" +
                    "\t\tcount(distinct case when severity = 'Alerting' then title else '' end ) distinct_alert_sensor_count,\n" +
                    "\t\tcount(distinct case when severity != 'Alerting' then title else '' end ) distinct_invalid_sensor_count\n" +
                    "from(\n" +
                    "\n" +
                    "select current_state  as severity,\n" +
                    "\t\tar.title ,\n" +
                    "\t\t(select v from atmp where k = 'grafana_folder' and atmp.rule_uid = ai.rule_uid  and atmp.labels_hash = ai.labels_hash)  as sensor, \n" +
                    "\t\t(select v from atmp where k = 'host'           and atmp.rule_uid = ai.rule_uid  and atmp.labels_hash = ai.labels_hash)  as host, \n" +
                    "\t\ttrim(both '\"' from (ar.annotations::jsonb->'alarm_id')::text)alarm_id, \n" +
                    "\t\tTO_TIMESTAMP(ai.last_eval_time/1000)  time,\n" +
                    "\t\t(select True from silence   where silence.state = 'active' and silence.silenced = ai.labels_hash  limit 1)  as silenced,\n" +
                    "\t\t(select True from silence_history   where silence_history.state = 'active' and silence_history.silenced = ai.labels_hash  limit 1)  as silenced_count,\n" +
                    "\t\t((select count(prev_state) from anno_tmp a where a.alert_id = ar.id and prev_state='Normal')) as resolve_count,\n" +
                    "\t\t((select count(prev_state) from anno_tmp a where a.alert_id = ar.id and prev_state='Alerting')) as alarm_count,\n" +
                    "\t\t((select count(prev_state) from anno_tmp a where a.alert_id = ar.id and prev_state='NoData')) as nodata_count,\n" +
                    "\t\tar.id as main_alert_id,\n" +
                    "\t\tar.uid as main_alert_uid\n" +
                    "from alert_instance ai\n" +
                    "left join alert_rule ar  on ai.rule_uid = ar.uid \n" +
                    "\n" +
                    ")r";

    public static final String SAMPLE_QUERY_6_WITH_CACHE=
            "--" + QueryParameters.TEXT_CACHE + "=true\n"
            + "--" + QueryParameters.TEXT_TTL + "=60\n"
            + "--" + QueryParameters.TEXT_REFRESH + "=10\n"
            //+ "--"+ TEXT_CACHE + "=true\n" +
            + "--" + QueryParameters.TEXT_DBTYPE + "=pg\n"
            + "--columns=resolve_count,alarm_count,nodata_count,site_count,silence_count,resolve_host_count,alarm_host_count,nodata_host_count,silenced_host_count,distinct_alert_sensor_count,distinct_invalid_sensor_count"
            + SAMPLE_QUERY_6;


    public static final String SAMPLE_QUERY_7 =
                    "  \n" +
                    "  {\n" +
                    "\"aggs\": { \n" +
                    "        \"3\": {\n" +
                    "          \"aggs\": {\n" +
                    "            \"1\": {\n" +
                    "              \"avg\": {\n" +
                    "                \"field\": \"span_attributes.tx\"\n" +
                    "              }\n" +
                    "            },\n" +
                    "            \"4\": {\n" +
                    "              \"avg\": {\n" +
                    "                \"field\": \"span_attributes.rx\"\n" +
                    "              }\n" +
                    "            }\n" +
                    "          },\n" +
                    "          \"terms\": {\n" +
                    "            \"field\": \"span_attributes.h\",\n" +
                    "            \"order\": {\n" +
                    "              \"1\": \"desc\"\n" +
                    "            },\n" +
                    "            \"size\":5,\n" +
                    "            \"min_doc_count\": 1\n" +
                    "          }\n" +
                    "        } \n" +
                    "    } ,\n" +
                    "  \"query\": \"span_attributes.p:interface  AND span_attributes.t:IN [if_octets]  AND span_attributes.h:IN [ee7b566c-68d7-4ffb-9d0a-29477a39b008|ee7b566c-68d7-4ffb-9d0a-29477a39b010|ee7b566c-68d7-4ffb-9d0a-29477a39b013|ee7b566c-68d7-4ffb-9d0a-29477a39b015|ee7b566c-68d7-4ffb-9d0a-29477a39b019]\",\n" +
                    "  \"max_hits\": 0,\n" +
                    "  \"start_timestamp\": 1719310491,\n" +
                    "  \"end_timestamp\": 1719310791\n" +
                    "}";

    public static final String SAMPLE_QUERY_7_WITH_CACHE=
                    "//" + QueryParameters.TEXT_TTL + "=172800\n" +
                    "//" + QueryParameters.TEXT_REFRESH + "=10\n" +
                    "//"+ QueryParameters.TEXT_CACHE+"=false\n" +
                    "//eagercache=false\n" +
                    "//name=Hub Network Throughput (bps)\n" +
                    "//columns=/3/buckets/1/key,/3/buckets/1/value,/3/buckets/4/value\n" +
                    "//" + QueryParameters.TEXT_DBTYPE + "=qw\n" +
                    SAMPLE_QUERY_7;


    public static final String SAMPLE_QUERY_8 =
            "  {\n" +
                    "  \"aggs\": {\n" +
                    "    \"3\": {\n" +
                    "      \"aggs\": {\n" +
                    "        \"2\": {\n" +
                    "          \"aggs\": {\n" +
                    "            \"4\": {\n" +
                    "              \"aggs\": {\n" +
                    "                \"5\": {\n" +
                    "                  \"aggs\": {\n" +
                    "                    \"6\": {\n" +
                    "                      \"aggs\": {\n" +
                    "                        \"7\": {\n" +
                    "                          \"aggs\": {\n" +
                    "                            \"8\": {\n" +
                    "                              \"aggs\": {\n" +
                    "                                \"9\": {\n" +
                    "                                  \"aggs\": {\n" +
                    "                                    \"10\": {\n" +
                    "                                      \"aggs\": {\n" +
                    "                                        \"1\": {\n" +
                    "                                          \"sum\": {\n" +
                    "                                            \"field\": \"span_attributes.status_ni\"\n" +
                    "                                          }\n" +
                    "                                        }\n" +
                    "                                      },\n" +
                    "                                      \"terms\": {\n" +
                    "                                        \"field\": \"span_attributes.m_src\",\n" +
                    "                                        \"size\": 100,\n" +
                    "                                        \"order\": {\n" +
                    "                                          \"_key\": \"desc\"\n" +
                    "                                        },\n" +
                    "                                        \"min_doc_count\": 1\n" +
                    "                                      }\n" +
                    "                                    }\n" +
                    "                                  },\n" +
                    "                                  \"terms\": {\n" +
                    "                                    \"field\": \"span_attributes.m_peer\",\n" +
                    "                                    \"size\": 100,\n" +
                    "                                    \"order\": {\n" +
                    "                                      \"_key\": \"desc\"\n" +
                    "                                    },\n" +
                    "                                    \"min_doc_count\": 1\n" +
                    "                                  }\n" +
                    "                                }\n" +
                    "                              },\n" +
                    "                              \"terms\": {\n" +
                    "                                \"field\": \"span_attributes.m_ns_id\",\n" +
                    "                                \"size\": 100,\n" +
                    "                                \"order\": {\n" +
                    "                                  \"_key\": \"desc\"\n" +
                    "                                },\n" +
                    "                                \"min_doc_count\": 1\n" +
                    "                              }\n" +
                    "                            }\n" +
                    "                          },\n" +
                    "                          \"terms\": {\n" +
                    "                            \"field\": \"span_attributes.m_uuid\",\n" +
                    "                            \"size\": 100,\n" +
                    "                            \"order\": {\n" +
                    "                              \"_key\": \"desc\"\n" +
                    "                            },\n" +
                    "                            \"min_doc_count\": 1\n" +
                    "                          }\n" +
                    "                        }\n" +
                    "                      },\n" +
                    "                      \"terms\": {\n" +
                    "                        \"field\": \"span_attributes.m_status_text\",\n" +
                    "                        \"size\": 100,\n" +
                    "                        \"order\": {\n" +
                    "                          \"_key\": \"desc\"\n" +
                    "                        },\n" +
                    "                        \"min_doc_count\": 1\n" +
                    "                      }\n" +
                    "                    }\n" +
                    "                  },\n" +
                    "                  \"terms\": {\n" +
                    "                    \"field\": \"span_attributes.m_overlay\",\n" +
                    "                    \"size\": 100,\n" +
                    "                    \"order\": {\n" +
                    "                      \"_key\": \"desc\"\n" +
                    "                    },\n" +
                    "                    \"min_doc_count\": 1\n" +
                    "                  }\n" +
                    "                }\n" +
                    "              },\n" +
                    "              \"terms\": {\n" +
                    "                \"field\": \"span_attributes.m_iface\",\n" +
                    "                \"size\": 100,\n" +
                    "                \"order\": {\n" +
                    "                  \"_key\": \"desc\"\n" +
                    "                },\n" +
                    "                \"min_doc_count\": 1\n" +
                    "              }\n" +
                    "            }\n" +
                    "          },\n" +
                    "          \"terms\": {\n" +
                    "            \"field\": \"span_attributes.h\",\n" +
                    "            \"size\": 100,\n" +
                    "            \"order\": {\n" +
                    "              \"_key\": \"desc\"\n" +
                    "            },\n" +
                    "            \"min_doc_count\": 1\n" +
                    "          }\n" +
                    "        }\n" +
                    "      },\n" +
                    "      \"date_histogram\": {\n" +
                    "        \"field\": \"span_start_timestamp_nanos\",\n" +
                    "        \"fixed_interval\": \"5s\",\n" +
                    "        \"min_doc_count\": 0\n" +
                    "      }\n" +
                    "    }\n" +
                    "  },\n" +
                    "  \"query\": \"span_attributes.p:maya_bfd AND NOT span_attributes.h:IN [e84b661f-8328-4c7a-863b-65cabdf43f94]\",\n" +
                    "  \"max_hits\": 0,\n" +
                    "  \"start_timestamp\": 1719827487,\n" +
                    "  \"end_timestamp\": 1719827787\n" +
                    "}";

    public static final String SAMPLE_QUERY_9 =
            "  {\n" +
                    "  \"aggs\": {\n" +
                    "  \"6\": {\n" +
                    "    \"aggs\": {\n" +
                    "      \"5\": {\n" +
                    "        \"aggs\": {\n" +
                    "          \"1\": {\n" +
                    "            \"min\": {\n" +
                    "              \"field\": \"span_attributes.tx\"\n" +
                    "            }\n" +
                    "          },\n" +
                    "          \"9\": {\n" +
                    "            \"aggs\": {\n" +
                    "              \"2\": {\n" +
                    "                \"min\": {\n" +
                    "                  \"field\": \"span_attributes.tx\"\n" +
                    "                }\n" +
                    "              },\n" +
                    "              \"21\": {\n" +
                    "                \"max\": {\n" +
                    "                  \"field\": \"span_attributes.tx\"\n" +
                    "                }\n" +
                    "              },\n" +
                    "              \"3\": {\n" +
                    "                \"min\": {\n" +
                    "                  \"field\": \"span_attributes.rx\"\n" +
                    "                }\n" +
                    "              },\n" +
                    "              \"4\": {\n" +
                    "                \"max\": {\n" +
                    "                  \"field\": \"span_attributes.rx\"\n" +
                    "                }\n" +
                    "              }\n" +
                    "            },\n" +
                    "            \"terms\": {\n" +
                    "              \"field\": \"span_attributes.pi\",\n" +
                    "              \"size\": 1000,\n" +
                    "              \"order\": {\n" +
                    "                \"2\": \"desc\"\n" +
                    "              },\n" +
                    "              \"min_doc_count\": 1\n" +
                    "            }\n" +
                    "          }\n" +
                    "        },\n" +
                    "        \"terms\": {\n" +
                    "          \"field\": \"span_attributes.h\",\n" +
                    "          \"size\": 10000,\n" +
                    "          \"order\": {\n" +
                    "            \"1\": \"desc\"\n" +
                    "          },\n" +
                    "          \"min_doc_count\": 1\n" +
                    "        }\n" +
                    "      }\n" +
                    "    },\n" +
                    "    \"date_histogram\": {\n" +
                    "      \"field\": \"span_start_timestamp_nanos\",\n" +
                    "      \"fixed_interval\": \"1h\",\n" +
                    "      \"min_doc_count\": 1\n" +
                    "    }\n" +
                    "  }\n" +
                    "},\n" +
                    "  \"query\": \"span_attributes.p:interface  AND span_attributes.t:IN [if_octets]  AND span_attributes.h:IN [ee7b566c-68d7-4ffb-9d0a-29477a39b004|ee7b566c-68d7-4ffb-9d0a-29477a39b012|ee7b566c-68d7-4ffb-9d0a-29477a39b013|ee7b566c-68d7-4ffb-9d0a-29477a39b020|ee7b566c-68d7-4ffb-9d0a-29477a39b024]\",\n" +
                    "\"max_hits\": 0,\n" +
                    "\"start_timestamp\": 1719844427,\n" +
                    "\"end_timestamp\": 1719845327\n" +
                    "}";

    public static final String SAMPLE_QUERY_8_WITH_CACHE=
            "//" + QueryParameters.TEXT_TTL + "=172800\n" +
                    "//" + QueryParameters.TEXT_REFRESH + "=10\n" +
                    "//"+ QueryParameters.TEXT_CACHE+"=false\n" +
                    "//eagercache=false\n" +
                    "//name=Hub Network Throughput (bps)\n" +
                    "//columns=/3/buckets/1/key,/3/buckets/1/value,/3/buckets/4/value\n" +
                    "//" + QueryParameters.TEXT_DBTYPE + "=qw\n" +
                    SAMPLE_QUERY_8;
}
