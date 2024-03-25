package com.facebook.presto.influxdb;

import static com.facebook.presto.influxdb.InfluxdbQueryParameters.*;

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
    public static final String SAMPLE_QUERY_WITH_CACHE= "//" + TEXT_CACHE + "=true\n"
            +"//" +  TEXT_TTL + "=60\n"
            +"//" +  TEXT_REFRESH + "=10\n"
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

    public static final String SAMPLE_QUERY_2_WITH_CACHE= "//" + TEXT_CACHE + "=true\n"
            + "//" + TEXT_TTL + "=60\n"
            + "//" + TEXT_REFRESH + "=10\n"
            + "//" + SAMPLE_QUERY_2;
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

    public static final String SAMPLE_QUERY_3_WITH_CACHE= "//" + TEXT_CACHE + "=true\n"
            + "//" + TEXT_TTL + "=60\n"
            + "//" + TEXT_REFRESH + "=10\n"
            + "//" + SAMPLE_QUERY_3;
    public static final String SAMPLE_QUERY_4 =
            "from(bucket: \"otlp_metric\")\n" +
                    "  |> range(start:  -5m)\n" +
                    "  |> filter(fn: (r) => r[\"_measurement\"] == \"maya_ifstatus\")\n" +
                    "  |> aggregateWindow(every: 1s, fn: mean, createEmpty: false)\n" +
                    "  |> last()\n" +
                    "  |> group()\n" +
                    "  |> map(fn: (r) => ({r with jn: r.host + \"-\" + r.plugin_instance , has_data: true  }))\n" +
                    "  |> yield(name: \"last\")";

    public static final String SAMPLE_QUERY_4_WITH_CACHE= "//" + TEXT_CACHE + "=true\n"
            + "//" + TEXT_TTL + "=60\n"
            + "//" + TEXT_REFRESH + "=10\n"
            + "//" + SAMPLE_QUERY_4;
}
