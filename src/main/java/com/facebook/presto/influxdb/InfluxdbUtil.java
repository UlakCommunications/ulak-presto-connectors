/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.influxdb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.influxdb.client.BucketsApi;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.QueryApi;
import com.influxdb.client.domain.Bucket;
import com.influxdb.query.FluxColumn;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.VarcharType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.params.SetParams;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class InfluxdbUtil {
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
    static JedisPool jedisPool = null;

    static {
        RedisCacheWorker redisCacheWorker = new RedisCacheWorker();
        redisCacheWorker.start();
    }

    public static final int TTL = 60 * 60 * 24;
    public static String redisUrl = "redis://10.20.4.53:30639";
    private static String token = "zNBXClD-3rbf82GiGNGNxx0lZsJeJ3RCc7ViONhffoKfp5tfbv1UtLGFFcw7IU9i4ebllttDWzaD3899LaRQKg==";
    private static String org = "ulak";
    private static String bucket = "collectd";
    private static final String time_interval = "-5m";
    private static Logger logger = LoggerFactory.getLogger(InfluxdbUtil.class);
    private static InfluxDBClient influxDBClient;

    private static JedisPoolConfig buildPoolConfig() {
        final JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(128);
        poolConfig.setMaxIdle(128);
        poolConfig.setMinIdle(16);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setMinEvictableIdleTimeMillis(Duration.ofSeconds(60).toMillis());
        poolConfig.setTimeBetweenEvictionRunsMillis(Duration.ofSeconds(30).toMillis());
        poolConfig.setNumTestsPerEvictionRun(3);
        poolConfig.setBlockWhenExhausted(true);
        return poolConfig;
    }

    public static JedisPool getJedisPool() {
        if (jedisPool == null) {
            final JedisPoolConfig poolConfig = buildPoolConfig();
            jedisPool = new JedisPool(poolConfig, redisUrl);

        }
        return jedisPool;
    }

    private InfluxdbUtil() {
    }

    public static void instance(String url, String org, String token, String bucket)
            throws IOException {
        InfluxdbUtil.org = org;
        InfluxdbUtil.token = token;
        InfluxdbUtil.bucket = bucket;
        influxDBClient = InfluxDBClientFactory.create(url, token.toCharArray(), org);
    }

    public static List<String> getSchemas() {
        logger.debug("getSchemas");
        logger.debug("influxdbUtil-getSchemas");
        List<String> res = new ArrayList<>();
        BucketsApi bucketsApi = influxDBClient.getBucketsApi();
        for (Bucket bucket1 : bucketsApi.findBuckets()) {
            res.add(bucket1.getName());
            logger.debug(bucket1.getName());
        }
        return res;
    }

    public static List<String> getTableNames(String bucket) {
        logger.debug("influxdbUtil- bucket->tableNames:" + bucket);
        //logger.debug("getTableNames in bucket: {}", bucket);
        List<String> res = new ArrayList<>();
        QueryApi queryApi = influxDBClient.getQueryApi();
        String flux = "import  \"influxdata/influxdb/schema\"\n" + "import \"strings\"\n" + "schema.measurements(bucket: \"" + bucket + "\")\n" + "|> filter(fn : (r) => not strings.hasPrefix(v: r._value, prefix: \"task\"))\n" + "|> filter(fn : (r) => not strings.hasPrefix(v: r._value, prefix: \"storage\"))\n" + "|> filter(fn : (r) => not strings.hasPrefix(v: r._value, prefix: \"service\"))\n" + "|> filter(fn : (r) => not strings.hasPrefix(v: r._value, prefix: \"query\"))\n" + "|> filter(fn : (r) => not strings.hasPrefix(v: r._value, prefix: \"qc\"))\n" + "|> filter(fn : (r) => not strings.hasPrefix(v: r._value, prefix: \"influxdb\"))\n" + "|> filter(fn : (r) => not strings.hasPrefix(v: r._value, prefix: \"http\"))\n" + "|> filter(fn : (r) => not strings.hasPrefix(v: r._value, prefix: \"go\"))\n" + "|> filter(fn : (r) => not strings.hasPrefix(v: r._value, prefix: \"boltdb\"))";
        List<FluxTable> tables = queryApi.query(flux);
        for (FluxTable fluxTable : tables) {
            List<FluxRecord> records = fluxTable.getRecords();
            for (FluxRecord fluxRecord : records) {
                res.add((String) fluxRecord.getValue());
                logger.debug(String.valueOf(fluxRecord.getValue()));
            }
        }
        return res;
    }

    public static String arrangeCase(String query) {
        return query
                .replaceAll("aggregatewindow", "aggregateWindow")
                .replaceAll("createempty", "createEmpty")
                .replaceAll("columnkey", "columnKey")
                .replaceAll("nonnegative", "nonNegative")
                .replaceAll("rowkey", "rowKey")
                .replaceAll("useprevious", "usePrevious")
                .replaceAll("valuecolumn", "valueColumn")
                .replaceAll("windowperiod", "windowPeriod");
    }

    //    private  static Map<Integer, List<ColumnMetadata>> columns = new LinkedHashMap<>();
//    private static Object columnsGetLock = new Object();
    private static Object queriesLock = new Object();

    public static List<ColumnMetadata> getColumns(String bucket, String tableName) throws IOException, ClassNotFoundException {
        tableName = arrangeCase(tableName);
        logger.debug("influxdbUtil bucket:" + bucket + "table:" + tableName + " columnsMetadata");
        //logger.debug("getColumns in bucket: {}, table : {}", bucket, tableName);
        List<ColumnMetadata> res = new ArrayList<>();
        // all fields
//        String flux = "from(bucket: \"" + bucket + "\")\n" +
//                "  |> range(start: " + time_interval + ")\n" +
//                "  |> filter(fn: (r) => r[\"_measurement\"] == \"" + tableName + "\")";
        Iterator<InfluxdbRow> tables = select(tableName,false);
        for (Iterator<InfluxdbRow> it = tables; it.hasNext(); ) {
            InfluxdbRow fluxTable = it.next();
            Map<String, Object> records = fluxTable.getColumnMap();
            for (String record : records.keySet()) {
                if (!res.stream().anyMatch(t -> t.getName().equals(record))) {
                    res.add(new ColumnMetadata( record, VarcharType.VARCHAR));
                }
            }
        }
//        // all tags
//        flux = "import \"influxdata/influxdb/schema\"\n" + "schema.measurementTagKeys(\n"
//                + "    bucket : \"" + bucket + "\",\n" + "    measurement : \"" + tableName + "\",\n" + "    start : " + time_interval + ")";
//        tables = queryApi.query(flux);
//        for (FluxTable fluxTable : tables) {
//            List<FluxRecord> records = fluxTable.getRecords();
//            for (FluxRecord record : records) {
//                if (!((String) record.getValue()).startsWith("_")) {
//                    res.add(new ColumnMetadata((String) record.getValue(), VarcharType.VARCHAR));
//                }
//            }
//        }
//        res.add(new ColumnMetadata("_time", VarcharType.VARCHAR));
        for (ColumnMetadata columnMetadata : res) {
            logger.debug(columnMetadata.getName() + ":" + columnMetadata.getType().getDisplayName());
        }
        return res;
    }
    public static <T> List<T> parseJsonArray(String json,
                                             Class<T> classOnWhichArrayIsDefined)
            throws IOException, ClassNotFoundException {
        ObjectMapper mapper = getObjectMapper();
        Class<T[]> arrayClass = (Class<T[]>) Class.forName("[L" + classOnWhichArrayIsDefined.getName() + ";");
        T[] objects = mapper.readValue(json, arrayClass);
        return Arrays.asList(objects);
    }

    static ObjectMapper getObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule( new JavaTimeModule());
        return mapper;
    }

    public static String getTrinoCacheString(String hash){
        return "trino:" + hash;
    }
    public static String getTrinoCacheString(int hash){
        return getTrinoCacheString(String.valueOf(hash));
    }
    public static Iterator<InfluxdbRow> select(String origTableName, boolean forceRefresh) throws IOException, ClassNotFoundException {
        String tableName = arrangeCase(origTableName);
//        String tblNoRange = null;
//        String newLineChar = System.lineSeparator();
//        String[] splits = tableName.split(newLineChar);
//        List<String> newLines = new ArrayList<>();
//        for (int i = 0; i < splits.length; i++) {
//            String current = splits[i];
//            if(!current.matches("[ ]*\\|\\>[ ]*range[ ]*\\(")){
//                newLines.add(current);
//            }
//        }
//        tblNoRange = String.join(newLineChar,newLines);
//        int hash = tblNoRange.hashCode();
        int hash = tableName.hashCode();
        try (Jedis jedis = getJedisPool().getResource()) {
            String json = jedis.get(getTrinoCacheString(hash));
            List<InfluxdbRow> list = null;
            if (!forceRefresh && json != null) {
                InfluxdbQueryParameters influxdbQueryParameters = getObjectMapper().readValue(json, InfluxdbQueryParameters.class);
                list = influxdbQueryParameters.getRows();
            } else {
                synchronized (queriesLock) {
                    json = jedis.get(getTrinoCacheString(hash));
                    if (!forceRefresh && json != null) {
                        InfluxdbQueryParameters influxdbQueryParameters = getObjectMapper().readValue(json, InfluxdbQueryParameters.class);
                        list = influxdbQueryParameters.getRows();
                    } else {
                        //logger.debug("select all rows in table: {}", tableName);
                        list = new ArrayList<>();
                        QueryApi queryApi = influxDBClient.getQueryApi();
                        String flux = tableName;//"from(bucket: " + "\"" + bucket + "\"" + ")\n" + "|> range(start:" + time_interval + ")\n" + "|> filter(fn : (r) => r._measurement == " + "\"" + tableName + "\"" + ")";
                        List<FluxTable> tables = queryApi.query(flux, org);
                        Map<Instant, Map<String, Object>> resMap = new HashMap<>();
                        for (FluxTable fluxTable : tables) {
                            List<FluxRecord> records = fluxTable.getRecords();
                            for (FluxRecord fluxRecord : records) {
                                Map<String, Object> curRow = resMap.get(fluxRecord.getTime());
                                if (curRow == null) {
                                    curRow = fluxRecord.getValues();
                                    Map<String, Object> newRow = new HashMap<>();
                                    for (Map.Entry<String, Object> entry : curRow.entrySet()) {
//                                if (!Objects.equals(entry.getKey(), "_field") && !Objects.equals(entry.getKey(), "_value")) {
                                        newRow.put(entry.getKey(), entry.getValue());
//                                }
                                    }
                                    String field = fluxRecord.getField();
                                    if (field != null) {
                                        newRow.put(field, fluxRecord.getValue());
                                    }
                                    Instant time = fluxRecord.getTime();
                                    if (time == null) {
                                        time = Instant.now();
                                    }
                                    resMap.put(time, newRow);
                                } else {
                                    String field = fluxRecord.getField();
                                    if (field == null) {
                                        field = "null";
                                    }
                                    curRow.put(field, fluxRecord.getValue());
                                }
                            }
                        }
                        // for debug
                        for (Map.Entry<Instant, Map<String, Object>> entry : resMap.entrySet()) {
//                      for (Map.Entry<String, Object> entry1 : entry.getValue().entrySet()) {
//                          logger.debug("k-v pair " + entry1.getKey() + ":" + entry1.getValue().toString() + ", " + entry1.getValue().getClass());
//                      }
                            list.add(new InfluxdbRow(entry.getValue()));
                        }
                        ObjectMapper mapper = getObjectMapper();

                        InfluxdbQueryParameters p = new InfluxdbQueryParameters();
                        p.setQuery(origTableName);
                        p.setHash(hash);
                        p.setRows(list);

                        SetParams param = new SetParams();
                        param.ex(TTL);
                        jedis.set(getTrinoCacheString(hash), mapper.writeValueAsString(p), param);
                    }
                }
            }
            return list.iterator();
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        int index=0;
        long start = System.currentTimeMillis();
        instance("http://10.20.4.53:30485?readTimeout=2m&connectTimeout=2m&writeTimeout=2m",
                "ulak",
                "KbSyJTKzIuvxqpKjVMTautakGg7uPxGTF3hz878Ye4CH_UgTl0k4W2UXYy79dwrzSle9QmEt2KCde0Sf88qhSQ==",
                "collectd");
        while(index<100) {
            getColumns("", SAMPLE_QUERY);
            select(SAMPLE_QUERY, false);
            getColumns("", SAMPLE_QUERY_2);
            select(SAMPLE_QUERY_2, false);
            index++;
        }
        logger.info(String.valueOf(System.currentTimeMillis() - start));
    }
}
