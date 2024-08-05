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

package com.facebook.presto.caching;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.influxdb.client.InfluxDBClient;
import com.quickwit.javaclient.ApiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.params.SetParams;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.function.Function;

//import static com.facebook.presto.influxdb.RedisCacheWorker.addOneStat;

public class InfluxdbUtil {
    public static boolean isCoordinator;
    public static String workerId;
    public static String workerIndexToRunIn;
    private final static Map<String, String> const_keywords = new HashMap<String, String>() {
        {
            put("aggregatewindow", "aggregateWindow");
            put("createempty", "createEmpty");
            put("columnkey", "columnKey");
            put("nonnegative", "nonNegative");
            put("rowkey", "rowKey");
            put("useprevious", "usePrevious");
            put("valuecolumn", "valueColumn");
            put("windowperiod", "windowPeriod");
            put("timesrc", "timeSrc");
            put("tolower", "toLower");
            put("toupper", "toUpper");
            put("\\:in \\[", "\\:IN \\[");
            put(" and ", " AND ");
        }
    };
    private static Map<String, String> keywords =new LinkedHashMap<>(const_keywords);
    public static void setKeywords(String ks){
        logger.error("Current keywords count :" + keywords.size());
        synchronized (inProgressLock) {
            if (ks != null && !ks.trim().equals("")) {
                String[] splits = ks.split(";");
                if (splits.length > 0) {
                    for (String split : splits) {
                        String[] kv = split.split(",");
                        if (kv.length > 1) {
                            if (kv.length > 2) {
                                logger.error("Configuration Error: keyword split:" + split);
                            }
                            String key = kv[0].toLowerCase(Locale.ENGLISH);
                            if (!keywords.containsKey(key)) {
                                keywords.put(key, kv[1]);
                            }
                        } else {
                            logger.error("Configuration Error: keyword split:" + split);
                        }
                    }
                } else {
                    logger.error("Configuration Error: keyword split:" + ks);
                }
            } else {
                logger.info("Empty keywords :" + ks);
            }
        }
    }
    static JedisPool jedisPool = null;


    public static String redisUrl = null;
    private static String token = "zNBXClD-3rbf82GiGNGNxx0lZsJeJ3RCc7ViONhffoKfp5tfbv1UtLGFFcw7IU9i4ebllttDWzaD3899LaRQKg==";
    private static String org = "ulak";
    private static String bucket = "collectd";
    private static final String time_interval = "-5m";
    private static Logger logger = LoggerFactory.getLogger(InfluxdbUtil.class);
    private static InfluxDBClient influxDBClient;
    private static ObjectMapper objectMapper = null;
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
        if(redisUrl == null){
            return null;
        }
        if (jedisPool == null) {
            final JedisPoolConfig poolConfig = buildPoolConfig();
            jedisPool = new JedisPool(poolConfig, redisUrl);

        }
        return jedisPool;
    }

    private InfluxdbUtil() {
    }


    public static String arrangeCase(String query) {
        Map<String, String> ktr = keywords;
        if (ktr == null || ktr.size() == 0) {
            ktr = const_keywords;
        }
        for (Map.Entry<String, String> kv: ktr.entrySet()){
            query = query.replaceAll(kv.getKey(),kv.getValue());
            logger.debug("Replacing keyword :" + kv.getKey() + " with " + kv.getValue() + " : Resulting in :" + query);
        }
        return query;
    }
    public static Map<Integer, Integer> inProgressLocks = new LinkedHashMap<>();
//    private static Object columnsGetLock = new Object();
    public static Object inProgressLock = new Object();
    public static <T> List<T> parseJsonArray(String json,
                                             Class<T> classOnWhichArrayIsDefined)
            throws IOException, ClassNotFoundException {
        ObjectMapper mapper = getObjectMapper();
        Class<T[]> arrayClass = (Class<T[]>) Class.forName("[L" + classOnWhichArrayIsDefined.getName() + ";");
        T[] objects = mapper.readValue(json, arrayClass);
        return Arrays.asList(objects);
    }

    static ObjectMapper getObjectMapper() {
        if(objectMapper==null) {
            objectMapper = new ObjectMapper();
            objectMapper.registerModule(new JavaTimeModule());
        }
        return objectMapper;
    }
    public static void setCacheItem(Jedis jedis,
                                    InfluxdbQueryParameters influxdbQueryParameters) throws JsonProcessingException {
        influxdbQueryParameters.setFinish(System.currentTimeMillis());
        SetParams param = new SetParams();
        param.ex(influxdbQueryParameters.getTtlInSeconds());
        jedis.set(getTrinoCacheString(influxdbQueryParameters.getHash()), getObjectMapper().writeValueAsString(influxdbQueryParameters), param);
    }

    public static String getTrinoCacheString(String hash){
        return "trino:" + hash;
    }
    public static String getTrinoCacheString(int hash){
        return getTrinoCacheString(String.valueOf(hash));
    }

    public static Iterator<InfluxdbRow> select(String tableName,
                                               boolean forceRefresh,
                                               Function<InfluxdbQueryParameters, ArrayList<InfluxdbRow>> exec) throws IOException {

        InfluxdbQueryParameters influxdbQueryParameters = InfluxdbQueryParameters.getQueryParameters(tableName);
        return select(influxdbQueryParameters,
                forceRefresh,
                exec);
    }
    public static Iterator<InfluxdbRow> select(InfluxdbQueryParameters influxdbQueryParameters,
                                               boolean forceRefresh,
                                               Function<InfluxdbQueryParameters, ArrayList<InfluxdbRow>> exec1) throws IOException {
        int hash = influxdbQueryParameters.getHash();
        influxdbQueryParameters.setStart(System.currentTimeMillis());

        JedisPool pool = getJedisPool();
        Jedis jedis = null;
        if(pool !=null){
            jedis = pool.getResource();
        }
        try {
            List<InfluxdbRow> fromCache = getCacheResultAsList(forceRefresh, jedis, hash);
            if (fromCache != null) {
                influxdbQueryParameters.setRows(fromCache);
                setCacheItem(jedis, influxdbQueryParameters);
                return fromCache.iterator();
            }
            synchronized (inProgressLock) {
                fromCache = getCacheResultAsList(forceRefresh, jedis, hash);
                if (fromCache != null) {
                    influxdbQueryParameters.setRows(fromCache);
                    setCacheItem(jedis, influxdbQueryParameters);
                    return fromCache.iterator();
                }
                if (!inProgressLocks.containsKey(hash)) {
                    inProgressLocks.put(hash, hash);
                }
            }
            try {
                synchronized (inProgressLocks.get(hash)) {

                    fromCache = getCacheResultAsList(forceRefresh, jedis, hash);
                    if (fromCache != null) {
                        influxdbQueryParameters.setRows(fromCache);
                        setCacheItem(jedis, influxdbQueryParameters);
                        return fromCache.iterator();
                    } else {
                        if (influxdbQueryParameters.isEagerCached() && !forceRefresh) {
                            LinkedList<InfluxdbRow> rows = new LinkedList<>();
                            influxdbQueryParameters.setRows(rows);
                            setCacheItem(jedis, influxdbQueryParameters);
                            return rows.iterator();
                        }
                    }

                    influxdbQueryParameters.setError("");
                    ArrayList<InfluxdbRow> list = exec1.apply(influxdbQueryParameters);
                    logger.debug("Running: " + hash);

                    if (jedis != null) {
                        influxdbQueryParameters.setRows(list);
                        influxdbQueryParameters.setFinish(System.currentTimeMillis());
                        setCacheItem(jedis, influxdbQueryParameters);
                    }
//                    addOneStat(hash, 1);
                    return list.iterator();
                }
            } finally {
                synchronized (inProgressLock) {
                    if (inProgressLocks.containsKey(hash)) {
                        inProgressLocks.remove(hash);
                    }
                }
            }
        }finally {
            if(jedis!=null){
                jedis.close();
            }
        }
    }

    public static void invalidateCache(int hash )  {
        JedisPool pool = getJedisPool();
        Jedis jedis = null;
        if(pool !=null){
            jedis = pool.getResource();
        }
        if(jedis!=null) {
            try {
                synchronized (inProgressLock) {
                    if (!inProgressLocks.containsKey(hash)) {
                        inProgressLocks.put(hash, hash);
                    }
                }
                synchronized (inProgressLocks.get(hash)) {
                    try {
                        jedis.del(getTrinoCacheString(hash));
                    } finally {
                        synchronized (inProgressLock) {
                            if (inProgressLocks.containsKey(hash)) {
                                inProgressLocks.remove(hash, hash);
                            }
                        }
                    }
                }
            } finally {
                if (jedis != null) {
                    jedis.close();
                }
            }
        }
    }
    private static Iterator<InfluxdbRow> getCacheResult(boolean forceRefresh,
                                                        Jedis jedis,
                                                        int hash) throws JsonProcessingException {
        List<InfluxdbRow> resultAsList = getCacheResultAsList(forceRefresh, jedis, hash);
        if (resultAsList != null) {
            return resultAsList.iterator();
        }
        return null;
    }

    public static List<InfluxdbRow> getCacheResultAsList(boolean forceRefresh,
                                                         Jedis jedis,
                                                         int hash) throws JsonProcessingException {
        if(jedis != null) {
            String json;
            List<InfluxdbRow> list;
            json = jedis.get(getTrinoCacheString(hash));
            if(json!=null){
//                addOneStat(hash, 1);
            }
            if (!forceRefresh && json != null) {
                InfluxdbQueryParameters influxdbQueryParameters = getObjectMapper().readValue(json, InfluxdbQueryParameters.class);
                return influxdbQueryParameters.getRows();
            }
        }
        return null;
    }
    public static void main(String[] args) throws ApiException  {
        DBType dbType= DBType.INFLUXDB2;
        long start = System.currentTimeMillis();
//        InfluxdbConnector c = new InfluxdbConnector()
//        redisUrl = "http://10.20.4.53:30485?readTimeout=2m&connectTimeout=2m&writeTimeout=2m";
////        redisUrl = "redis://:ulak@10.20.4.53:31671";
//        instance(redisUrl,
//                "ulak",
//                "KbSyJTKzIuvxqpKjVMTautakGg7uPxGTF3hz878Ye4CH_UgTl0k4W2UXYy79dwrzSle9QmEt2KCde0Sf88qhSQ==",
//                "collectd");
//        setKeywords("");
//        String name = getTableNameForHash(SAMPLE_QUERY_3_WITH_CACHE);
//        tryOneQuery(dbType, SAMPLE_QUERY_4, 1);
//        tryOneQuery(dbType, SAMPLE_QUERY_4_WITH_CACHE, 1);
//
//
//        tryOneQuery(dbType, SAMPLE_QUERY_4, 1);
//        tryOneQuery(dbType, SAMPLE_QUERY_4_WITH_CACHE, 1);
//
//        tryOneQuery(dbType, SAMPLE_QUERY, 1);
//        tryOneQuery(dbType, SAMPLE_QUERY_WITH_CACHE, 1);
//        tryOneQuery(dbType, SAMPLE_QUERY_2, 1);
//        tryOneQuery(dbType, SAMPLE_QUERY_2_WITH_CACHE, 1);
//        tryOneQuery(dbType, SAMPLE_QUERY_3, 1);
//        tryOneQuery(dbType, SAMPLE_QUERY_3_WITH_CACHE, 1);

//        dbType = DBType.PG;
//        PGUtil.pgUrl = "jdbc:postgresql://10.20.4.53:30758/grafana";
//        PGUtil.pgUser = "postgres";
//        PGUtil.pgPwd = "m1latDB";
////        redisUrl = "redis://:ulak@10.20.4.53:31671";
//        PGUtil.instance(PGUtil.pgUrl,
//                PGUtil.pgUser,
//                PGUtil.pgPwd);
//        setKeywords("");
////        tryOneQuery(dbType, SAMPLE_QUERY_6_WITH_CACHE, 1);
//        tryOneQuery(dbType, SAMPLE_QUERY_6_WITH_CACHE, 1);


        dbType = DBType.QW;
//        tryOneQuery(dbType, SAMPLE_QUERY_6, 1);
//        InfluxdbQueryParameters params = InfluxdbQueryParameters.getQueryParameters(null,"{\n" +
//                "  \"aggs\": {\n" +
//                "    \"6\": {\n" +
//                "      \"aggs\": {\n" +
//                "        \"5\": {\n" +
//                "          \"aggs\": {\n" +
//                "            \"1\": {\n" +
//                "              \"min\": {\n" +
//                "                \"field\": \"span_attributes.tx\"\n" +
//                "              }\n" +
//                "            },\n" +
//                "            \"9\": {\n" +
//                "              \"aggs\": {\n" +
//                "                \"a\": {\n" +
//                "                  \"aggs\": {\n" +
//                "                    \"2\": {\n" +
//                "                      \"min\": {\n" +
//                "                        \"field\": \"span_attributes.tx\"\n" +
//                "                      }\n" +
//                "                    },\n" +
//                "                    \"21\": {\n" +
//                "                      \"max\": {\n" +
//                "                        \"field\": \"span_attributes.tx\"\n" +
//                "                      }\n" +
//                "                    },\n" +
//                "                    \"3\": {\n" +
//                "                      \"min\": {\n" +
//                "                        \"field\": \"span_attributes.rx\"\n" +
//                "                      }\n" +
//                "                    },\n" +
//                "                    \"4\": {\n" +
//                "                      \"max\": {\n" +
//                "                        \"field\": \"span_attributes.rx\"\n" +
//                "                      }\n" +
//                "                    }\n" +
//                "                  },\n" +
//                "                  \"terms\": {\n" +
//                "                    \"field\": \"span_attributes.t\",\n" +
//                "                    \"size\": 1000,\n" +
//                "                    \"order\": {\n" +
//                "                      \"21\": \"desc\"\n" +
//                "                    },\n" +
//                "                    \"min_doc_count\": 1\n" +
//                "                  }\n" +
//                "                }\n" +
//                "              },\n" +
//                "              \"terms\": {\n" +
//                "                \"field\": \"span_attributes.pi\",\n" +
//                "                \"size\": 1000,\n" +
//                "                \"order\": {\n" +
//                "                  \"_key\": \"desc\"\n" +
//                "                },\n" +
//                "                \"min_doc_count\": 1\n" +
//                "              }\n" +
//                "            }\n" +
//                "          },\n" +
//                "          \"terms\": {\n" +
//                "            \"field\": \"span_attributes.h\",\n" +
//                "            \"order\": {\n" +
//                "              \"1\": \"desc\"\n" +
//                "            },\n" +
//                "            \"min_doc_count\": 1\n" +
//                "          }\n" +
//                "        }\n" +
//                "      },\n" +
//                "      \"date_histogram\": {\n" +
//                "        \"field\": \"span_start_timestamp_nanos\",\n" +
//                "        \"fixed_interval\": \"10s\",\n" +
//                "        \"min_doc_count\": 1\n" +
//                "      }\n" +
//                "    }\n" +
//                "  },\n" +
//                "  \"query\": \"span_attributes.p:interface AND span_attributes.h:IN [5e0bb4d8-4661-426a-81fc-e5b8bbc9e9df]\",\n" +
//                "  \"max_hits\": 0,\n" +
//                "  \"start_timestamp\": now - (90*d) ,\n" +
//                "  \"end_timestamp\": now\n" +
//                "}");

//        params.setQuery(replaceAll(params.getQuery(),"|"," "));
//        params.setQuery(replaceAll(params.getQuery()," not "," NOT "));
//        params.setQwIndex("metrics3");
//        params.setQwUrl("http://10.20.4.53:31410");
//        params.setReplaceFromColumns("/3/buckets/2/buckets/4/buckets/5/buckets/6/buckets/7/buckets/8/buckets/9/buckets/10/buckets/1");
//        params.setHasJs(true);
//        List<InfluxdbRow> ret = QwUtil.executeOneQuery(null,params,params.getQwIndex(), params.getQuery());
//        logger.info(String.valueOf(System.currentTimeMillis() - start));

    }

//    private static void tryOneQuery(DBType dbType, String query, int runCount) throws IOException, ClassNotFoundException, SQLException, ApiException {
//        int index=0;
//        query = query.toLowerCase(Locale.ENGLISH);
//        logger.info("Testing no-cache queries");
//        while(index <runCount) {
//            getColumns("", query);
//            logger.info("columns 1: " + index);
//            switch (dbType){
//                case INFLUXDB2:
//                    select(query, false);
//                    logger.info("query 1: " + index);
//                    break;
//                case PG:
//                    PGUtil.select(query, true);
//                    logger.info("query 1: " + index);
//                    break;
//                case QW:
//                    QwUtil.select(query, true);
//                    logger.info("query 1: " + index);
//                    break;
//            }
//            index++;
//        }
//    }
}
