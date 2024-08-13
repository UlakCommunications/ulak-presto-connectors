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

package com.facebook.presto.ulak.caching;

import com.facebook.presto.ulak.QueryParameters;
import com.facebook.presto.ulak.UlakRow;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.collect.Lists;
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
import java.util.*;
import java.util.function.Function;


public class ConnectorBaseUtil {
    public static final int NONE_CACHE_TTL_IN_SECONDS = 5;
    public static boolean isCoordinator;
    public static String workerId;
    public static String workerIndexToRunIn;
    private static final Map<String, String> const_keywords = new HashMap<String, String>() {
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
        String errorString = "Configuration Error: keyword split: {}";
        logger.error("Current keywords count : {}", keywords.size());
        synchronized (inProgressLock) {
            if (ks != null && !ks.trim().equals("")) {
                String[] splits = ks.split(";");
                if (splits.length > 0) {
                    for (String split : splits) {
                        String[] kv = split.split(",");
                        if (kv.length > 1) {
                            if (kv.length > 2) {
                                logger.error(errorString, split);
                            }
                            String key = kv[0].toLowerCase(Locale.ENGLISH);
                            if (!keywords.containsKey(key)) {
                                keywords.put(key, kv[1]);
                            }
                        } else {
                            logger.error(errorString, split);
                        }
                    }
                } else {
                    logger.error(errorString, ks);
                }
            } else {
                logger.info("Empty keywords : {}", ks);
            }
        }
    }
    static JedisPool jedisPool = null;


    public static String redisUrl = null;
    private static Logger logger = LoggerFactory.getLogger(ConnectorBaseUtil.class);
    private static ObjectMapper objectMapper = null;

    private static JedisPoolConfig buildPoolConfig() {
        final JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(128);
        poolConfig.setMaxIdle(128);
        poolConfig.setMinIdle(16);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setMinEvictableIdleDuration(Duration.ofSeconds(60));
        poolConfig.setTimeBetweenEvictionRuns(Duration.ofSeconds(30));
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

    private ConnectorBaseUtil() {
    }


    public static String arrangeCase(String query) {
        Map<String, String> ktr = keywords;
        if (ktr == null || ktr.size() == 0) {
            ktr = const_keywords;
        }
        for (Map.Entry<String, String> kv: ktr.entrySet()){
            query = query.replaceAll(kv.getKey(),kv.getValue());
            logger.debug("Replacing keyword : {} with {} : Resulting in {}",kv.getKey(), kv.getValue(), query);
        }
        return query;
    }
    public static final Map<Integer, Object> inProgressLocks = new LinkedHashMap<>();
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
                                    QueryParameters queryParameters) throws JsonProcessingException {
        queryParameters.setFinish(System.currentTimeMillis());
        SetParams param = new SetParams();
        if(queryParameters.isToBeCached()) {
            param.ex(queryParameters.getTtlInSeconds());
        }else{
            param.ex(NONE_CACHE_TTL_IN_SECONDS);
        }
        jedis.set(getTrinoCacheString(queryParameters.getHash()),
                getObjectMapper().writeValueAsString(queryParameters), param);
    }

    public static String getTrinoCacheString(String hash){
        return "trino:" + hash;
    }
    public static String getTrinoCacheString(int hash){
        return getTrinoCacheString(String.valueOf(hash));
    }

    public static List<UlakRow> select(String tableName,
                                               boolean forceRefresh,
                                               Function<QueryParameters, List<UlakRow>> exec) throws IOException {

        QueryParameters queryParameters = QueryParameters.getQueryParameters(tableName);
        return select(queryParameters,
                forceRefresh,
                exec);
    }
    public static List<UlakRow> select(QueryParameters queryParameters,
                                               boolean forceRefresh,
                                               Function<QueryParameters, List<UlakRow>> exec1) throws IOException {
        int hash = queryParameters.getHash();
        queryParameters.setStart(System.currentTimeMillis());

        JedisPool pool = getJedisPool();
        Jedis jedis = null;
        if(pool !=null){
            jedis = pool.getResource();
        }
        try {
            List<UlakRow> fromCache = getCacheResultAsList(forceRefresh, jedis, hash);
            if (fromCache != null) {
                queryParameters.setRows(fromCache);
                setCacheItem(jedis, queryParameters);
                return fromCache ;
            }
            synchronized (inProgressLock) {
                fromCache = getCacheResultAsList(forceRefresh, jedis, hash);
                if (fromCache != null) {
                    queryParameters.setRows(fromCache);
                    setCacheItem(jedis, queryParameters);
                    return fromCache ;
                }
                if (!inProgressLocks.containsKey(hash)) {
                    inProgressLocks.put(hash, new Object());
                }
            }
            try {
                synchronized (inProgressLocks.get(hash)) {

                    fromCache = getCacheResultAsList(forceRefresh, jedis, hash);
                    if (fromCache != null) {
                        queryParameters.setRows(fromCache);
                        setCacheItem(jedis, queryParameters);
                        return fromCache ;
                    } else {
                        //TODO: eager caching is to be added
//                        if (queryParameters.isEagerCached() && !forceRefresh) {
//                            LinkedList<UlakRow> rows = new LinkedList<>();
//                            queryParameters.setRows(rows);
//                            setCacheItem(jedis, queryParameters);
//                            return rows.iterator();
//                        }
                    }

                    queryParameters.setError("");
                    List<UlakRow> list = exec1.apply(queryParameters);
                    logger.debug("Running: {}", hash);

                    if (jedis != null) {
                        queryParameters.setRows(Lists.newArrayList(list));
                        queryParameters.setFinish(System.currentTimeMillis());
                        setCacheItem(jedis, queryParameters);
                    }
                    //TODO: stat checking is to be added
//                    addOneStat(hash, 1);
                    return list ;
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
                        inProgressLocks.put(hash, new Object());
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
    private static Iterator<UlakRow> getCacheResult(boolean forceRefresh,
                                                        Jedis jedis,
                                                        int hash) throws JsonProcessingException {
        List<UlakRow> resultAsList = getCacheResultAsList(forceRefresh, jedis, hash);
        if (resultAsList != null) {
            return resultAsList.iterator();
        }
        return null;
    }

    public static List<UlakRow> getCacheResultAsList(boolean forceRefresh,
                                                         Jedis jedis,
                                                         int hash) throws JsonProcessingException {
        if(jedis != null) {
            String json;
            List<UlakRow> list;
            json = jedis.get(getTrinoCacheString(hash));
            if(json!=null){
//                addOneStat(hash, 1);
            }
            if (!forceRefresh && json != null) {
                QueryParameters queryParameters = getObjectMapper().readValue(json, QueryParameters.class);
                return queryParameters.getRows();
            }
        }
        return Collections.emptyList();
    }


    public static List<ColumnMetadata> getColumnsBase(List<UlakRow> tables) throws Exception {
        List<ColumnMetadata> res = new ArrayList<>();


        if (tables!=null) {
            for (UlakRow fluxTable :tables) {
                Map<String, Object> records = fluxTable.getColumnMap();
                for (String rec : records.keySet()) {
                    if (res.stream().noneMatch(t -> t.getName().equals(rec))) {
                        res.add(new ColumnMetadata(rec, VarcharType.VARCHAR));
                    }
                }
            }
        } else {
            throw new NullPointerException("Empty Query");
            //TODO: eager caching is to be added
//            String[] cols = QueryParameters.getQueryParameters(tableName).getColumns();
//            if (cols.length > 0) {
//                for (String record : cols) {
//                    if (!res.stream().anyMatch(t -> t.getName().equals(record))) {
//                        res.add(new ColumnMetadata(record, VarcharType.VARCHAR));
//                    }
//                }
//            }
        }
        for (ColumnMetadata columnMetadata : res) {
            logger.debug("{}:{}", columnMetadata.getName(), columnMetadata.getType().getDisplayName());
        }
        return res;
    }
}
