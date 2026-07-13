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
import java.util.function.BiFunction;

import static com.facebook.presto.ulak.QueryParameters.getTableNameForHash;


public class ConnectorBaseUtil {
    public static final int NONE_CACHE_TTL_IN_SECONDS = 10;
    public static boolean isCoordinator;
    public static String workerId;
    public static String workerIndexToRunIn;
    private static final Map<String, String> const_keywords = new HashMap<>();

    static {
        const_keywords.put("aggregatewindow", "aggregateWindow");
        const_keywords.put("createempty", "createEmpty");
        const_keywords.put("columnkey", "columnKey");
        const_keywords.put("nonnegative", "nonNegative");
        const_keywords.put("rowkey", "rowKey");
        const_keywords.put("useprevious", "usePrevious");
        const_keywords.put("valuecolumn", "valueColumn");
        const_keywords.put("windowperiod", "windowPeriod");
        const_keywords.put("timesrc", "timeSrc");
        const_keywords.put("tolower", "toLower");
        const_keywords.put("toupper", "toUpper");
        const_keywords.put("\\:in \\[", "\\:IN \\[");
        const_keywords.put(" and ", " AND ");
        const_keywords.put("\\\"\\\"", "\"\"");
    }

    private static Map<String, String> keywords =new LinkedHashMap<>(const_keywords);
    public static void setKeywords(String ks){
        String errorString = "Configuration Error: keyword split: {}";
        logger.debug("Current keywords count : {}", keywords.size());
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
                logger.warn("Empty keywords : {}", ks);
            }
        }
    }
    static volatile JedisPool jedisPool = null;


    public static String redisUrl = null;
    private static final Logger logger = LoggerFactory.getLogger(ConnectorBaseUtil.class);
    private static volatile ObjectMapper objectMapper = null;

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            JedisPool p = jedisPool;
            if (p != null) {
                try {
                    p.close();
                } catch (Exception e) {
                    logger.warn("Error closing Jedis pool on shutdown", e);
                }
            }
        }, "connector-base-jedis-shutdown"));
    }

    private static JedisPoolConfig buildPoolConfig() {
        final JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(1000);
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

    public static String arrangeCase(String query) {
        final Map<String, String> ktr;
        synchronized (inProgressLock) {
            ktr = (keywords == null || keywords.isEmpty()) ? const_keywords : keywords;
        }
        for (Map.Entry<String, String> kv: ktr.entrySet()){
            query = query.replaceAll(kv.getKey(),kv.getValue());
            logger.debug("Replacing keyword : {} with {} : Resulting in {}",kv.getKey(), kv.getValue(), query);
        }
        return query;
    }
    public static final Map<String, Object> inProgressLocks = new LinkedHashMap<>();
    public static Object inProgressLock = new Object();


    public static ObjectMapper getObjectMapper() {
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
        jedis.set(getTrinoCacheString(queryParameters.getCacheKey()),
                getObjectMapper().writeValueAsString(queryParameters), param);
    }

    public static String getTrinoCacheString(String hash){
        return "trino:" + hash;
    }
    public static String getTrinoCacheString(int hash){
        return getTrinoCacheString(String.valueOf(hash));
    }

    public static String getDefaultParameter(String[] s, int i) {
        return s != null && i > 0 && s.length > i ? s[i] : "";
    }
    public static List<UlakRow> select(QueryParameters queryParameters,
                                               boolean forceRefresh,
                                                String[] defaultParameters,
                                       BiFunction<QueryParameters,String[], List<UlakRow>> exec1) throws IOException {
        int hash = queryParameters.getHash();

        if(queryParameters.isToBeCached() && queryParameters.getHasJs()) {
            if (queryParameters.getFrom() > 0
                    && queryParameters.getTo() > 0) {
//                long difFrom2Now = System.currentTimeMillis() / 1000 - queryParameters.getFrom();
                long difTo2Now = System.currentTimeMillis() / 1000 - queryParameters.getTo();
                long diffInSecods = queryParameters.getTo() - queryParameters.getFrom();
                if (diffInSecods == 300 && difTo2Now <= 60) {//this is a last five mins query
                    queryParameters.setQuery(queryParameters.getQuery()
                            .replaceAll(String.valueOf(queryParameters.getFrom()),
                                    "now - (5*m)")
                            .replaceAll(String.valueOf(queryParameters.getTo()),
                                    "now "));

                    String tableNameForHash = getTableNameForHash(queryParameters.getQuery());

                    hash = tableNameForHash.hashCode();
                    queryParameters.setHash(hash);
                    queryParameters.setCacheKey(QueryParameters.sha256Hex(tableNameForHash));
                }
            }
        }
        queryParameters.setStart(System.currentTimeMillis());

        String cacheKey = queryParameters.getCacheKey();
        JedisPool pool = getJedisPool();
        try (Jedis jedis = pool != null ? pool.getResource() : null) {
            List<UlakRow> fromCache = getCacheResultAsList(forceRefresh, jedis, cacheKey);
            if (fromCache != null) {
                queryParameters.setRows(fromCache);
                setCacheItem(jedis, queryParameters);
                return fromCache;
            }
            synchronized (inProgressLock) {
                fromCache = getCacheResultAsList(forceRefresh, jedis, cacheKey);
                if (fromCache != null) {
                    queryParameters.setRows(fromCache);
                    setCacheItem(jedis, queryParameters);
                    return fromCache;
                }

                if (!inProgressLocks.containsKey(cacheKey)) {
                    inProgressLocks.put(cacheKey, new Object());
                }
            }
            try {
                synchronized (inProgressLocks.get(cacheKey)) {

                    fromCache = getCacheResultAsList(forceRefresh, jedis, cacheKey);
                    if (fromCache != null) {
                        queryParameters.setRows(fromCache);
                        setCacheItem(jedis, queryParameters);
                        return fromCache;
                    }

                    queryParameters.setError("");
                    long execStart = System.currentTimeMillis();
                    List<UlakRow> list = exec1.apply(queryParameters, defaultParameters);
                    long execMs = System.currentTimeMillis() - execStart;
                    if (execMs > 5000) {
                        logger.warn("SLOW_QUERY {}ms name={} index={} rows={}",
                                execMs,
                                queryParameters.getName(),
                                queryParameters.getQwIndex(),
                                list == null ? 0 : list.size());
                    } else {
                        logger.debug("query {}ms name={}", execMs, queryParameters.getName());
                    }

                    if (jedis != null) {
                        queryParameters.setRows(Lists.newArrayList(list));
                        queryParameters.setFinish(System.currentTimeMillis());
                        setCacheItem(jedis, queryParameters);
                    }
                    return list;
                }
            } finally {
                synchronized (inProgressLock) {
                    inProgressLocks.remove(cacheKey);
                }
            }
        }
    }

    public static void invalidateCache(String cacheKey) {
        JedisPool pool = getJedisPool();
        if (pool == null) {
            return;
        }
        try (Jedis jedis = pool.getResource()) {
            synchronized (inProgressLock) {
                if (!inProgressLocks.containsKey(cacheKey)) {
                    inProgressLocks.put(cacheKey, new Object());
                }
            }
            synchronized (inProgressLocks.get(cacheKey)) {
                try {
                    jedis.del(getTrinoCacheString(cacheKey));
                } finally {
                    synchronized (inProgressLock) {
                        inProgressLocks.remove(cacheKey);
                    }
                }
            }
        }
    }
    private static Iterator<UlakRow> getCacheResult(boolean forceRefresh,
                                                        Jedis jedis,
                                                        String key) throws JsonProcessingException {
        List<UlakRow> resultAsList = getCacheResultAsList(forceRefresh, jedis, key);
        if (resultAsList != null) {
            return resultAsList.iterator();
        }
        return null;
    }

    public static List<UlakRow> getCacheResultAsList(boolean forceRefresh,
                                                         Jedis jedis,
                                                         String key) throws JsonProcessingException {
        if(jedis != null) {
            String json;
            json = jedis.get(getTrinoCacheString(key));
            if(json!=null){
//                addOneStat(hash, 1);
            }
            if (!forceRefresh && json != null) {
                QueryParameters queryParameters = getObjectMapper().readValue(json, QueryParameters.class);
                return queryParameters.getRows();
            }
        }
        return null;
    }


    public static List<ColumnMetadata> getColumnsBase(List<UlakRow> tables) {
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
            throw new io.trino.spi.TrinoException(io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "Empty Query");
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
