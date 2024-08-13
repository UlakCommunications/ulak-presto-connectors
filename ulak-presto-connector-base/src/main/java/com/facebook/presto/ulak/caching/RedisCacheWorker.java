package com.facebook.presto.ulak.caching;

import com.facebook.presto.ulak.DBType;
import com.facebook.presto.ulak.QueryParameters;
import com.facebook.presto.ulak.UlakRow;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.facebook.presto.ulak.caching.ConnectorBaseUtil.*;

public class RedisCacheWorker extends Thread{
    private static Logger logger = LoggerFactory.getLogger(RedisCacheWorker.class);

    public static final int DEFAULT_N_THREADS = 10;
    private static final Map<Integer, CacheUsageStats> stats = new LinkedHashMap<>();
    private static final Object statLock = new Object();
    private long lastAllPrint = System.currentTimeMillis();
    private int numThreads = DEFAULT_N_THREADS;
    private final Function<QueryParameters, List<UlakRow>> exec1;
    private final DBType dbType;
    private ExecutorService executor = null;

    public RedisCacheWorker(Function<QueryParameters, List<UlakRow>> exec1,
                            int numThreads, DBType dbType) {
        this.exec1 = exec1;
        this.dbType = dbType;
        setNumThreads(numThreads);
    }

    public void setNumThreads(int numThreads){
        this.numThreads = numThreads;
        this.executor = Executors.newFixedThreadPool(numThreads);
    }
    public int getNumThreads(){
        return this.numThreads ;
    }
    public static CacheUsageStats addOneStat(int hash, int used){
        synchronized (statLock){
            CacheUsageStats ret = null;
            if(!stats.containsKey(hash)){
                ret = new CacheUsageStats(used,new Date(),hash);
                stats.put(hash, ret);
            }else{
                ret = stats.get(hash);
                if(ret.getUsed() == Integer.MAX_VALUE){
                    ret.setUsed(0) ;
                }
                ret.setUsed(ret.getUsed());
                ret.setLastUsed(new Date());
                stats.replace(hash, ret);
            }
            return ret;
        }
    }
    private static void printUsage(Jedis jedis, Object[] array)  {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (Object key : array) {
            String json = jedis.get((String) key);
            if (json == null) {
                logger.error("Key does not exists to be printed: {}", key);
                continue;
            }
            try {
                QueryParameters queryParameters = getObjectMapper().readValue(json, QueryParameters.class);
                sb.append("{\"key\":\"").append(key).append("\",\"name\":\"").
                        append(queryParameters.getName()).append("\",\"start\":").
                        append(queryParameters.getStart()).append(",\"startSeconds\":").
                        append((System.currentTimeMillis() - queryParameters.getStart()) / 1000).
                        append(",\"stop\":").append(queryParameters.getFinish()).
                        append(",\"stopSeconds\":").append((System.currentTimeMillis() - queryParameters.getFinish()) / 1000).
                        append(",\"duration\":").append((queryParameters.getFinish() - queryParameters.getStart()) / 1000).
                        append(",\"ttl\":").append(queryParameters.getTtlInSeconds()).append(",\"refresh\":").
                        append(queryParameters.getRefreshDurationInSeconds());
//                      +",\"eager\":"+ queryParameters.isEagerCached() +"},");
            } catch (JsonProcessingException e) {
                logger.error("JsonProcessingException{}", key);
            }
        }
        sb.append("]");
        logger.debug("Running stats: \n\t{}", sb);
    }
    @Override
    public void run() {
        String jedisConnExceptionString = "JedisConnectionException";
        Map<String,CompletableFuture<?>> futures = new LinkedHashMap<>();
        while(true) {
            boolean sleepWorked = false;
            try {
                sleep(1);
                sleepWorked=true;
            } catch (InterruptedException e) {
                logger.error("JedisInterruptedException", e);
            }
            //if not sleep worked then do not clutch
            if(sleepWorked) {
                //do not clutch if no redis
                JedisPool pool = null;
                try {
                    pool = getJedisPool();
                }catch (Exception e){
                    logger.error("ThrowableException", e);
                }
                if (pool == null) {
                    continue;
                }
                long runningTaskCount = futures.values().stream().filter(t -> !t.isDone() && !t.isCancelled()).count();
                if (runningTaskCount < numThreads) {
                    //first clean finished
                    for (Object key : futures.keySet().toArray()) {
                        Future<?> future = futures.get(key);
                        if (future.isDone() || future.isCancelled()) {
                            futures.remove(key);
                        }
                    }

                    try (Jedis jedis = pool.getResource()) {
                        try {
                            Set<String> allKeys = jedis.keys(getTrinoCacheString("*"));
                            for (String currentRedisKey : allKeys) {
                                Future<?> future = futures.get(currentRedisKey);
                                boolean isKeyIsRunning = future == null
                                        ? false
                                        : !(future.isDone() || future.isCancelled());
                                if (!isKeyIsRunning) {
                                    try {
                                        QueryParameters queryParameters = null;
                                        try {
                                            String json = jedis.get(currentRedisKey);
                                            if (json == null) {
                                                logger.error("Key does not exists (ttl expired?): {}", currentRedisKey);
                                            }
                                            queryParameters = getObjectMapper().readValue(json,
                                                    QueryParameters.class);
//                                            if(stats.equals((queryParameters.getHash()))){
//                                                logger.error("Hash does not match: " + currentRedisKey + "/"
//                                                        + queryParameters.getHash());
//                                            }
                                        } catch (JsonProcessingException e) {
                                            logger.error("JsonProcessingException", e);
                                            continue;
                                        }
                                        if(queryParameters.getDbType()!=this.dbType){
                                            logger.info("not the same db type {}/{}",queryParameters.getDbType(), this.dbType);
                                            continue;
                                        }

                                        if(!queryParameters.isToBeCached()){
                                            logger.debug("not the to be cached {}",queryParameters.getHash());
                                            continue;
                                        }
                                        CacheUsageStats usageStats = stats.get(queryParameters.getHash());
                                        if(usageStats==null){
                                            //maybe we have restarted.
                                            usageStats = addOneStat(queryParameters.getHash(),1);
                                            //removeCacheFromRefreshWorker(influxdbQueryParameters.getHash());
                                        }

                                        long ttl = jedis.ttl(currentRedisKey);
                                        long passed = queryParameters.getTtlInSeconds() - ttl;
                                        if (passed >= queryParameters.getRefreshDurationInSeconds() ) {
                                                    //TODO: eager cache to be added
                                                    //|| (queryParameters.getRows().size() == 0)
                                                    // && queryParameters.isEagerCached()

                                            logger.debug("Seconds passed : {} for {}", passed,
                                                    queryParameters.getHash());

                                            futures.put(currentRedisKey,
                                                    CompletableFuture.supplyAsync(
                                                        new RedisCacheWorkerItem(currentRedisKey,this.exec1 ),
                                                            executor));

                                            StringBuilder sb = new StringBuilder();
                                            for (Object key : futures.keySet()) {
                                                sb.append(key + ",");
                                            }
                                            logger.debug("Running stats: \n\tRunning Count: {}\n\tRunning keys: {}",
                                                    futures.size(), sb);

                                            printUsage(jedis,futures.keySet().toArray());
                                            if(System.currentTimeMillis() - lastAllPrint>10000) {
                                                lastAllPrint = System.currentTimeMillis();
                                                printUsage(jedis, allKeys.toArray());
                                            }
                                        }
                                    } catch (JedisConnectionException e) {
                                        logger.error(jedisConnExceptionString, e);
                                    }
                                }
                            }
                        } catch (JedisConnectionException e) {
                            logger.error(jedisConnExceptionString, e);
                        }
                    } catch (JedisDataException e) {
                        logger.error(jedisConnExceptionString, e);
                    }
                }
            }
        }
    }
}
