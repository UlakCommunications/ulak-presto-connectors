package com.facebook.presto.influxdb;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;

import java.time.Duration;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.facebook.presto.influxdb.InfluxdbUtil.*;

public class RedisCacheWorker extends Thread{

    private static Logger logger = LoggerFactory.getLogger(RedisCacheWorker.class);
    private static ExecutorService executor = null;

    public static final int N_THREADS = 10;

    static {
        executor = Executors.newFixedThreadPool(N_THREADS);
    }
    private static Map<Integer, CacheUsageStats> stats = new LinkedHashMap<>();
    private static Object statLock = new Object();
    private long lastAllPrint = System.currentTimeMillis();

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
                logger.error("Key does not exists to be printed: " + key);
                continue;
            }
            InfluxdbQueryParameters influxdbQueryParameters = null;
            try {
                influxdbQueryParameters = getObjectMapper().readValue(json, InfluxdbQueryParameters.class);
            } catch (JsonProcessingException e) {
                logger.error("JsonProcessingException" + key);
                continue;
            }

            sb.append("{\"key\":\"" + key
                    + "\",\"start\":"+ influxdbQueryParameters.getStart()
                    + ",\"startSeconds\":"+ ((System.currentTimeMillis() - influxdbQueryParameters.getStart())/1000)
                    +",\"stop\":"+ influxdbQueryParameters.getFinish()
                    +",\"stopSeconds\":"+ ((System.currentTimeMillis() - influxdbQueryParameters.getFinish())/1000)
                    +",\"duration\":"+ ((influxdbQueryParameters.getFinish() - influxdbQueryParameters.getStart())/1000)
                    +",\"ttl\":"+ influxdbQueryParameters.getTtlInSeconds()
                    +",\"refresh\":"+ influxdbQueryParameters.getRefreshDurationInSeconds()
                    +",\"eager\":"+ influxdbQueryParameters.isEagerCached() +"},");
        }
        sb.append("]");
        logger.info("Running stats: \n\t" + sb.toString());
    }
    @Override
    public void run() {
        Map<String,Future<?>> futures = new LinkedHashMap<>();
        while(true) {
            boolean sleepWorked = false;
            try {
                sleep(1);
                sleepWorked=true;
            } catch (InterruptedException e) {
                logger.error("JedisConnectionException", e);
            }
            //if not sleep worked then do not clutch
            if(sleepWorked) {
                //do not clutch if no redis
                JedisPool pool = null;
                try {
                    pool = getJedisPool();
                }catch (Throwable e){
                    logger.error("JedisConnectionException", e);
                }
                if (pool == null) {
                    continue;
                }
                long runningTaskCount = futures.values().stream().filter(t -> !t.isDone() && !t.isCancelled()).count();
                if (runningTaskCount < N_THREADS) {
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
                                        InfluxdbQueryParameters influxdbQueryParameters = null;
                                        try {
                                            String json = jedis.get(currentRedisKey);
                                            if (json == null) {
                                                logger.error("Key does not exists (ttl expired?): " + currentRedisKey);
                                            }
                                            influxdbQueryParameters = getObjectMapper().readValue(json,
                                                    InfluxdbQueryParameters.class);
                                            if(stats.equals((influxdbQueryParameters.getHash()))){
                                                logger.error("Hash does not match: " + currentRedisKey + "/"
                                                        + influxdbQueryParameters.getHash());
                                            }
                                        } catch (Throwable e) {
                                            logger.error("JsonProcessingException", e);
                                            continue;
                                        }

                                        CacheUsageStats usageStats = stats.get(influxdbQueryParameters.getHash());
                                        if(usageStats==null){
                                            //maybe we have restarted.
                                            usageStats = addOneStat(influxdbQueryParameters.getHash(),1);
                                            //removeCacheFromRefreshWorker(influxdbQueryParameters.getHash());
                                        }
                                        long diff = Duration.between(usageStats.getLastUsed().toInstant(),
                                                new Date().toInstant()).getSeconds();
                                        if (diff >= influxdbQueryParameters.getTtlInSeconds()) {
                                            removeCacheFromRefreshWorker(influxdbQueryParameters.getHash());
                                            continue;
                                        }

                                        long ttl = jedis.ttl(currentRedisKey);
                                        long passed = influxdbQueryParameters.getTtlInSeconds() - ttl;
                                        if (passed > influxdbQueryParameters.getRefreshDurationInSeconds()
                                                || (influxdbQueryParameters.getRows().size() == 0
                                                    && influxdbQueryParameters.isEagerCached())) {

                                            logger.debug("Seconds passed : " + passed + " for " + influxdbQueryParameters.getHash());

                                            futures.put(currentRedisKey, executor.submit(new RedisCacheWorkerItem(currentRedisKey, influxdbQueryParameters)));

                                            StringBuilder sb = new StringBuilder();
                                            for (Object key : futures.keySet()) {
                                                sb.append(key + ",");
                                            }
                                            logger.info("Running stats: \n\tRunning Count: " + futures.size()
                                                + "\n\tRunning keys: " + sb.toString()
                                                + "\n\t Current is eager: " + influxdbQueryParameters.isEagerCached());

                                            printUsage(jedis,futures.keySet().toArray());
                                            if(System.currentTimeMillis() - lastAllPrint>10000) {
                                                lastAllPrint = System.currentTimeMillis();
                                                printUsage(jedis, allKeys.toArray());
                                            }
                                        }
                                    } catch (JedisConnectionException e) {
                                        logger.error("JedisConnectionException", e);
                                    }
                                }
                            }
                        } catch (JedisConnectionException e) {
                            logger.error("JedisConnectionException", e);
                        }
                    } catch (JedisDataException e) {
                        logger.error("JedisConnectionException", e);
                    }
                }
//                synchronized (statLock){
//                    for (CacheUsageStats s:stats.values()) {
//                        Duration diff = Duration.between(s.lastUsed.toInstant(), new Date().toInstant());
//                        long diffDays = diff.toDays();
//                        if (diffDays>0) {
//                            logger.debug("Invalidating :" + s.hash);
//                            try {
//                                invalidateCache(s.hash);
//                            }catch (Throwable e){
//                                logger.error("Unable to invalidate cache: " + s.hash, e);
//                            }
//                        }
//                    }
//                }

            }
        }
    }

    private static void removeCacheFromRefreshWorker(int hash) {
        logger.info("Invalidating :" + hash);
        try {
            invalidateCache(hash);
        }catch (Throwable e){
            logger.error("Unable to invalidate cache: " + hash, e);
        }
    }
}
