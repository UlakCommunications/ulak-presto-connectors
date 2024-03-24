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

    public static final int N_THREADS = 5;

    static {
        executor = Executors.newFixedThreadPool(N_THREADS);
    }
    private static Map<Integer, CacheUsageStats> stats = new LinkedHashMap<>();
    private static Object statLock = new Object();
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
                JedisPool pool = getJedisPool();
                if(pool == null){
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
                            for (String s : allKeys) {
                                Future<?> future = futures.get(s);
                                boolean isKeyIsRunning = future == null
                                        ? false
                                        : !future.isDone() && !future.isCancelled();
                                if (!isKeyIsRunning) {
                                    try {
                                        InfluxdbQueryParameters influxdbQueryParameters = null;
                                        try {
                                            String json = jedis.get(s);
                                            if (json == null) {
                                                logger.error("Key does not exists (ttl expired?): " + s);
                                            }
                                            influxdbQueryParameters = getObjectMapper().readValue(json,
                                                    InfluxdbQueryParameters.class);
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

                                        long ttl = jedis.ttl(s);
                                        long passed = influxdbQueryParameters.getTtlInSeconds() - ttl;
                                        if (passed > influxdbQueryParameters.getRefreshDurationInSeconds()) {
                                            logger.debug("Passed :" + passed);
                                            futures.put(s, executor.submit(new RedisCacheWorkerItem(s, influxdbQueryParameters)));
                                        }
//                                        }
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
