package com.facebook.presto.influxdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
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

    public static final int WAITING_TIME_FOR_TTL = 10;
    private static Logger logger = LoggerFactory.getLogger(RedisCacheWorker.class);
    private static ExecutorService executor = null;

    public static final int N_THREADS = 5;

    static {
        executor = Executors.newFixedThreadPool(N_THREADS);
    }
    private static Map<Integer, CacheUsageStats> stats = new LinkedHashMap<>();
    private static Object statLock = new Object();
    public static void addOneStat(int hash, int used){
        synchronized (statLock){
            if(!stats.containsKey(hash)){
                stats.put(hash, new CacheUsageStats(used,new Date(),hash));
            }else{
                CacheUsageStats c = stats.get(hash);
                if(c.used == Integer.MAX_VALUE){
                    c.used = 0;
                }
                c.used+=used;
                c.lastUsed=new Date();
                stats.replace(hash, c);
            }
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
                long runningTaskCount = futures.values().stream().filter(t -> !t.isDone() && !t.isCancelled()).count();
                if (runningTaskCount < N_THREADS) {
                    //first clean finished
                    for (Object key : futures.keySet().toArray()) {
                        Future<?> future = futures.get(key);
                        if (future.isDone() || future.isCancelled()) {
                            futures.remove(key);
                        }
                    }
                    try (Jedis jedis = getJedisPool().getResource()) {
                        try {
                            Set<String> allKeys = jedis.keys(getTrinoCacheString("*"));
                            for (String s : allKeys) {
                                Future<?> future = futures.get(s);
                                boolean isKeyIsRunning = future == null
                                        ? false
                                        : !future.isDone() && !future.isCancelled();
                                if (!isKeyIsRunning) {
                                    try {
                                        long ttl = jedis.ttl(s);
                                        long passed = TTL - ttl;
                                        if (passed > WAITING_TIME_FOR_TTL) {

                                            logger.debug("Passed :" + passed);
                                            futures.put(s, executor.submit(new RedisCacheWorkerItem(s)));
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
                synchronized (statLock){
                    for (CacheUsageStats s:stats.values()) {
                        Duration diff = Duration.between(s.lastUsed.toInstant(), new Date().toInstant());
                        long diffDays = diff.toDays();
                        if (diffDays>0) {
                            logger.debug("Invalidating :" + s.hash);
                            try {
                                invalidateCache(s.hash);
                            }catch (Throwable e){
                                logger.error("Unable to invalidate cache: " + s.hash, e);
                            }
                        }
                    }
                }

            }
        }
    }
}
