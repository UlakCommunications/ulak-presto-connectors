package com.facebook.presto.influxdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;

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

    @Override
    public void run() {
        Map<String,Future<?>> futures = new LinkedHashMap<>();
        while(true) {
            try {
                sleep(1);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            long runningTaskCount = futures.values().stream().filter(t -> !t.isDone() && !t.isCancelled()).count();
            if(runningTaskCount <N_THREADS) {
                //first clean finished
                for (Object key: futures.keySet().toArray()){
                    Future<?> future = futures.get(key);
                    if(future.isDone() || future.isCancelled()){
                        futures.remove(key);
                    }
                }
                try (Jedis jedis = getJedisPool().getResource()) {
                    try {
                        Set<String> allKeys = jedis.keys(getTrinoCacheString("*"));
                        for (String s : allKeys) {
                            Future<?> future = futures.get(s);
                            boolean isKeyIsRunning =future == null
                                        ? false
                                        : !future.isDone() && !future.isCancelled();
                            if(!isKeyIsRunning) {
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
                }catch (JedisDataException e){
                    logger.error("JedisConnectionException", e);
                }
            }
        }
    }
}
