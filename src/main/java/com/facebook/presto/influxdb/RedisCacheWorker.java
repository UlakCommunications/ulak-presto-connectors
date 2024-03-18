package com.facebook.presto.influxdb;

import redis.clients.jedis.Jedis;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.influxdb.InfluxdbUtil.*;

public class RedisCacheWorker extends Thread{
    private static ExecutorService executor = null;
    static {
        executor = Executors.newFixedThreadPool(5);
    }

    @Override
    public void run() {
        while(true) {
            Jedis jedis = getJedis();
            Set<String> allKeys = jedis.keys(getTrinoCacheString("*"));
            for (String s : allKeys){
                long ttl = jedis.ttl(s);
                if(TTL - ttl>10){
                    executor.submit(()->{

                    });
                }
            }
        }
    }
}
