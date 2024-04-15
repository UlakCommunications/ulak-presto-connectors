package com.facebook.presto.influxdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.function.Supplier;

import static com.facebook.presto.influxdb.InfluxdbUtil.*;

public class RedisCacheWorkerItem extends Thread implements Supplier<String> {

    private static Logger logger = LoggerFactory.getLogger(RedisCacheWorkerItem.class);
    private final String key;
    public RedisCacheWorkerItem(String key) {
        this.key = key;
    }

    @Override
    public void run() {

        InfluxdbQueryParameters influxdbQueryParameters = null;
        //do not clutch if no redis
        JedisPool pool = null;

        try {
            pool = getJedisPool();

            try (Jedis jedis = pool.getResource()) {
                try {
                    String json = jedis.get(key);
                    if (json == null) {
                        logger.error("Key does not exists (ttl expired?): {}", key);
                    }
                    influxdbQueryParameters = getObjectMapper().readValue(json,
                            InfluxdbQueryParameters.class);

                    InfluxdbUtil.select(influxdbQueryParameters, true);
                } catch (Throwable e) {
                    logger.error("Query Execution Error: {}/{}", this.key, influxdbQueryParameters != null ? influxdbQueryParameters.getName() : "", e);
                }
            }
        } catch (Throwable e) {
            logger.error("Query Execution Error: {}/{}", this.key, influxdbQueryParameters != null ? influxdbQueryParameters.getName() : "", e);
        }
    }

    @Override
    public String get() {
        run();
        return "finished";
    }
}
