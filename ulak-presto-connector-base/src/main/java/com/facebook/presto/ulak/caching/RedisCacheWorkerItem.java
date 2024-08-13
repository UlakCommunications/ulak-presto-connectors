package com.facebook.presto.ulak.caching;

import com.facebook.presto.ulak.QueryParameters;
import com.facebook.presto.ulak.UlakRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.facebook.presto.ulak.caching.ConnectorBaseUtil.getJedisPool;
import static com.facebook.presto.ulak.caching.ConnectorBaseUtil.getObjectMapper;


public class RedisCacheWorkerItem extends Thread implements Supplier<String> {

    private static Logger logger = LoggerFactory.getLogger(RedisCacheWorkerItem.class);
    private final String key;
    private final Function<QueryParameters, List<UlakRow>> exec1;

    public RedisCacheWorkerItem(String key, Function<QueryParameters, List<UlakRow>> exec1) {
        this.key = key;
        this.exec1 = exec1;
    }

    @Override
    public void run() {

        QueryParameters influxdbQueryParameters = null;
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
                    influxdbQueryParameters = getObjectMapper().readValue(json, QueryParameters.class);

                    ConnectorBaseUtil.select(influxdbQueryParameters,true, exec1);
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
        start();
        return "finished";
    }
}
