package com.facebook.presto.influxdb;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.facebook.presto.influxdb.InfluxdbUtil.*;

public class RedisCacheWorkerItem extends Thread {

    private static Logger logger = LoggerFactory.getLogger(RedisCacheWorkerItem.class);
    private final String key;

    public RedisCacheWorkerItem(String key) {
        this.key = key;
    }

    @Override
    public void run() {
        try (Jedis jedis = getJedisPool().getResource()) {
            try {
                try {
                    String json = jedis.get(key);
                    try {
                        InfluxdbQueryParameters influxdbQueryParameters = getObjectMapper().readValue(json, InfluxdbQueryParameters.class);
                        InfluxdbUtil.select(influxdbQueryParameters.getQuery(), true);
                    } catch (JsonProcessingException e) {
                        logger.error("JsonProcessingException", e);
                    } catch (IOException e) {
                        logger.error("IOException", e);
                    } catch (ClassNotFoundException e) {
                        logger.error("ClassNotFoundException", e);
                    }
                } catch (JedisConnectionException e) {
                    logger.error("JedisConnectionException", e);
                }
            } catch (JedisConnectionException e) {
                logger.error("JedisConnectionException", e);
            }
        } catch (JedisDataException e) {
            logger.error("JedisConnectionException", e);
        }
    }
}
