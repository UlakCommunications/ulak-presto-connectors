package com.facebook.presto.influxdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static com.facebook.presto.influxdb.InfluxdbUtil.*;


public class InfluxdbQueryParameters {
    private static Logger logger = LoggerFactory.getLogger(InfluxdbQueryParameters.class);

    public static final String NEW_LINE_CHAR = System.lineSeparator();
    public static final int DEFAULT_CACHE_TTL = 60 * 60 * 24;
    public static final int DEFAULT_TTL = 10;

    private String query;
    private int hash;
    private List<InfluxdbRow> rows;
    private boolean toBeCached=false;
    private long ttlInSeconds = DEFAULT_TTL;
    private long refreshDurationInSeconds = DEFAULT_TTL;
    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public int getHash() {
        return hash;
    }

    public void setHash(int hash) {
        this.hash = hash;
    }

    public List<InfluxdbRow> getRows() {
        return rows;
    }

    public void setRows(List<InfluxdbRow> rows) {
        this.rows = rows;
    }

    public InfluxdbQueryParameters() {

    }

    public InfluxdbQueryParameters(String query, int hash, List<InfluxdbRow> rows) {
        this.query = query;
        this.hash = hash;
        this.rows = rows;
    }

    public static InfluxdbQueryParameters getQueryParameters(String tableName){
        tableName = arrangeCase(tableName);
        int hash = tableName.hashCode();

        InfluxdbQueryParameters ret = new InfluxdbQueryParameters();
        ret.setQuery(tableName);
        ret.setHash(hash);

        String[] splits = tableName.split(NEW_LINE_CHAR);
        List<String> newLines = new ArrayList<>();
        for (int i = 0; i < splits.length; i++) {
            String current = splits[i];
            //get query parameters
            current = current.trim();
            while (!current.equals("") && current.length() > 0) {
                if (current.startsWith("/")) {
                    current = current.substring(1).trim();
                } else {
                    break;
                }
            }
            String[] params = current.split("=");
            if(params.length>1) {
                String param = params[0];
                String value = params[1];
                try {
                    switch (param.toLowerCase(Locale.ENGLISH)) {
                        case "ttl":
                            long v = Long.parseLong(value);
                            if (v > 0) {
                                ret.setTtlInSeconds(v);
                            }
                            break;
                        case "cache":
                            ret.setToBeCached(Boolean.parseBoolean(value));
                            if (ret.ttlInSeconds == DEFAULT_TTL) {
                                ret.ttlInSeconds = DEFAULT_CACHE_TTL;
                            }
                            break;
                        case "refreshin":
                            v = Long.parseLong(value);
                            if (v > 0) {
                                ret.setRefreshDurationInSeconds(v);
                            }
                            break;
                    }
                } catch (Throwable e) {
                    logger.error("getQueryParameters: " + param + "/" + value);
                }
            }
        }
//
//        String tblNoRange;
//        String[] splits = tableName.split(NEW_LINE_CHAR);
//        List<String> newLines = new ArrayList<>();
//        for (int i = 0; i < splits.length; i++) {
//            String current = splits[i];
//            if(!current.matches("[ ]*\\|\\>[ ]*range[ ]*\\(")){
//                newLines.add(current);
//            }
//        }
//        tblNoRange = String.join(NEW_LINE_CHAR,newLines);

        return ret;
    }

    public boolean isToBeCached() {
        return toBeCached;
    }

    public void setToBeCached(boolean toBeCached) {
        this.toBeCached = toBeCached;
    }

    public long getTtlInSeconds() {
        return ttlInSeconds;
    }

    public void setTtlInSeconds(long ttlInSeconds) {
        this.ttlInSeconds = ttlInSeconds;
    }

    public long getRefreshDurationInSeconds() {
        return refreshDurationInSeconds;
    }

    public void setRefreshDurationInSeconds(long refreshDurationInSeconds) {
        this.refreshDurationInSeconds = refreshDurationInSeconds;
    }
}
