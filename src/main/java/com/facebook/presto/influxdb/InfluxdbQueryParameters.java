package com.facebook.presto.influxdb;

import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.influxdb.InfluxdbUtil.*;


public class InfluxdbQueryParameters {
    public static final String NEW_LINE_CHAR = System.lineSeparator();
    long from;
    long to;

    boolean isAbsoluteRange;

    boolean isColumnsQuery;

    String completeQuery;
    String noRangeQuery;

    int noRangeQueryHash;

    int withRangeQueryHash;

    public InfluxdbQueryParameters() {

    }

    public long getTo() {
        return to;
    }

    public long getFrom() {
        return from;
    }

    public String getCompleteQuery() {
        return completeQuery;
    }

    public int getWithRangeQueryHash() {
        return withRangeQueryHash;
    }

    public int getNoRangeQueryHash() {
        return noRangeQueryHash;
    }

    public String getNoRangeQuery() {
        return noRangeQuery;
    }

    public void setFrom(long from) {
        this.from = from;
    }

    public void setTo(long to) {
        this.to = to;
    }

    public boolean isAbsoluteRange() {
        return isAbsoluteRange;
    }

    public void setAbsoluteRange(boolean absoluteRange) {
        isAbsoluteRange = absoluteRange;
    }

    public boolean isColumnsQuery() {
        return isColumnsQuery;
    }

    public void setColumnsQuery(boolean columnsQuery) {
        isColumnsQuery = columnsQuery;
    }

    public void setCompleteQuery(String completeQuery) {
        this.completeQuery = completeQuery;
    }

    public void setNoRangeQuery(String noRangeQuery) {
        this.noRangeQuery = noRangeQuery;
    }

    public void setNoRangeQueryHash(int noRangeQueryHash) {
        this.noRangeQueryHash = noRangeQueryHash;
    }

    public void setWithRangeQueryHash(int withRangeQueryHash) {
        this.withRangeQueryHash = withRangeQueryHash;
    }

    public InfluxdbQueryParameters(long from,
                                   long to,
                                   boolean isAbsoluteRange,
                                   boolean isColumnsQuery,
                                   String completeQuery,
                                   String noRangeQuery,
                                   int noRangeQueryHash,
                                   int withRangeQueryHash) {
        this.from = from;
        this.to = to;
        this.isAbsoluteRange = isAbsoluteRange;
        this.isColumnsQuery = isColumnsQuery;
        this.completeQuery = completeQuery;
        this.noRangeQuery = noRangeQuery;
        this.noRangeQueryHash = noRangeQueryHash;
        this.withRangeQueryHash = withRangeQueryHash;
    }

    public static InfluxdbQueryParameters getQueryParameters(String tableName){
        tableName = arrangeCase(tableName);
        String tblNoRange;
        String[] splits = tableName.split(NEW_LINE_CHAR);
        List<String> newLines = new ArrayList<>();
        for (int i = 0; i < splits.length; i++) {
            String current = splits[i];
            if(!current.matches("[ ]*\\|\\>[ ]*range[ ]*\\(")){
                newLines.add(current);
            }
        }
        tblNoRange = String.join(NEW_LINE_CHAR,newLines);
        int hash = tblNoRange.hashCode();
        Jedis jedis = getJedis();
        String json = jedis.get(getTrinoCacheString(hash));
        List<InfluxdbRow> list =null;

        InfluxdbQueryParameters ret = new InfluxdbQueryParameters();
        ret.setColumnsQuery(false);
        ret.setAbsoluteRange(false);

        ret.setFrom(-1);
        ret.setTo(-1);

        ret.setNoRangeQuery(tblNoRange);
        ret.setNoRangeQueryHash(hash);

        ret.setCompleteQuery(tableName);
        ret.setWithRangeQueryHash(tableName.hashCode());
        return ret;
    }
}
