package com.facebook.presto.influxdb;

import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.influxdb.InfluxdbUtil.*;


public class InfluxdbQueryParameters {
    public static final String NEW_LINE_CHAR = System.lineSeparator();
    String query;

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

    int hash;
    List<InfluxdbRow> rows;
    public InfluxdbQueryParameters() {

    }

    public InfluxdbQueryParameters(String query, int hash, List<InfluxdbRow> rows) {
        this.query = query;
        this.hash = hash;
        this.rows = rows;
    }
//
//    public static InfluxdbQueryParameters getQueryParameters(String tableName){
//        tableName = arrangeCase(tableName);
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
//        int hash = tblNoRange.hashCode();
//        Jedis jedis = getJedis();
//        String json = jedis.get(getTrinoCacheString(hash));
//        List<InfluxdbRow> list =null;
//
//        InfluxdbQueryParameters ret = new InfluxdbQueryParameters();
//        ret.setColumnsQuery(false);
//        ret.setAbsoluteRange(false);
//
//        ret.setFrom(-1);
//        ret.setTo(-1);
//
//        ret.setNoRangeQuery(tblNoRange);
//        ret.setNoRangeQueryHash(hash);
//
//        ret.setCompleteQuery(tableName);
//        ret.setWithRangeQueryHash(tableName.hashCode());
//        return ret;
//    }
}
