package com.facebook.presto.influxdb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static com.facebook.presto.influxdb.InfluxdbUtil.arrangeCase;


public class InfluxdbQueryParameters {
    public static final String TEXT_TTL = "ttl";
    public static final String TEXT_CACHE = "cache";
    public static final String TEXT_REFRESH = "refresh";
    public static final String TEXT_COLUMNS = "columns";
    public static final String TEXT_DBTYPE = "dbtype";
    public static final String TEXT_EAGER_CACHE = "eagercache";
    public static final String TEXT_NAME = "name";
    private static Logger logger = LoggerFactory.getLogger(InfluxdbQueryParameters.class);

    public static final String NEW_LINE_CHAR = System.lineSeparator();
    public static final int DEFAULT_CACHE_TTL = 60 * 60 * 24;
    public static final int DEFAULT_TTL = 10;
    private String[] columns;

    private String query;
    private int hash;
    private List<InfluxdbRow> rows;
    private boolean toBeCached = false;
    private boolean eagerCached = false;
    private long ttlInSeconds = DEFAULT_TTL;
    private long refreshDurationInSeconds = DEFAULT_TTL + 5;
    private long start;
    private long finish;
    private String error;

    public DBType getDbType() {
        return dbType;
    }

    public void setDbType(DBType dbType) {
        this.dbType = dbType;
    }

    DBType dbType = DBType.INFLUXDB2;


    String name = "";

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

//    public InfluxdbQueryParameters(String query, int hash, List<InfluxdbRow> rows, String[] columns, DBType dbType) {
//        this();
//        this.query = query;
//        this.hash = hash;
//        this.rows = rows;
//        this.columns = columns;
//        this.dbType = dbType;
//    }

    public static String getTableNameForHash(String tableName){
        String lastTableName = tableName;
        String beforeTableName = tableName;
        do{
            beforeTableName = lastTableName;
            lastTableName = lastTableName.replaceAll("(.*)(\\/\\/.*)","");
        }while (!lastTableName.equals(beforeTableName));

        String[] splits = lastTableName.split(NEW_LINE_CHAR);
        List<String> newLines = new ArrayList<>();
        for (int i = 0; i < splits.length; i++) {
            String current = splits[i].trim();
            if(current!=""){
                newLines.add(current);
            }
        }
        return String.join(NEW_LINE_CHAR,newLines).replaceAll("[ \r\n]","");
    }
    public static InfluxdbQueryParameters getQueryParameters(String tableName) {
        tableName = arrangeCase(tableName);
        String tableNameForHash = getTableNameForHash(tableName);

        int hash = tableNameForHash.hashCode();

        InfluxdbQueryParameters ret = new InfluxdbQueryParameters();
        ret.setQuery(tableName);
        ret.setHash(hash);

        String[] splits = tableName.split(NEW_LINE_CHAR);
        for (int i = 0; i < splits.length; i++) {
            String current = splits[i];
            //get query parameters
            current = current.trim();
            while (!current.equals("") && current.length() > 0) {
                if (current.startsWith("/") || current.startsWith("-")) {
                    current = current.substring(1).trim();
                } else {
                    break;
                }
            }
            String[] params = current.split("=");
            if (params.length > 1) {
                String param = params[0].trim();
                String value = params[1].trim();
                try {
                    switch (param.toLowerCase(Locale.ENGLISH)) {
                        case TEXT_TTL:
                            long v = Long.parseLong(value);
                            if (v > 0) {
                                ret.setTtlInSeconds(v);
                            }
                            break;
                        case TEXT_CACHE:
                            ret.setToBeCached(Boolean.parseBoolean(value));
                            if (ret.ttlInSeconds == DEFAULT_TTL) {
                                ret.ttlInSeconds = DEFAULT_CACHE_TTL;
                            }
                            break;
                        case TEXT_REFRESH:
                            v = Long.parseLong(value);
                            if (v > 0) {
                                ret.setRefreshDurationInSeconds(v);
                            }
                            break;
                        case TEXT_COLUMNS:
                            String[] vs = value.split(",");
                            ret.setColumns(vs);
                            break;
                        case TEXT_DBTYPE:
                            ret.setDbType(DBType.valueOf(value.toUpperCase(Locale.ENGLISH)));
                            break;
                        case TEXT_EAGER_CACHE:
                            ret.setEagerCached(Boolean.parseBoolean(value));
                            break;
                        case TEXT_NAME:
                            ret.setName(value);
                            break;
                    }
                } catch (Throwable e) {
                    logger.error("getQueryParameters: " + param + "/" + value);
                }
            }
        }

//        String tblNoRange = null;
//        String newLineChar = System.lineSeparator();
//        String[] splits = tableName.split(newLineChar);
//        List<String> newLines = new ArrayList<>();
//        for (int i = 0; i < splits.length; i++) {
//            String current = splits[i];
//            if(!current.matches("[ ]*\\|\\>[ ]*range[ ]*\\(")){
//                newLines.add(current);
//            }
//        }
//        tblNoRange = String.join(newLineChar,newLines);
//        int hash = tblNoRange.hashCode();

        return ret;
    }
    public void setColumns(String[] vs) {
        columns = vs;
    }
    public String[] getColumns( ) {
        return columns;
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

    public boolean isEagerCached() {
        return eagerCached;
    }

    public void setEagerCached(boolean eagerCached) {
        this.eagerCached = eagerCached;
    }


    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getFinish() {
        return finish;
    }

    public void setFinish(long finish) {
        this.finish = finish;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
