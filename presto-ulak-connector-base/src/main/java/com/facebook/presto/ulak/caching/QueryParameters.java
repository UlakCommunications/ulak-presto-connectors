package com.facebook.presto.ulak.caching;

import com.facebook.presto.ulak.UlakRow;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;



public class QueryParameters {
    public static final String TEXT_TTL = "ttl";
    public static final String TEXT_CACHE = "cache";
    public static final String TEXT_REFRESH = "refresh";
//    public static final String TEXT_COLUMNS = "columns";
    public static final String TEXT_DBTYPE = "dbtype";
//    public static final String TEXT_EAGER_CACHE = "eagercache";
    public static final String TEXT_NAME = "name";
    public static final String TEXT_QWINDEX = "qwindex";
    public static final String TEXT_QWURL = "qwurl";
    public static final String TEXT_HASJS = "hasjs";
    public static final String TEXT_QWREPLACEFROMCOLUMN = "replacefromcolumns";
    private static Logger logger = LoggerFactory.getLogger(QueryParameters.class);

    public static final String NEW_LINE_CHAR = System.lineSeparator();
    public static final int DEFAULT_CACHE_TTL = 60 * 60 * 24;
    public static final int DEFAULT_TTL = 10;
    private String[] columns;

    private String query;
    private int hash;
    private List<UlakRow> rows;
    private boolean toBeCached = false;
//    private boolean eagerCached = false;
    private boolean hasJs = false;
    private long ttlInSeconds = DEFAULT_TTL;
    private long refreshDurationInSeconds = DEFAULT_TTL + 5;
    private long start;
    private long finish;
    private String error;
    private String qwUrl;
    private String qwIndex;
    private String replaceFromColumns;

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

    public List<UlakRow> getRows() {
        return rows;
    }

    public void setRows(List<UlakRow> rows) {
        this.rows = rows;
    }

    public QueryParameters() {

    }

//    public QueryParameters(String query, int hash, List<UlakRow> rows, String[] columns, DBType dbType) {
//        this();
//        this.query = query;
//        this.hash = hash;
//        this.rows = rows;
//        this.columns = columns;
//        this.dbType = dbType;
//    }
    public static String replaceAll(String tableName, String find, String replace){
        String lastTableName = tableName;
        String beforeTableName = tableName;
        do{
            beforeTableName = lastTableName;
            lastTableName = StringUtils.replace(lastTableName,find,replace);
        }while (!lastTableName.equals(beforeTableName));
        return lastTableName;
    }
    public static String getTableNameForHash(String tableName){
        String lastTableName = replaceAll(tableName,"(.*)(\\/\\/.*)","");


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
    public static QueryParameters getQueryParameters(String tableName) {
        tableName = ConnectorBaseUtil.arrangeCase(tableName);
        String tableNameForHash = getTableNameForHash(tableName);

        int hash = tableNameForHash.hashCode();

        QueryParameters ret = new QueryParameters();
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
//                        case TEXT_COLUMNS:
//                            String[] vs = value.split(",");
//                            ret.setColumns(vs);
//                            break;
                        case TEXT_DBTYPE:
                            ret.setDbType(DBType.valueOf(value.toUpperCase(Locale.ENGLISH)));
                            break;
//                        case TEXT_EAGER_CACHE:
//                            ret.setEagerCached(Boolean.parseBoolean(value));
//                            break;
                        case TEXT_HASJS:
                            ret.setHasJs(Boolean.parseBoolean(value));
                            break;
                        case TEXT_NAME:
                            ret.setName(value);
                            break;
                        case TEXT_QWURL:
                            ret.setQwUrl(value);
                            break;
                        case TEXT_QWINDEX:
                            ret.setQwIndex(value);
                            break;
                        case TEXT_QWREPLACEFROMCOLUMN:
                            ret.setReplaceFromColumns(value);
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
//    public void setColumns(String[] vs) {
//        columns = vs;
//    }
//    public String[] getColumns( ) {
//        return columns;
//    }
    public boolean isToBeCached() {
        return toBeCached;
    }

    public void setToBeCached(boolean toBeCached) {
        this.toBeCached = toBeCached;
    }
    public boolean isHasJs() {
        return hasJs;
    }

    public void setHasJs(boolean hasJs) {
        this.hasJs = hasJs;
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
//
//    public boolean isEagerCached() {
//        return eagerCached;
//    }
//
//    public void setEagerCached(boolean eagerCached) {
//        this.eagerCached = eagerCached;
//    }


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

    public void setQwUrl(String qwUrl) {
        this.qwUrl = qwUrl;
    }

    public String getQwUrl() {
        return qwUrl;
    }
    public void setQwIndex(String qwIndex) {
        this.qwIndex = qwIndex;
    }

    public String getQwIndex() {
        return qwIndex;
    }
    public void setReplaceFromColumns(String replaceFromColumns) {
        this.replaceFromColumns = replaceFromColumns;
    }

    public String getReplaceFromColumns() {
        return replaceFromColumns;
    }
}
