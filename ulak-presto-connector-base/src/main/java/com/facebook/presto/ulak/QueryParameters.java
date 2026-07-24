package com.facebook.presto.ulak;

import com.facebook.presto.ulak.caching.ConnectorBaseUtil;
import com.google.common.io.BaseEncoding;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;



public class QueryParameters {
    public static final String TEXT_TTL = "ttl";
    public static final String TEXT_CACHE = "cache";
    public static final String TEXT_REFRESH = "refresh";
    //TODO: eager caching is to be added
    public static final String TEXT_COLUMNS = "columns";
    public static final String TEXT_DBTYPE = "dbtype";
    //TODO: eager caching is to be added
    //public static final String TEXT_EAGER_CACHE = "eagercache";
    public static final String TEXT_NAME = "name";
    public static final String TEXT_QWINDEX = "qwindex";
    public static final String TEXT_HISTORY_ENABLED = "historyenabled";
    public static final String TEXT_HISTORY_INDEX = "historyindex";
    public static final String TEXT_NULL_FILL = "nullfill";
    public static final String TEXT_QWURL = "qwurl";
    public static final String TEXT_HASJS = "hasjs";
    public static final String TEXT_TIMEFIELD = "timefield";
    public static final String TEXT_QWREPLACEFROMCOLUMN = "replacefromcolumns";

    public static final String TEXT_QWCONNECTTIMEOUT = "connecttimeout";
    public static final String TEXT_QWREADTIMEOUT = "readtimeout";
    public static final String TEXT_QWWRITETIMEOUT = "writetimeout";

    public static final String TEXT_FROM = "from";
    public static final String TEXT_TO = "to";
    public static final String TEXT_SQL_VERSION = "sqlversion";
    private static final Logger logger = LoggerFactory.getLogger(QueryParameters.class);
    public static final String NEW_LINE_CHAR = System.lineSeparator();
    public static final int DEFAULT_CACHE_TTL = 60 * 60 * 24;
    public static final int DEFAULT_TTL = 10;
    //TODO: eager caching is to be added
    private String[] columns;
    private String sqlVersion = "0";

    private String query;
    private int hash;
    private String cacheKey;
    private List<UlakRow> rows;
    private boolean toBeCached = false;
    //TODO: eager caching is to be added
    //private boolean eagerCached = false;
    private boolean hasJs = false;
    private String timeField = null;
    private boolean nullFill = true;
    private int ttlInSeconds = DEFAULT_TTL;
    private int refreshDurationInSeconds = DEFAULT_TTL + 5;
    private long start;
    private long finish;
    private long from;
    private long to;
    private String error;
    private String qwUrl;
    private String qwIndex;
    private boolean historyEnabled = false;
    private String historyIndex;
    private String replaceFromColumns;
    private Integer connectTimeout;
    private Integer readTimeout;
    private Integer writeTimeout;

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

    public String getCacheKey() {
        return cacheKey != null ? cacheKey : String.valueOf(hash);
    }

    public void setCacheKey(String cacheKey) {
        this.cacheKey = cacheKey;
    }

    public static String sha256Hex(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            return String.valueOf(input.hashCode());
        }
    }

    public List<UlakRow> getRows() {
        return rows;
    }

    public void setRows(List<UlakRow> rows) {
        this.rows = rows;
    }

    public static String replaceAll(String tableName, String find, String replace){
        String lastTableName = tableName;
        String beforeTableName = null;
        do{
            beforeTableName = lastTableName;
            lastTableName = StringUtils.replace(lastTableName,find,replace);
        }while (!lastTableName.equals(beforeTableName));
        return lastTableName;
    }
    public static String getTableNameForHash(String tableName){
        String lastTableName = tableName;


        String[] splits = lastTableName.split(NEW_LINE_CHAR);
        List<String> newLines = new ArrayList<>();
        for (int i = 0; i < splits.length; i++) {
            String current = splits[i].trim();
            if(!current.isEmpty() && !current.startsWith("//") && !current.startsWith("-")){
                newLines.add(current);
            }
        }
        return String.join(NEW_LINE_CHAR,newLines).replaceAll("[ \r\n]","");
    }

    public static String stringTrimmer (String current) {
        current = current.trim();
        while (!current.isEmpty()) {
            if (current.startsWith("/") || current.startsWith("-")) {
                current = current.substring(1).trim();
            } else {
                break;
            }
        }
        return current;
    }
    public static QueryParameters getQueryParameters(String tableName) {
        if (!tableName.contains("//") && !tableName.contains("\n") && !tableName.contains(" ")
                && tableName.length() % 8 == 0
                && BaseEncoding.base32().canDecode(tableName.toUpperCase())) {
            logger.debug("Encoded: {}\n", tableName);
            byte[] decodedBytes = BaseEncoding.base32().decode(tableName.toUpperCase());
            tableName = (new String(decodedBytes, StandardCharsets.UTF_8));
            logger.debug("Decoded: {}\n", tableName);
        }

        tableName = ConnectorBaseUtil.arrangeCase(tableName);
        String tableNameForHash = getTableNameForHash(tableName);

        int hash = tableNameForHash.hashCode();

        QueryParameters ret = new QueryParameters();
        ret.setQuery(tableName);
        ret.setHash(hash);
        ret.setCacheKey(sha256Hex(tableNameForHash));

        String[] splits = tableName.split(NEW_LINE_CHAR);
        for (int i = 0; i < splits.length; i++) {
            //get query parameters
            String current = stringTrimmer(splits[i]);
            String[] params = current.split("=", 2);
            if (params.length > 1) {
                String param = params[0].trim();
                String value = params[1].trim();
                int v;
                long l;
                try {
                    switch (param.toLowerCase(Locale.ENGLISH)) {
                        case TEXT_TTL:
                            v = Integer.parseInt(value);
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
                            v = Integer.parseInt(value);
                            if (v > 0) {
                                ret.setRefreshDurationInSeconds(v);
                            }
                            break;
                        case TEXT_FROM:
                            l = Long.parseLong(value);
                            if (l > 0) {
                                ret.setFrom(l);
                            }
                            break;
                        case TEXT_TO:
                            l = Long.parseLong(value);
                            if (l > 0) {
                                ret.setTo(l);
                            }
                            break;
                        case TEXT_COLUMNS:
                            String normalizedValue = value.replaceAll("(?<=value)(?=[0-9])", ",")
                                                          .replaceAll("(?<=key)(?=[a-zA-Z])", ",");
                            String[] vs = normalizedValue.split(",");
                            ret.setColumns(vs);
                            break;
                        case TEXT_DBTYPE:
                            ret.setDbType(DBType.valueOf(value.toUpperCase(Locale.ENGLISH)));
                            break;
                        //TODO: eager caching is to be added
                        //case TEXT_EAGER_CACHE:
                            //ret.setEagerCached(Boolean.parseBoolean(value));
                            //break;
                        case TEXT_HASJS:
                            ret.setHasJs(Boolean.parseBoolean(value));
                            break;
                        case TEXT_TIMEFIELD:
                            ret.setTimeField(value);
                            break;
                        case TEXT_NULL_FILL:
                            ret.setNullFill(Boolean.parseBoolean(value));
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
                        case TEXT_HISTORY_ENABLED:
                            ret.setHistoryEnabled(Boolean.parseBoolean(value));
                            break;
                        case TEXT_HISTORY_INDEX:
                            ret.setHistoryIndex(value);
                            break;
                        case TEXT_QWREPLACEFROMCOLUMN:
                            ret.setReplaceFromColumns(value);
                            break;
                        case TEXT_QWREADTIMEOUT:
                            ret.setReadTimeout(Integer.valueOf(value));
                            break;
                        case TEXT_QWWRITETIMEOUT:
                            ret.setWriteTimeout(Integer.valueOf(value));
                            break;
                        case TEXT_QWCONNECTTIMEOUT:
                            ret.setConnectTimeout(Integer.valueOf(value));
                            break;
                        case TEXT_SQL_VERSION:
                            ret.setSqlVersion(value);
                            break;
                    }
                } catch (Exception e) {
                    logger.error("getQueryParameters failed for param={} value={}", param, redactIfSecret(param, value), e);
                }
            }
        }

        StringBuilder hashBuilder = new StringBuilder(tableNameForHash);
        hashBuilder.append("|dbtype=").append(ret.getDbType());
        hashBuilder.append("|historyenabled=").append(ret.isHistoryEnabled());
        if (ret.getHistoryIndex() != null) {
            hashBuilder.append("|historyindex=").append(ret.getHistoryIndex());
        }
        if (ret.getQwIndex() != null) {
            hashBuilder.append("|qwindex=").append(ret.getQwIndex());
        }
        hashBuilder.append("|from=").append(ret.getFrom());
        hashBuilder.append("|to=").append(ret.getTo());
        if (ret.getSqlVersion() != null) {
            hashBuilder.append("|sqlversion=").append(ret.getSqlVersion());
        }
        if (ret.getColumns() != null) {
            hashBuilder.append("|columns=").append(String.join(",", ret.getColumns()));
        }
        hashBuilder.append("|hasjs=").append(ret.getHasJs());
        if (ret.getReplaceFromColumns() != null) {
            hashBuilder.append("|replacefromcolumns=").append(ret.getReplaceFromColumns());
        }

        String finalHashStr = hashBuilder.toString();
        ret.setHash(finalHashStr.hashCode());
        ret.setCacheKey(sha256Hex(finalHashStr));

        return ret;
    }
    public void setColumns(String[] vs) {
        columns = vs;
    }
    public String[] getColumns( ) {
        return columns;
    }
    public String getSqlVersion() {
        return sqlVersion;
    }
    public void setSqlVersion(String sqlVersion) {
        this.sqlVersion = sqlVersion;
    }
    public boolean isToBeCached() {
        return toBeCached;
    }

    public void setToBeCached(boolean toBeCached) {
        this.toBeCached = toBeCached;
    }
    public boolean getHasJs() {
        return hasJs;
    }

    public void setHasJs(boolean hasJs) {
        this.hasJs = hasJs;
    }
    public String getTimeField() {
        return timeField;
    }

    public void setTimeField(String timeField) {
        this.timeField = timeField;
    }
    public boolean getNullFill() {
        return nullFill;
    }

    public void setNullFill(boolean nullFill) {
        this.nullFill = nullFill;
    }
    public long getTtlInSeconds() {
        return ttlInSeconds;
    }

    public void setTtlInSeconds(int ttlInSeconds) {
        this.ttlInSeconds = ttlInSeconds;
    }

    public long getRefreshDurationInSeconds() {
        return refreshDurationInSeconds;
    }

    public void setRefreshDurationInSeconds(int refreshDurationInSeconds) {
        this.refreshDurationInSeconds = refreshDurationInSeconds;
    }
    //TODO: eager caching is to be added
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

    public long getFrom() {
        return from;
    }
//    public Date getFromAsDate() {
//        return getTsAsDate(from);
//    }

    public void setFrom(long from) {
        this.from = from;
    }
//    public static Date getTsAsDate(long ts){
//        Timestamp stamp = new Timestamp(ts);
//        Date date = new Date(stamp.getTime());
//        return date;
//
//    }
    public long getTo() {
        return to;
    }
//    public Date getToAsDate() {
//        return getTsAsDate(to);
//    }
    public void setTo(long to) {
        this.to = to;
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

    public boolean isHistoryEnabled() {
        return historyEnabled;
    }

    public void setHistoryEnabled(boolean historyEnabled) {
        this.historyEnabled = historyEnabled;
    }

    public String getHistoryIndex() {
        return historyIndex;
    }

    public void setHistoryIndex(String historyIndex) {
        this.historyIndex = historyIndex;
    }

    public void setReplaceFromColumns(String replaceFromColumns) {
        this.replaceFromColumns = replaceFromColumns;
    }

    public String getReplaceFromColumns() {
        return replaceFromColumns;
    }
    private static final Pattern SECRET_NAME = Pattern.compile(
            "(?i).*(pass|pwd|secret|token|key|credential|url).*");

    static String redactIfSecret(String name, String value) {
        if (name != null && SECRET_NAME.matcher(name).matches() && value != null && !value.isEmpty()) {
            return "***REDACTED***";
        }
        return value;
    }

    public static String replaceEnv(String source, String env, boolean encode){
        if(StringUtils.isNotBlank(source)){
            //${ENV:RO_POSTGRES_PASSWORD}
            //source = source.replace("${ENV:","").replace("}","");
            String resEnv = System.getenv(env);
            // Never log resEnv directly — for *_PASSWORD / *_TOKEN env vars it
            // is a credential. Log only whether the env was set.
            if (logger.isDebugEnabled()) {
                logger.debug("replaceEnv env={} present={}", env, StringUtils.isNotBlank(resEnv));
            }

            if(StringUtils.isNotBlank(resEnv)) {
                source = source.replace("${ENV:" + env + "}", encode ? encodeUriComponent(resEnv):resEnv);
            }
        }
        return source;
    }
    public static String encodeUriComponent(String s) {
        // Do not log `s`: this method is called with secret env values
        // (passwords, tokens) and earlier versions logged them in plaintext.
        if(s == null){
            return "";
        }
        StringBuilder out = new StringBuilder();
        for (byte b : s.getBytes(StandardCharsets.UTF_8)) {
            char c = (char) b;
            if (
                    (c >= 'a' && c <= 'z') ||
                            (c >= 'A' && c <= 'Z') ||
                            (c >= '0' && c <= '9') ||
                            c == '-' || c == '_' || c == '.' || c == '~'
            ) {
                out.append(c);
            } else {
                out.append(String.format("%%%02X", b));
            }
        }

        return out.toString();
    }

    public Integer getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(Integer connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public Integer getReadTimeout() {
        return readTimeout;
    }

    public void setReadTimeout(Integer readTimeout) {
        this.readTimeout = readTimeout;
    }

    public Integer getWriteTimeout() {
        return writeTimeout;
    }

    public void setWriteTimeout(Integer writeTimeout) {
        this.writeTimeout = writeTimeout;
    }
}
