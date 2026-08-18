/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.quickwit;

import com.facebook.presto.ulak.DBType;
import com.facebook.presto.ulak.QueryParameters;
import com.facebook.presto.ulak.UlakRow;
import com.facebook.presto.ulak.caching.ConnectorBaseUtil;
import com.github.opendevl.JFlat;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import com.quickwit.javaclient.ApiClient;
import com.quickwit.javaclient.ApiException;
import com.quickwit.javaclient.ApiResponse;
import com.quickwit.javaclient.Configuration;
import com.quickwit.javaclient.api.IndexesApi;
import com.quickwit.javaclient.api.SearchApi;
import com.quickwit.javaclient.models.DocMapping;
import com.quickwit.javaclient.models.FieldMappingEntryForSerialization;
import com.quickwit.javaclient.models.SearchRequestQueryString;
import com.quickwit.javaclient.models.SearchResponseRest;
import com.quickwit.javaclient.models.VersionedIndexMetadata;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import okhttp3.Call;
import okhttp3.Response;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.facebook.presto.ulak.QueryParameters.replaceAll;

public class QwUtil {

    private static final Logger logger = LoggerFactory.getLogger(QwUtil.class);

    private static final Map<String, ApiClient> defaultClients = new ConcurrentHashMap<>();

    public static final String DOC_COUNT = "doc_count";
    public static final String KEY = "key";
    public static final String KEY_AS_STRING = "key_as_string";
    public static final String SUM_OTHER_DOC_COUNT = "sum_other_doc_count";
    public static final String VALUE = "value";
    public static final String BUCKETS = "buckets";
    private static final long DEFAULT_HISTORY_TIME_THRESHOLD_SECONDS = 10800L; // 3 hours

    public static ApiClient getDefaultClient(QueryParameters queryParameters,
                                             Integer connectTimeout,
                                             Integer readTimeout,
                                             Integer writeTimeout) {
        String qwUrl = queryParameters != null ? queryParameters.getQwUrl() : null;
        if (StringUtils.isBlank(qwUrl)) {
            if (queryParameters != null) {
                logger.error("url is null : {}\n\n\nurl:{}\n\n\nindex:{}",
                        queryParameters.getQuery(),
                        queryParameters.getQwUrl(),
                        queryParameters.getQwIndex());
            } else {
                logger.error("getDefaultClient called with null queryParameters");
            }
            return null;
        }
        return defaultClients.computeIfAbsent(qwUrl, url -> {
            ApiClient c = Configuration.getDefaultApiClient();
            c.setBasePath(url);

            Integer ct = queryParameters.getConnectTimeout() != null ? queryParameters.getConnectTimeout() : connectTimeout;
            if (ct != null) c.setConnectTimeout(ct * 1000);

            Integer rt = queryParameters.getReadTimeout() != null ? queryParameters.getReadTimeout() : readTimeout;
            if (rt != null) c.setReadTimeout(rt * 1000);

            Integer wt = queryParameters.getWriteTimeout() != null ? queryParameters.getWriteTimeout() : writeTimeout;
            if (wt != null) c.setWriteTimeout(wt * 1000);

            return c;
        });
    }

    private QwUtil() {
    }

    public static List<String> getSchemas() throws ApiException {
        logger.debug("QwUtil-getSchemas");
        List<String> res = new ArrayList<>();
        ApiClient client = getDefaultClient(null, null, null, null);
        if (client == null) return res;
        IndexesApi indexesApi = new IndexesApi(client);
        List<VersionedIndexMetadata> indexesMetadatas = indexesApi.getIndexesMetadatas();

        for (VersionedIndexMetadata bucket1 : indexesMetadatas) {
            String schemaName = bucket1.getVersionedIndexMetadataOneOf().getIndexConfig().getVersionedIndexConfigOneOf().getIndexId();
            res.add(schemaName);
            logger.debug(schemaName);
        }
        return res;
    }

    public static List<String> getTableNames(String schema) throws ApiException {
        logger.debug("QwUtil-getTableNames");
        List<String> res = new ArrayList<>();
        ApiClient client = getDefaultClient(null, null, null, null);
        if (client == null) return res;
        IndexesApi indexesApi = new IndexesApi(client);
        List<VersionedIndexMetadata> indexesMetadatas = indexesApi.getIndexesMetadatas();

        for (VersionedIndexMetadata bucket1 : indexesMetadatas) {
            String schemaName = bucket1.getVersionedIndexMetadataOneOf().getIndexConfig().getVersionedIndexConfigOneOf().getIndexId();
            res.add(schemaName);
            logger.debug(schemaName);
        }
        return res;
    }

//    public static List<UlakRow> select(String tableName,String qwUrl, String qwIndex) throws IOException, ClassNotFoundException, SQLException, ApiException  {
//
//        QueryParameters queryParameters = QueryParameters.getQueryParameters(tableName);
//        return select(queryParameters, qwUrl, qwIndex);
//    }

    /**
     * Extract the {@code "message"} field from a Quickwit error response body
     * (Quickwit returns {@code {"message":"..."}} on parse / config errors).
     * Returns {@code null} if the body is not JSON or has no message field.
     */
    private static String extractQwErrorMessage(String body) {
        if (body == null || body.isEmpty()) return null;
        try {
            com.fasterxml.jackson.databind.JsonNode node = ConnectorBaseUtil.getObjectMapper().readTree(body);
            if (node.has("message")) return node.get("message").asText();
        } catch (Exception ignore) { }
        return null;
    }

    public static String replaceTrinoQWVars(String query){
        query=replaceAll(query,"|"," ");
        query=replaceAll(query," not "," NOT ");
        query=replaceAll(query," or "," OR ");
        query=replaceAll(query," and "," AND ");
        query=replaceAll(query,":IN [*]",":*");
        query=replaceAll(query,":IN [-]",":*");
        query=replaceAll(query,":IN []",":*");
        query=replaceAll(query,":IN [ ]",":*");
        return query;
    }
    public static List<UlakRow> select(QueryParameters queryParameters,
                                           String qwUrl,
                                           String qwIndex,
                                           Integer connectTimeout,
                                           Integer readTimeout,
                                           Integer writeTimeout ) throws ApiException {
        return select(queryParameters, qwUrl, qwIndex, connectTimeout, readTimeout, writeTimeout, null);
    }

    public static List<UlakRow> select(QueryParameters queryParameters,
                                           String qwUrl,
                                           String qwIndex,
                                           Integer connectTimeout,
                                           Integer readTimeout,
                                           Integer writeTimeout,
                                           Long catalogHistoryTimeThresholdSeconds ) throws ApiException {
        queryParameters.setQuery(replaceTrinoQWVars(queryParameters.getQuery()));
        queryParameters.setDbType(DBType.QW);
        if(StringUtils.isBlank(queryParameters.getQwUrl())) {
            queryParameters.setQwUrl(qwUrl);
        }
        if(StringUtils.isBlank(queryParameters.getQwIndex())) {
            queryParameters.setQwIndex(qwIndex);
        }

        long effectiveThreshold = catalogHistoryTimeThresholdSeconds != null
                ? catalogHistoryTimeThresholdSeconds
                : DEFAULT_HISTORY_TIME_THRESHOLD_SECONDS;

        logger.warn("DEBUG HISTORY: isHistoryEnabled={}, historyIndex={}, from={}, to={}, range={}, qwIndex={}, threshold={}",
                queryParameters.isHistoryEnabled(),
                queryParameters.getHistoryIndex(),
                queryParameters.getFrom(),
                queryParameters.getTo(),
                (queryParameters.getTo() - queryParameters.getFrom()),
                queryParameters.getQwIndex(),
                effectiveThreshold);

        // Switch to history index if enable_history is true and time range exceeds threshold
        if (StringUtils.isNotBlank(queryParameters.getHistoryIndex())) {
            long from = queryParameters.getFrom();
            long to = queryParameters.getTo();
            long range = to - from;

            boolean rangeExceedsThreshold = (from > 0 && to > 0 && range > effectiveThreshold);
            if (queryParameters.isHistoryEnabled() && rangeExceedsThreshold) {
                logger.warn("Switching to history index '{}' (isHistoryEnabled=true, range {}s > threshold {}s)",
                        queryParameters.getHistoryIndex(), range, effectiveThreshold);
                queryParameters.setQwIndex(queryParameters.getHistoryIndex());
            } else {
                logger.warn("Staying on raw index '{}' (isHistoryEnabled={}, rangeExceedsThreshold={})",
                        queryParameters.getQwIndex(), queryParameters.isHistoryEnabled(), rangeExceedsThreshold);
            }
        }

        logger.debug("Executing select : {}\n\n\nurl:{}\n\n\nindex:{}",
                queryParameters.getQuery(),
                queryParameters.getQwUrl(),
                queryParameters.getQwIndex());
        queryParameters.setStart(System.currentTimeMillis());

        queryParameters.setError("");

        //                    addOneStat(hash, 1);
        return executeOneQuery( queryParameters,queryParameters.getQuery(),
                  connectTimeout,
                  readTimeout,
                  writeTimeout);
    }
    public static String executeQueryScript(String query) {
        long unixTime = System.currentTimeMillis() / 1000L;

        if (query != null) {
            String[] lines = query.split("\\r?\\n");
            StringBuilder sb = new StringBuilder();
            for (String line : lines) {
                String trimmed = line.trim();
                if (!trimmed.startsWith("//") && !trimmed.startsWith("-")) {
                    sb.append(line).append(" ");
                }
            }
            query = sb.toString();
        }

        query = executeScript(
                "var now = " + unixTime + ";" +
                        "var d = 24*60*60 /*number of seconds in a day*/;" +
                        "var h = 60*60 /*number of seconds in an hour*/;" +
                        "var m = 60 /*number of seconds in a minute*/;" +
                        "var s = 1 /*number of seconds in a second*/;" +
                        "var math = Math;" +
                        "var a = " + query + ";" +
                        "JSON.stringify(a);");

        return query;
    }
    public static String executeScript(String query) {
        return RhinoExecutor.executeScript(query);
    }
    public static List<UlakRow> executeOneQuery( QueryParameters queryParameters,
                                                     String query,
                                                     Integer connectTimeout,
                                                     Integer readTimeout,
                                                     Integer writeTimeout) throws ApiException {

        queryParameters.setQuery(replaceTrinoQWVars(queryParameters.getQuery()));
        if (queryParameters.getHasJs()) {
            try {
                query = executeQueryScript(query);
            } catch (Exception e) {
                logger.error("hasjs script execution failed for {} on {}/{}",
                        queryParameters.getQuery(),
                        queryParameters.getQwUrl(),
                        queryParameters.getQwIndex(),
                        e);
                // Surface the real Rhino failure to the caller. Without this
                // rethrow the un-evaluated query (with raw `Math.floor(...)`)
                // silently falls through to Gson, which then dies with an
                // opaque NumberFormatException — masking the actual cause.
                throw new ApiException("hasjs script execution failed: "
                        + e.getClass().getSimpleName() + ": " + e.getMessage());
            }
        }

        if (queryParameters.isHistoryEnabled() &&
                queryParameters.getHistoryIndex() != null &&
                queryParameters.getHistoryIndex().equals(queryParameters.getQwIndex())) {
            logger.debug("Rewriting query for history index '{}'", queryParameters.getQwIndex());
            query = QwQueryRewriter.rewriteQueryForHistory(query);
        }

        logger.debug("Executing executeOneQuery: {}\n\n\nurl:{}\n\n\nindex:{}",
                queryParameters.getQuery(),
                queryParameters.getQwUrl(),
                queryParameters.getQwIndex());

        String qwIndex = queryParameters.getQwIndex();

        SearchApi searchApi = new SearchApi(getDefaultClient(queryParameters,
                connectTimeout,
                readTimeout,
                writeTimeout));

        SearchRequestQueryString toQuery;
        try {
            toQuery = getGson().fromJson(query, SearchRequestQueryString.class);
        } catch (Exception e) {
            logger.error("Error parsing query JSON in {}/{}: {}", queryParameters.getQwUrl(), qwIndex, query, e);
            throw new ApiException("Failed to parse Quickwit query JSON: " + e.getMessage());
        }
        if (toQuery == null) {
            throw new ApiException("Quickwit query JSON parsed to null body — refusing to send empty POST");
        }
        // Defensive: if a Grafana template variable made it this far un-substituted
        // (e.g. `${retention_period_in_hours}h` inside `fixed_interval`), Quickwit
        // would reject the request with an opaque "NumberMissing" tantivy error.
        // Reject early with the dashboard variable name in the message.
        if (query.contains("${")) {
            int idx = query.indexOf("${");
            int end = query.indexOf("}", idx);
            String token = end > idx ? query.substring(idx, end + 1) : query.substring(idx, Math.min(idx + 60, query.length()));
            throw new ApiException("Grafana template not substituted: " + token + " — set a default value on the dashboard variable");
        }
        logger.debug("Running on {}/{}: {}", queryParameters.getQwUrl(), qwIndex, query);
        Call call = searchApi.searchPostHandlerCall(qwIndex, toQuery, null);

        try {
            // Wait for response (this is where it blocks)
            // ApiResponse<SearchResponseRest> resp = searchApi.getApiClient()
            //         .execute(call);
            try(Response execResp = call.execute()) {
                String body = execResp.body() != null ? execResp.body().string() : "";
                if (!execResp.isSuccessful()) {
                    throw new ApiException("Quickwit " + execResp.code() + ": " + extractQwErrorMessage(body));
                }
                SearchResponseRest ret;
                try {
                    ret = SearchResponseRest.fromJson(body);
                }
                catch (RuntimeException parseErr) {
                    String qwMsg = extractQwErrorMessage(body);
                    if (qwMsg != null) {
                        throw new ApiException("Quickwit error: " + qwMsg);
                    }
                    throw new ApiException("Quickwit response parse failed: " + parseErr.getMessage());
                }

                List<String> errors = ret == null ? new ArrayList<>() : ret.getErrors();
                if (!errors.isEmpty()) {
                    String error_text = String.join("\n\n", errors);
                    logger.error("Error from quickwit server: {}\n\n\nquery:{}\n\n\nurl:{}\n\n\nindex:{}\n\n\nret size:{}",
                            query,
                            queryParameters.getQwUrl(),
                            qwIndex,
                            error_text);
                    throw new ApiException(error_text);
                }
                if (ret == null) {
                    logger.error("Empty response quickwit server: {}\n\n\nquery:{}\n\n\nurl:{}\n\n\nindex:{}",
                            query,
                            queryParameters.getQwUrl(),
                            qwIndex);
                    throw new ApiException("Empty response quickwit server");
                }
                logger.debug("Query executed executeOneQuery: {}\n\n\nurl:{}\n\n\nindex:{}\n\n\nret size:{}",
                        query,
                        queryParameters.getQwUrl(),
                        queryParameters.getQwIndex(),
                        ret == null || ret.getAggregations() == null ? 0 : ((Map<String, Object>) ret.getAggregations()).size());
                List<UlakRow> parsed = parseResponse(queryParameters, ret);
                return parsed;
            }
        } catch (ApiException e) {
            if (call.isCanceled() || Thread.currentThread().isInterrupted()) {
                throw new TrinoException(StandardErrorCode.USER_CANCELED, "Query canceled", e);
            }
            throw e;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Gson getGson() {
        GsonBuilder gsonBuilder = new GsonBuilder().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE);
        Gson gson = gsonBuilder.create();
        return gson;
    }

    public static List<UlakRow> parseResponse(QueryParameters queryParameters,
                                                  SearchResponseRest ret) {
        String sv = queryParameters.getSqlVersion();
        if (sv == null || sv.trim().isEmpty()) {
            sv = "0";
        }
        if ("0".equals(sv)) {
            logger.warn("DEPRECATED: sqlversion=0 (JFlat) is deprecated. Migrate to sqlversion=0.2. See docs/adr/0003-sqlversion-deprecation.md");
        } else if ("0.1".equals(sv)) {
            logger.warn("DEPRECATED: sqlversion=0.1 is deprecated. Migrate to sqlversion=0.2. See docs/adr/0003-sqlversion-deprecation.md");
        }
        Object g = ret.getAggregations();
        if (g != null) {
            List<UlakRow> results = new ArrayList<>();
            if ("0".equals(sv)) {
                parseResponseAggregations(ret);
                List<Map<String, Object>> rawRows = flatten(g, "");
                for (Map<String, Object> rawRow : rawRows) {
                    Map<String, Object> newRow = new LinkedHashMap<>();
                    for (Map.Entry<String, Object> entry : rawRow.entrySet()) {
                        String k = entry.getKey();
                        Object val = entry.getValue();
                        String slashedKey = k.startsWith("/") ? k : "/" + k;
                        String bareKey = k.startsWith("/") ? k.substring(1) : k;
                        newRow.put(slashedKey, String.valueOf(val));
                        newRow.put(bareKey, String.valueOf(val));
                    }
                    results.add(new UlakRow(newRow));
                }
            } else {
                boolean stripSuffixes = "0.2".equals(sv);
                traverseAggregations((Map<String, Object>) g, new LinkedHashMap<>(), results, stripSuffixes);
            }
            if (!results.isEmpty()) {
                results = postProcessRows(results, queryParameters);
                return trimTimeEdges(results, queryParameters);
            }
            // No data from traverseAggregations (empty time window / no matching docs).
            // If //columns= is declared, fall through to parseResponseHits which produces
            // a 1-null-row with the declared schema — prevents COLUMN_NOT_FOUND at plan time.
            Object[] declaredCols = queryParameters.getColumns();
            if (declaredCols == null || declaredCols.length == 0) {
                return results; // truly empty, no fallback declared
            }
            // fall through to parseResponseHits below
        }
        if (g != null) {
            parseResponseAggregations(ret);
        }
        return parseResponseHits(queryParameters, ret);
    }

    private static List<UlakRow> postProcessRows(List<UlakRow> rows, QueryParameters queryParameters) {
        String toReplace = queryParameters.getReplaceFromColumns();
        if (StringUtils.isBlank(toReplace)) {
            return rows;
        }
        List<UlakRow> processed = new ArrayList<>();
        for (UlakRow row : rows) {
            Map<String, Object> originalMap = row.getColumnMap();
            Map<String, Object> newMap = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : originalMap.entrySet()) {
                String k = entry.getKey();
                Object val = entry.getValue();
                
                String newKey = StringUtils.replace(k, toReplace, "");
                String slashedKey = newKey.startsWith("/") ? newKey : "/" + newKey;
                String bareKey = newKey.startsWith("/") ? newKey.substring(1) : newKey;
                
                newMap.put(bareKey, val);
                newMap.put(slashedKey, val);
            }
            processed.add(new UlakRow(newMap));
        }
        return processed;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> flatten(Object node, String path) {
        List<Map<String, Object>> rows = new ArrayList<>();
        if (node instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) node;
            if (map.isEmpty()) {
                rows.add(new LinkedHashMap<>());
                return rows;
            }
            List<Map<String, Object>> currentRows = new ArrayList<>();
            currentRows.add(new LinkedHashMap<>());
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                String subPath = path.isEmpty() ? entry.getKey() : path + "/" + entry.getKey();
                List<Map<String, Object>> subRows = flatten(entry.getValue(), subPath);
                
                List<Map<String, Object>> newRows = new ArrayList<>();
                for (Map<String, Object> r1 : currentRows) {
                    for (Map<String, Object> r2 : subRows) {
                        Map<String, Object> merged = new LinkedHashMap<>(r1);
                        merged.putAll(r2);
                        newRows.add(merged);
                    }
                }
                currentRows = newRows;
            }
            return currentRows;
        } else if (node instanceof List) {
            List<?> list = (List<?>) node;
            if (list.isEmpty()) {
                rows.add(new LinkedHashMap<>());
                return rows;
            }
            for (Object item : list) {
                rows.addAll(flatten(item, path));
            }
            return rows;
        } else {
            // Primitive
            Map<String, Object> single = new LinkedHashMap<>();
            single.put(path, node);
            rows.add(single);
            return rows;
        }
    }

    @SuppressWarnings("unchecked")
    private static void traverseAggregations(
            Map<String, Object> aggMap,
            Map<String, Object> currentRow,
            List<UlakRow> results,
            boolean stripSuffixes) {

        boolean hasBucketAgg = false;

        // First pass: collect leaf metric values (aggs with "value") into currentRow
        for (Map.Entry<String, Object> entry : aggMap.entrySet()) {
            Object aggValueObj = entry.getValue();
            if (!(aggValueObj instanceof Map)) continue;
            Map<String, Object> aggValue = (Map<String, Object>) aggValueObj;
            if (aggValue.containsKey(BUCKETS)) {
                hasBucketAgg = true;
            } else {
                Object value = aggValue.get(VALUE);
                if (value != null) {
                    // v0.2: bare aggId; v0.1: aggId/value
                    String colName = stripSuffixes ? entry.getKey() : entry.getKey() + "/" + VALUE;
                    currentRow.put(colName, String.valueOf(value));
                    // expose "/aggId/value" alongside "aggId/value" so dashboards using either form work
                    if (!stripSuffixes) currentRow.put("/" + colName, String.valueOf(value));
                }
            }
        }

        if (!hasBucketAgg) {
            results.add(new UlakRow(new LinkedHashMap<>(currentRow)));
            return;
        }

        // Second pass: recurse into bucket aggs (aggs with "buckets")
        for (Map.Entry<String, Object> entry : aggMap.entrySet()) {
            Object aggValueObj = entry.getValue();
            if (!(aggValueObj instanceof Map)) continue;
            Map<String, Object> aggValue = (Map<String, Object>) aggValueObj;
            Object bucketsObj = aggValue.get(BUCKETS);
            if (!(bucketsObj instanceof List)) continue;

            String aggId = entry.getKey();
            for (Object bucketObj : (List<?>) bucketsObj) {
                if (!(bucketObj instanceof Map)) continue;
                Map<String, Object> bucket = (Map<String, Object>) bucketObj;
                Map<String, Object> rowForBucket = new LinkedHashMap<>(currentRow);

                Object key = bucket.get(KEY);
                Object keyAsString = bucket.get(KEY_AS_STRING);
                // v0.2: bare aggId for key, aggId_str for key_as_string; v0.1: aggId/key, aggId/key_as_string
                String keyCol = stripSuffixes ? aggId : aggId + "/" + KEY;
                String keyStrCol = stripSuffixes ? aggId + "_str" : aggId + "/" + KEY_AS_STRING;
                if (key instanceof Number) {
                    // Store as plain integer string, not scientific notation (e.g. "1773792000000" not "1.773792E12")
                    rowForBucket.put(keyCol, String.valueOf(((Number) key).longValue()));
                    // expose "/aggId/key" alongside "aggId/key" for dashboard compatibility
                    if (!stripSuffixes) rowForBucket.put("/" + keyCol, String.valueOf(((Number) key).longValue()));
                } else if (key != null) {
                    rowForBucket.put(keyCol, String.valueOf(key));
                    if (!stripSuffixes) rowForBucket.put("/" + keyCol, String.valueOf(key));
                }
                if (keyAsString != null) {
                    rowForBucket.put(keyStrCol, String.valueOf(keyAsString));
                    if (!stripSuffixes) rowForBucket.put("/" + keyStrCol, String.valueOf(keyAsString));
                }

                // collect sub-aggregation maps (skip primitive metadata: key, key_as_string, doc_count, etc.)
                Map<String, Object> subAggs = new LinkedHashMap<>();
                for (Map.Entry<String, Object> e2 : bucket.entrySet()) {
                    if (e2.getValue() instanceof Map) {
                        subAggs.put(e2.getKey(), e2.getValue());
                    }
                }

                if (subAggs.isEmpty()) {
                    results.add(new UlakRow(rowForBucket));
                } else {
                    traverseAggregations(subAggs, rowForBucket, results, stripSuffixes);
                }
            }
        }
    }

    private static List<UlakRow> trimTimeEdges(List<UlakRow> rows, QueryParameters queryParameters) {
        String timeField = queryParameters.getTimeField();
        if (StringUtils.isBlank(timeField) || rows.isEmpty()) return rows;

        // find the actual column name that ends with the configured timeField suffix
        String actualTimeField = null;
        for (String col : rows.get(0).getColumnMap().keySet()) {
            if (col.endsWith(timeField)) {
                actualTimeField = col;
                break;
            }
        }
        if (actualTimeField == null) return rows;

        long maxTime = 0, minTime = Long.MAX_VALUE;
        for (UlakRow row : rows) {
            String o = (String) row.getColumnMap().get(actualTimeField);
            if (o != null) {
                long v = (long) Double.parseDouble(o);
                if (v > maxTime) maxTime = v;
                if (v < minTime) minTime = v;
            }
        }

        if (maxTime == 0 || minTime == Long.MAX_VALUE || maxTime == minTime) return rows;

        final long fMax = maxTime, fMin = minTime;
        final String fTimeField = actualTimeField;
        rows.removeIf(row -> {
            String o = (String) row.getColumnMap().get(fTimeField);
            if (o == null) return false;
            long v = (long) Double.parseDouble(o);
            return v == fMax || v == fMin;
        });
        return rows;
    }

    public static void parseResponseAggregations(SearchResponseRest ret) {
        Object g = ret.getAggregations();
        if (g != null) {
            arrangeAggregations((Map<String, Object>)g, null );
        }
    }

    public static  void arrangeAggregations(Map<String,Object> aggregation, Map<String,Object> currentValuesIn){
        if (currentValuesIn == null) {
            currentValuesIn = new HashMap<>();
        }

        for(Map.Entry<String, Object> agg:aggregation.entrySet()) {
            String aggKey = agg.getKey();
            Object aggValueObj = agg.getValue();
            if(!(aggValueObj instanceof Map)) {
                arrangeAggregation(currentValuesIn, new HashMap<String, Object>() {{put(aggKey, aggValueObj);}},aggKey);
            }else{
                arrangeAggregation(currentValuesIn, (Map<String, Object>) aggValueObj,aggKey);
            }
        }
    }
    private static Map<String, Object> arrangeAggregation(Map<String, Object> currentValuesIn, Map<String, Object> aggValue, String aggKey) {
        Map<String, Object> currentValues = new HashMap<>(currentValuesIn);
        Object docCntVal = aggValue.getOrDefault(DOC_COUNT, -1L);
        long doc_count = docCntVal instanceof Long ? (long) docCntVal
                : docCntVal instanceof Integer ? (long)(int) docCntVal
                : (long)(double) docCntVal;
        Object key = aggValue.getOrDefault(KEY, null);
        String key_as_string = (String) aggValue.getOrDefault(KEY_AS_STRING, null);
        Object sumOtherDocCountObj =  aggValue.getOrDefault(SUM_OTHER_DOC_COUNT, -1L);
        double sum_other_doc_count = sumOtherDocCountObj instanceof Long ? (long) sumOtherDocCountObj
                : sumOtherDocCountObj instanceof Integer ? (long)(int) sumOtherDocCountObj
                : (long)(double) sumOtherDocCountObj;
        String prefix = aggKey + "/" ;
        if (doc_count > 0) {
            currentValues.put(prefix +  DOC_COUNT, doc_count);
            aggValue.remove(DOC_COUNT);
        }
        if (key != null) {
            currentValues.put(prefix +  KEY, key);
            aggValue.remove(KEY);
        }
        if (key_as_string != null) {
            currentValues.put(prefix +  KEY_AS_STRING, key_as_string);
            aggValue.remove(KEY_AS_STRING);
        }
        if (sum_other_doc_count > -1) {
            currentValues.put(prefix +  SUM_OTHER_DOC_COUNT, sum_other_doc_count);
            aggValue.remove(SUM_OTHER_DOC_COUNT);
        }


        Object value = aggValue.getOrDefault(VALUE, null);
        if (value != null) {
            aggValue.putAll(currentValues);
        }
        Object buckets = aggValue.getOrDefault(BUCKETS, null);
        if (buckets != null) {
            if(value!=null) {
                currentValues.put(prefix +  VALUE, value);
            }
            for(Map<String, Object> bucket:((List<Map<String, Object>>)buckets)) {
                Map<String, Object> newCurrentValues=arrangeAggregation(currentValues,bucket,aggKey);
                if(newCurrentValues!=null){
                    currentValues=newCurrentValues;
                    arrangeAggregations(bucket, currentValues);
                }
            }
        }
        if((buckets==null && value==null) && aggValue.isEmpty()){
            aggValue.putAll(currentValues);
            return null;
        }
        return currentValues;
    }
    public static List<UlakRow> parseResponseHits(QueryParameters queryParameters,
                                                      SearchResponseRest ret) {
        Object g = ret.getAggregations();
        if (g == null) {
            g = ret.getHits();
        }
        String json = getGson().toJson(g);
        JFlat flatMe = new JFlat(json);
        List<Object[]> flatted = null;
        if(json.equals("[]")){
            flatted=new ArrayList<>();
        }else{
            flatted = flatMe.json2Sheet().getJsonAsSheet();
        }

        Map<String, Integer> headerIndexes = new HashMap<>();
        Object[] headers = flatted.size()==0 ? null : flatted.get(0);
        if(headers==null || headers.length==0){
            //get headers from columns
            headers = queryParameters.getColumns();
            if(headers!=null && headers.length>0 ){
//                headers = new Object[0];
                //a simple empty row for columns
                flatted.add(new Object[headers.length]);
            }else{
                headers =  flatted.get(0);
            }
        }

        for (int i = 0; i < headers.length; i++) {
            headerIndexes.put((String) headers[i], i);
        }
        List<UlakRow> toRet = new ArrayList<>();
        long maxTime = 0;
        long minTime = Long.MAX_VALUE;
        String timeField = queryParameters.getTimeField();
        for (int i = 1; i < flatted.size(); i++) {
            Map<String, Object> r = new HashMap<>();
            Object[] c = flatted.get(i);
            boolean allNulls=true;
            for (int j = 0; j < headers.length; j++) {
                Object val = j < c.length ? c[j] : null;
                if (queryParameters.getNullFill()
                        && val == null
                        && i + 1 < flatted.size()) {
                    val = flatted.get(i + 1)[j];
                }
                String value = cleanScientificNotation(String.valueOf(val));
                if(!Strings.isNullOrEmpty(value)){
                    value = StringUtils.strip(value, "\"");
                    if(value.equals("null")) {
                        value=null;
                    }
                }
                String k = (String) headers[j];
                String toReplace =queryParameters.getReplaceFromColumns();
                if(StringUtils.isNotBlank(toReplace)){
                    k=StringUtils.replace((String) k, toReplace,"");
                }
                String slashedKey = k.startsWith("/") ? k : "/" + k;
                if (k.startsWith("/")) k = k.substring(1);
                boolean isTimeField =StringUtils.isNotBlank(timeField) && k.endsWith(timeField);
                if(value!=null){
                    if(isTimeField){
                        timeField = k;
                        double parsed = Double.parseDouble(value);
                        if(parsed >maxTime){
                            maxTime=(long)parsed;
                        }
                        if(parsed <minTime){
                            minTime=(long)parsed;
                        }
                    }
                    allNulls=false;
                }
                // Trino column names: existing dashboards select either
                // "X/key" (no leading slash) or "/X/key" (with). Expose
                // both forms so a `select "/6/key"` works alongside the
                // historical `select "1/5/key"`.
                r.put(k, value);
                if (!slashedKey.equals(k)) {
                    r.put(slashedKey, value);
                }
            }
            if (!allNulls) {
                toRet.add(new UlakRow(r));
            }
        }
        if(maxTime>0 && minTime<Long.MAX_VALUE && StringUtils.isNotBlank(timeField)){
            ArrayList<Integer> toRemove = new ArrayList<>();
            for(int i=0; i<toRet.size(); i++){
                UlakRow row = toRet.get(i);
                String o = (String) row.getColumnMap().get(timeField);
                if(o!=null ){
                    long v = (long)Double.parseDouble(o);
                    if(v == maxTime || v == minTime) {
                        toRemove.add(i);
                    }
                }
            }
            while(toRemove.size()>0){
                toRet.remove(toRemove.get(toRemove.size()-1));
                toRemove.remove(toRemove.size()-1);
            }
        }
        return toRet;
    }

    // -----------------------------------------------------------------------
    // J56 — Plain Table Query Mode helpers
    // -----------------------------------------------------------------------

    /** Returns true when tableName is a bare index name (no //param= directives). */
    public static boolean isPlainTableMode(String tableName) {
        return PlainTableQuery.isPlainMode(tableName);
    }

    /** Builds a match-all query string for a plain index name. */
    public static String buildPlainTableQuery(String indexName) {
        return PlainTableQuery.buildMatchAllQuery(indexName);
    }

    /** Builds a filtered query string for a plain index name. */
    public static String buildPlainTableQuery(String indexName, String qwFilter, int maxHits) {
        return PlainTableQuery.buildFilteredQuery(indexName, qwFilter, maxHits);
    }

    /**
     * Returns schema columns for a plain-mode index by reading its DocMapping
     * from the Quickwit index API — no live search query required.
     */
    public static List<ColumnMetadata> getColumnsFromDocMapping(
            String indexName, String qwUrl,
            Integer connectTimeout, Integer readTimeout, Integer writeTimeout) throws ApiException {
        ApiClient client = defaultClients.computeIfAbsent(qwUrl, url -> {
            ApiClient c = Configuration.getDefaultApiClient();
            c.setBasePath(url);
            if (connectTimeout != null) c.setConnectTimeout(connectTimeout * 1000);
            if (readTimeout != null) c.setReadTimeout(readTimeout * 1000);
            if (writeTimeout != null) c.setWriteTimeout(writeTimeout * 1000);
            return c;
        });
        IndexesApi indexesApi = new IndexesApi(client);
        List<VersionedIndexMetadata> metas = indexesApi.getIndexesMetadatas();
        for (VersionedIndexMetadata meta : metas) {
            com.quickwit.javaclient.models.VersionedIndexConfigOneOf cfg =
                    meta.getVersionedIndexMetadataOneOf().getIndexConfig().getVersionedIndexConfigOneOf();
            if (!indexName.equals(cfg.getIndexId())) continue;
            DocMapping docMapping = cfg.getDocMapping();
            if (docMapping == null || docMapping.getFieldMappings() == null) break;
            List<ColumnMetadata> cols = new ArrayList<>();
            for (FieldMappingEntryForSerialization fm : docMapping.getFieldMappings()) {
                cols.add(new ColumnMetadata(fm.getName(), fieldTypeToTrino(fm.getType())));
            }
            return cols;
        }
        logger.warn("No DocMapping found for index '{}' at {}; plain-mode schema will be empty", indexName, qwUrl);
        return Collections.emptyList();
    }

    private static Type fieldTypeToTrino(String qwType) {
        if (qwType == null) return VarcharType.VARCHAR;
        switch (qwType.toLowerCase()) {
            case "u64": case "i64": case "u32": case "i32":
            case "u16": case "i16": case "u8":  case "i8":
                return BigintType.BIGINT;
            case "f64": case "f32":
                return DoubleType.DOUBLE;
            case "bool":
                return BooleanType.BOOLEAN;
            default:
                return VarcharType.VARCHAR;
        }
    }

    public static boolean hasTimestampField(String indexName, String qwUrl,
                                            Integer connectTimeout,
                                            Integer readTimeout,
                                            Integer writeTimeout) {
        try {
            ApiClient client = defaultClients.computeIfAbsent(qwUrl, url -> {
                ApiClient c = Configuration.getDefaultApiClient();
                c.setBasePath(url);
                if (connectTimeout != null) c.setConnectTimeout(connectTimeout * 1000);
                if (readTimeout != null) c.setReadTimeout(readTimeout * 1000);
                if (writeTimeout != null) c.setWriteTimeout(writeTimeout * 1000);
                return c;
            });
            IndexesApi indexesApi = new IndexesApi(client);
            List<VersionedIndexMetadata> metas = indexesApi.getIndexesMetadatas();
            for (VersionedIndexMetadata meta : metas) {
                com.quickwit.javaclient.models.VersionedIndexConfigOneOf cfg =
                        meta.getVersionedIndexMetadataOneOf().getIndexConfig().getVersionedIndexConfigOneOf();
                if (indexName.equals(cfg.getIndexId())) {
                    DocMapping docMapping = cfg.getDocMapping();
                    return docMapping != null && docMapping.getTimestampField() != null && !docMapping.getTimestampField().trim().isEmpty();
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to check timestamp field for index '{}' at {}: {}", indexName, qwUrl, e.getMessage());
        }
        return false;
    }

    public static String rewriteQueryForHistory(String queryJson) {
        return QwQueryRewriter.rewriteQueryForHistory(queryJson);
    }

    public static String cleanScientificNotation(String value) {
        if (value == null || "null".equals(value)) return value;
        if (value.contains("E") || value.contains("e")) {
            try {
                java.math.BigDecimal bd = new java.math.BigDecimal(value);
                if (bd.scale() <= 0 || bd.remainder(java.math.BigDecimal.ONE).compareTo(java.math.BigDecimal.ZERO) == 0) {
                    return bd.toBigInteger().toString();
                }
                return bd.toPlainString();
            } catch (Exception e) {
                // ignore
            }
        }
        if (value.endsWith(".0")) {
            return value.substring(0, value.length() - 2);
        }
        return value;
    }

}

