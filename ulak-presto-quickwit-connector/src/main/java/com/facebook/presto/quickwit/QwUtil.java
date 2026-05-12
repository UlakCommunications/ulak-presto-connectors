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
import com.quickwit.javaclient.models.SearchRequestQueryString;
import com.quickwit.javaclient.models.SearchResponseRest;
import com.quickwit.javaclient.models.VersionedIndexMetadata;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import okhttp3.Call;
import okhttp3.Response;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.mozilla.javascript.ClassShutter;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.Scriptable;

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
        queryParameters.setQuery(replaceTrinoQWVars(queryParameters.getQuery()));
        queryParameters.setDbType(DBType.QW);
        if(StringUtils.isBlank(queryParameters.getQwUrl())) {
            queryParameters.setQwUrl(qwUrl);
        }
        if(StringUtils.isBlank(queryParameters.getQwIndex())) {
            queryParameters.setQwIndex(qwIndex);
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

        query = executeScript(
                "var now = " + unixTime + ";" +
                        "var d = 24*60*60 /*number of seconds in a day*/;" +
                        "var h = 60*60 /*number of seconds in an hour*/;" +
                        "var m = 60 /*number of seconds in a minute*/;" +
                        "var s = 1 /*number of seconds in a second*/;" +
                        "var a = " + query + ";" +
                        "JSON.stringify(a);");

        return query;
    }
    private static final int RHINO_INSTRUCTION_LIMIT = 100_000;

    public static String executeScript(String query) {
        Context cx = Context.enter();
        try {
            cx.setClassShutter(className -> false);
            cx.setInstructionObserverThreshold(RHINO_INSTRUCTION_LIMIT);
            Scriptable scope = cx.initSafeStandardObjects();
            Object result = cx.evaluateString(scope, query, "<cmd>", 1, null);
            if (result == null) {
                throw new RuntimeException("Rhino script returned null (script: " + query.substring(0, Math.min(120, query.length())) + ")");
            }
            if (!(result instanceof String)) {
                throw new RuntimeException("Rhino script returned " + result.getClass().getSimpleName() + " not String");
            }
            logger.debug(result.toString());
            return (String) result;
        }
        finally {
            Context.exit();
        }
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
//            ApiResponse<SearchResponseRest> resp = searchApi.getApiClient()
//                    .execute(call);
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
        Object g = ret.getAggregations();
        if (g != null && !"0".equals(queryParameters.getSqlVersion())) {
            List<UlakRow> results = new ArrayList<>();
            boolean stripSuffixes = "0.2".equals(queryParameters.getSqlVersion());
            traverseAggregations((Map<String, Object>) g, new LinkedHashMap<>(), results, stripSuffixes);
            return trimTimeEdges(results, queryParameters);
        }
        if (g != null) {
            parseResponseAggregations(ret);
        }
        return parseResponseHits(queryParameters, ret);
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
                } else if (key != null) {
                    rowForBucket.put(keyCol, String.valueOf(key));
                }
                if (keyAsString != null) rowForBucket.put(keyStrCol, String.valueOf(keyAsString));

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
                String value = String.valueOf(val);
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
//            if(!allNulls) {
                toRet.add(new UlakRow(r));
//            };
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

}

