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
import org.mozilla.javascript.Context;
import org.mozilla.javascript.Scriptable;

import static com.facebook.presto.ulak.QueryParameters.replaceAll;

public class QwUtil {

    private static Logger logger = LoggerFactory.getLogger(QwUtil.class);

    private static Map<String, ApiClient> defaultClients = null;

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
            logger.error("url is null : {}\n\n\nurl:{}\n\n\nindex:{}",
                    queryParameters.getQuery(),
                    queryParameters.getQwUrl(),
                    queryParameters.getQwIndex());
            return null;
        }
        ApiClient client;
        if (defaultClients == null) {
            defaultClients = new LinkedHashMap<>();
        }
        if (!defaultClients.containsKey(qwUrl)) {
            client = Configuration.getDefaultApiClient();
            client.setBasePath(qwUrl);

            Integer newTimeout = queryParameters.getConnectTimeout();
            if (newTimeout == null) {
                newTimeout =  connectTimeout;
            }
            if (newTimeout != null) {
                client.setConnectTimeout(newTimeout*1000);
            }

            newTimeout = queryParameters.getReadTimeout();
            if (newTimeout == null) {
                newTimeout =  readTimeout;
            }
            if (newTimeout != null) {
                client.setReadTimeout(newTimeout*1000);
            }

            newTimeout = queryParameters.getWriteTimeout();
            if (newTimeout == null) {
                newTimeout =  writeTimeout;
            }
            if (newTimeout != null) {
                client.setWriteTimeout(newTimeout*1000);
            }

            defaultClients.put(qwUrl, client);
        } else {
            client = defaultClients.get(qwUrl);
        }
        return client;
    }

    private QwUtil() {
    }

    public static List<String> getSchemas() throws ApiException {
        logger.debug("QwUtil-getSchemas");
        List<String> res = new ArrayList<>();
        IndexesApi indexesApi = new IndexesApi(getDefaultClient(null,null,null,null));
        List<VersionedIndexMetadata> indexesMetadatas = indexesApi.getIndexesMetadatas();

        for (VersionedIndexMetadata bucket1 : indexesMetadatas) {
            String schemaName = bucket1.getVersionedIndexMetadataOneOf().getIndexConfig().getVersionedIndexConfigOneOf().getIndexId();
            res.add(schemaName);
            logger.debug(schemaName);
        }
        return res;
    }

    public static List<String> getTableNames(String schema) throws ApiException {
        logger.debug("QwUtil-getSchemas");
        List<String> res = new ArrayList<>();
        IndexesApi indexesApi = new IndexesApi(getDefaultClient(null,null,null,null));
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

    public static String replaceTrinoQWVars(String query){
        query=replaceAll(query,"|"," ");
        query=replaceAll(query," not "," NOT ");
        query=replaceAll(query,":IN [*]",":*");
        query=replaceAll(query,":IN [-]",":*");
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
    public static String executeScript(String query) {
        // Creates and enters a Context. The Context stores information
        // about the execution environment of a script.
        Context cx = Context.enter();
        try {
            // Initialize the standard objects (Object, Function, etc.)
            // This must be done before scripts can be executed. Returns
            // a scope object that we use in later calls.
            Scriptable scope = cx.initStandardObjects();


            // Now evaluate the string we've colected.
            Object result = cx.evaluateString(scope, query, "<cmd>", 1, null);

            // Convert the result to a string and print it.
            logger.debug(result.toString());
            return (String) result;
        } finally {
            // Exit from the context.
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
                logger.error("Error Executing executeQueryScript: {}\n\n\nurl:{}\n\n\nindex:{}\n\n\nerror:{}",
                        queryParameters.getQuery(),
                        queryParameters.getQwUrl(),
                        queryParameters.getQwIndex(),
                        e.getMessage());
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

        SearchRequestQueryString toQuery = null;
        try {
            toQuery = getGson().fromJson(query, SearchRequestQueryString.class);
        } catch (Exception e) {
            logger.error("Error in {}/{}: {}", queryParameters.getQwUrl(), qwIndex, query);
        }
        logger.debug("Running on {}/{}: {}", queryParameters.getQwUrl(), qwIndex, query);
        Call call = searchApi.searchPostHandlerCall(qwIndex, toQuery, null);

        try {
            // Wait for response (this is where it blocks)
//            ApiResponse<SearchResponseRest> resp = searchApi.getApiClient()
//                    .execute(call);
            try(Response execResp = call.execute()) {
                SearchResponseRest ret = SearchResponseRest.fromJson(execResp.body().string());

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
            traverseAggregations((Map<String, Object>) g, new HashMap<>(), results);
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
            List<UlakRow> results) {

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
                    currentRow.put(entry.getKey() + "/" + VALUE, String.valueOf(value));
                }
            }
        }

        if (!hasBucketAgg) {
            results.add(new UlakRow(new HashMap<>(currentRow)));
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
                Map<String, Object> rowForBucket = new HashMap<>(currentRow);

                Object key = bucket.get(KEY);
                Object keyAsString = bucket.get(KEY_AS_STRING);
                if (key instanceof Number) {
                    // Store as plain integer string, not scientific notation (e.g. "1773792000000" not "1.773792E12")
                    rowForBucket.put(aggId + "/" + KEY, String.valueOf(((Number) key).longValue()));
                } else if (key != null) {
                    rowForBucket.put(aggId + "/" + KEY, String.valueOf(key));
                }
                if (keyAsString != null) rowForBucket.put(aggId + "/" + KEY_AS_STRING, String.valueOf(keyAsString));

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
                    traverseAggregations(subAggs, rowForBucket, results);
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

        if (maxTime == 0 || minTime == Long.MAX_VALUE) return rows;

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
        long doc_count =docCntVal instanceof Long ? (long) docCntVal : (long)(double)docCntVal;
        Object key = aggValue.getOrDefault(KEY, null);
        String key_as_string = (String) aggValue.getOrDefault(KEY_AS_STRING, null);
        Object sumOtherDocCountObj =  aggValue.getOrDefault(SUM_OTHER_DOC_COUNT, -1L);
        double sum_other_doc_count = sumOtherDocCountObj instanceof Long ? (long) sumOtherDocCountObj : (long)(double)sumOtherDocCountObj;
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
                Object val = c[j];
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
                r.put(k, value);
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

    public static void main(String[] args)    {
        long start = System.currentTimeMillis();
        QueryParameters params = QueryParameters.getQueryParameters(" \n" +
                "  //ttl=150\n" +
                "              //refresh=75\n" +
                "              //cache=false\n" +
                "              //name=Dataplane Status\n" +
                "              //columns=1/5/key,1/10/key,1/4/key,1/9/key,1/8/key,1/7/key,11/value,1/6/key,1/2/key,1/value,111/value,222/value,abc\n" +
                "              //dbtype=qw\n" +
                "              //qwindex=metrics3\n" +
                "              //replacefromcolumns=/3/buckets/2/buckets/4/buckets/5/buckets/6/buckets/7/buckets/8/buckets/9/buckets/10/buckets/\n" +
                "              //hasjs=true\n" +
                "              //from=1725547942\n" +
                "              //to=1725548242\n" +
                "              \n" +
                "              {\n" +
                "              \"aggs\": {\n" +
                "                \"3\": {\n" +
                "                  \"aggs\": {\n" +
                "                    \"2\": {\n" +
                "                      \"aggs\": {\n" +
                "                        \"4\": {\n" +
                "                          \"aggs\": {\n" +
                "                            \"5\": {\n" +
                "                              \"aggs\": {\n" +
                "                                \"6\": {\n" +
                "                                  \"aggs\": {\n" +
                "                                    \"7\": {\n" +
                "                                      \"aggs\": {\n" +
                "                                        \"8\": {\n" +
                "                                          \"aggs\": {\n" +
                "                                            \"9\": {\n" +
                "                                              \"aggs\": {\n" +
                "                                                \"10\": {\n" +
                "                                                  \"aggs\": {\n" +
                "                                                    \"222\": {\n" +
                "                                                      \"avg\": {\n" +
                "                                                        \"field\": \"span_attributes.availability\"\n" +
                "                                                      }\n" +
                "                                                    },\n" +
                "                                                    \"1\": {\n" +
                "                                                      \"sum\": {\n" +
                "                                                        \"field\": \"span_attributes.status_ni\"\n" +
                "                                                      }\n" +
                "                                                    },\n" +
                "                                                    \"11\": {\n" +
                "                                                      \"max\": {\n" +
                "                                                        \"field\": \"span_start_timestamp_nanos\"\n" +
                "                                                      }\n" +
                "                                                    },\n" +
                "                                                    \"111\": {\n" +
                "                                                      \"value_count\": {\n" +
                "                                                        \"field\": \"span_attributes.status_ni\"\n" +
                "                                                      }\n" +
                "                                                    }\n" +
                "                                                  },\n" +
                "                                                  \"terms\": {\n" +
                "                                                    \"field\": \"span_attributes.m_ns_name\", \n" +
                "                                                    \"size\":1,\n" +
                "                                                    \"order\": {\n" +
                "                                                      \"11\": \"desc\"\n" +
                "                                                    },\n" +
                "                                                    \"min_doc_count\": 1\n" +
                "                                                  }\n" +
                "                                                }\n" +
                "                                              },\n" +
                "                                              \"terms\": {\n" +
                "                                                \"field\": \"span_attributes.m_target\", \n" +
                "                                                \"size\":9999,\n" +
                "                                                \"order\": {\n" +
                "                                                  \"_key\": \"desc\"\n" +
                "                                                },\n" +
                "                                                \"min_doc_count\": 1\n" +
                "                                              }\n" +
                "                                            }\n" +
                "                                          },\n" +
                "                                          \"terms\": {\n" +
                "                                            \"field\": \"span_attributes.m_ns_id\", \n" +
                "                                            \"size\":9999,\n" +
                "                                            \"order\": {\n" +
                "                                              \"_key\": \"desc\"\n" +
                "                                            },\n" +
                "                                            \"min_doc_count\": 1\n" +
                "                                          }\n" +
                "                                        }\n" +
                "                                      },\n" +
                "                                      \"terms\": {\n" +
                "                                        \"field\": \"span_attributes.m_uuid\", \n" +
                "                                        \"size\":9999,\n" +
                "                                        \"order\": {\n" +
                "                                          \"_key\": \"desc\"\n" +
                "                                        },\n" +
                "                                        \"min_doc_count\": 1\n" +
                "                                      }\n" +
                "                                    }\n" +
                "                                  },\n" +
                "                                  \"terms\": {\n" +
                "                                    \"field\": \"span_attributes.m_iface\", \n" +
                "                                    \"size\":9999,\n" +
                "                                    \"order\": {\n" +
                "                                      \"_key\": \"desc\"\n" +
                "                                    },\n" +
                "                                    \"min_doc_count\": 1\n" +
                "                                  }\n" +
                "                                }\n" +
                "                              },\n" +
                "                              \"terms\": {\n" +
                "                                \"field\": \"span_attributes.m_overlay\", \n" +
                "                                \"size\":9999,\n" +
                "                                \"order\": {\n" +
                "                                  \"_key\": \"desc\"\n" +
                "                                },\n" +
                "                                \"min_doc_count\": 1\n" +
                "                              }\n" +
                "                            }\n" +
                "                          },\n" +
                "                          \"terms\": {\n" +
                "                            \"field\": \"span_attributes.m_origin\", \n" +
                "                            \"size\":9999,\n" +
                "                            \"order\": {\n" +
                "                              \"_key\": \"desc\"\n" +
                "                            },\n" +
                "                            \"min_doc_count\": 1\n" +
                "                          }\n" +
                "                        }\n" +
                "                      },\n" +
                "                      \"terms\": {\n" +
                "                        \"field\": \"span_attributes.h\", \n" +
                "                        \"size\":9999,\n" +
                "                        \"order\": {\n" +
                "                          \"_key\": \"desc\"\n" +
                "                        },\n" +
                "                        \"min_doc_count\": 1\n" +
                "                      }\n" +
                "                    }\n" +
                "                  },\n" +
                "                  \"date_histogram\": {\n" +
                "                    \"field\": \"span_start_timestamp_nanos\",\n" +
                "                    \"fixed_interval\": \"10s\",\n" +
                "                    \"min_doc_count\": 1\n" +
                "                  }\n" +
                "                }\n" +
                "              },\n" +
                "              \"query\": \"span_attributes.p:maya_probe AND span_attributes.h:IN [ee7b566c-68d7-4ffb-9d0a-29477a39b195 ee7b566c-68d7-4ffb-9d0a-29477a39b196 ee7b566c-68d7-4ffb-9d0a-29477a39b197 ee7b566c-68d7-4ffb-9d0a-29477a39b198 ee7b566c-68d7-4ffb-9d0a-29477a39b199]\",\n" +
                "              \"max_hits\": 0,\n" +
                "              \"start_timestamp\": 1725547942,\n" +
                "              \"end_timestamp\": 1725548242\n" +
                "            }".toLowerCase());

        params.setQuery(replaceAll(params.getQuery(),"|"," "));
        params.setQuery(replaceAll(params.getQuery()," not "," NOT "));
        params.setQuery(replaceAll(params.getQuery(),":IN [*]",":*"));
        params.setQuery(replaceAll(params.getQuery(),":IN [-]",":*"));
        params.setQwIndex("flows3");
        params.setDbType(DBType.QW);
        params.setQwUrl("http://10.20.4.53:32215");
        params.setReplaceFromColumns("/3/buckets/2/buckets/4/buckets/5/buckets/1");
        params.setHasJs(true);
        params.setToBeCached(true);
        List<UlakRow> ret = null;
        try {
            ret = ConnectorBaseUtil.select(params,
                    false,new String[]{params.getQwUrl(), params.getQwIndex()}, (q, s)-> {
                        try {
//                                logger.debug("From UlakQuickwitMetadata getTableMetadata: {}\n\n\nurl:{}\n\n\nindex:{}",
//                                        q.getQuery(),
//                                        s[0],
//                                        s[1]);
                            return  QwUtil.select(q , s[0], s[1],null,null,null);
                        } catch (ApiException e) {
                            logger.error("ERRORSTRING", e);
                            throw new RuntimeException(e);
                        }
                    });
//            ret = Lists.newArrayList(QwUtil.select(params,params.getQwUrl(), params.getQwIndex()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        logger.info(String.valueOf(System.currentTimeMillis() - start));

    }
}
