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
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import com.quickwit.javaclient.ApiClient;
import com.quickwit.javaclient.ApiException;
import com.quickwit.javaclient.Configuration;
import com.quickwit.javaclient.api.IndexesApi;
import com.quickwit.javaclient.api.SearchApi;
import com.quickwit.javaclient.models.SearchRequestQueryString;
import com.quickwit.javaclient.models.SearchResponseRest;
import com.quickwit.javaclient.models.VersionedIndexMetadata;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
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

    public static ApiClient getDefaultClient(QueryParameters queryParameters) {
        String qwUrl = queryParameters!=null?queryParameters.getQwUrl():null;
        if (StringUtils.isBlank(qwUrl) ) {
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
        if(!defaultClients.containsKey(qwUrl)){
            client = Configuration.getDefaultApiClient();
            client.setBasePath(qwUrl);
            defaultClients.put(qwUrl, client);
        }else{
            client  = defaultClients.get(qwUrl);
        }
        return client;
    }

    private QwUtil() {
    }

    public static List<String> getSchemas() throws ApiException {
        logger.debug("QwUtil-getSchemas");
        List<String> res = new ArrayList<>();
        IndexesApi indexesApi = new IndexesApi(getDefaultClient(null));
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
        IndexesApi indexesApi = new IndexesApi(getDefaultClient(null));
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


    public static List<UlakRow> select(QueryParameters queryParameters,
                                           String qwUrl,
                                           String qwIndex ) throws ApiException {
        String q = queryParameters.getQuery();
        queryParameters.setQuery(replaceAll(q,"|"," "));
        queryParameters.setQuery(replaceAll(q," not "," NOT "));
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
        String query = queryParameters.getQuery();//"from(bucket: " + "\"" + bucket + "\"" + ")\n" + "|> range(start:" + time_interval + ")\n" + "|> filter(fn : (r) => r._measurement == " + "\"" + tableName + "\"" + ")";

        List<UlakRow> ret = executeOneQuery( queryParameters,query);

//                    addOneStat(hash, 1);
        return ret ;
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
                                                     String query) throws ApiException {

        query=replaceAll(query,"|"," ");
        query=replaceAll(query," not "," NOT ");

        if(queryParameters.getHasJs()) {
            query= executeQueryScript(query);
        }

        logger.debug("Executing executeOneQuery: {}\n\n\nurl:{}\n\n\nindex:{}",
                queryParameters.getQuery(),
                queryParameters.getQwUrl(),
                queryParameters.getQwIndex());

        String qwIndex = queryParameters.getQwIndex();

        SearchApi searchApi = new SearchApi(getDefaultClient(queryParameters));

        SearchRequestQueryString toQuery = getGson().fromJson(query, SearchRequestQueryString.class);
        logger.debug("Running on {}/{}: {}", queryParameters.getQwUrl(), qwIndex, query);
        SearchResponseRest ret = searchApi.searchPostHandler(qwIndex, toQuery);
        List<String> errors = ret.getErrors();
        if(errors!=null && !errors.isEmpty()) {
            String error_text = String.join("\n\n",errors);
            logger.error("Error from quickwit server: {}\n\n\nurl:{}\n\n\nindex:{}\n\n\nret size:{}",
                    query,
                    queryParameters.getQwUrl(),
                    qwIndex,
                    error_text);
            throw new ApiException(error_text);
        }
        logger.debug("Query executed executeOneQuery: {}\n\n\nurl:{}\n\n\nindex:{}\n\n\nret size:{}",
                query,
                queryParameters.getQwUrl(),
                queryParameters.getQwIndex(),
                ret == null && ret.getAggregations() == null ? 0 : ((Map<String, Object>)ret.getAggregations()).size());
        List<UlakRow> parsed = parseResponse(queryParameters,ret);
        return parsed;
    }

    public static Gson getGson() {
        GsonBuilder gsonBuilder = new GsonBuilder().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE);
        Gson gson = gsonBuilder.create();
        return gson;
    }

    public static List<UlakRow> parseResponse(QueryParameters queryParameters,
                                                  SearchResponseRest ret) {
        Object g = ret.getAggregations();
        if (g == null) {
            return parseResponseHits(queryParameters,ret);
        }
        parseResponseAggregations(ret);
        return parseResponseHits(queryParameters, ret);
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
        JFlat flatMe = new JFlat(getGson().toJson(g));
        List<Object[]> flatted = flatMe.json2Sheet().getJsonAsSheet();
        Map<String, Integer> headerIndexes = new HashMap<>();
        Object[] headers = flatted.get(0);
        if(headers==null || headers.length==0){
            //get headers from columns
            headers = queryParameters.getColumns();
            if(headers!=null){
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

        for (int i = 1; i < flatted.size(); i++) {
            Map<String, Object> r = new HashMap<>();
            Object[] c = flatted.get(i);
            boolean allNulls=true;
            for (int j = 0; j < headers.length; j++) {
                Object val = c[j];
                if (val == null && i + 1 < flatted.size()) {
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
                if(value!=null){
                    allNulls=false;
                }
                r.put(k, value);
            }
//            if(!allNulls) {
                toRet.add(new UlakRow(r));
//            };
        }
        return toRet;
    }

    public static void main(String[] args)    {
        long start = System.currentTimeMillis();
        QueryParameters params = QueryParameters.getQueryParameters(" \n" +
                "          //cache=true\n" +
                "          //name=1. Dataplane Status\n" +
                "          //columns=host,_time,_value\n" +
                "          //dbtype=qw\n" +
                "          //qwindex=metrics3\n" +
                "          //replacefromcolumns=/3/buckets/2/buckets/4/buckets/5/buckets/1\n" +
                "          //hasjs=true\n" +
                "          //ttl=150\n" +
                "          //refresh=75\n" +
                "          //from=1725019120\n" +
                "          //to=1725019420\n" +
                "          {\n" +
                "          \"aggs\": {\n" +
                "            \"3\": {\n" +
                "              \"aggs\": {\n" +
                "                \"2\": {\n" +
                "                  \"aggs\": {\n" +
                "                    \"4\": {\n" +
                "                      \"aggs\": {\n" +
                "                        \"5\": {\n" +
                "                          \"aggs\": {\n" +
                "                            \"1\": {\n" +
                "                              \"sum\": {\n" +
                "                                \"field\": \"span_attributes.u\"\n" +
                "                              }\n" +
                "                            }\n" +
                "                          },\n" +
                "                          \"terms\": {\n" +
                "                            \"field\": \"span_attributes.n\", \n" +
                "                            \"size\":9999,\n" +
                "                            \"order\": {\n" +
                "                              \"1\": \"desc\"\n" +
                "                            },\n" +
                "                            \"min_doc_count\": 1\n" +
                "                          }\n" +
                "                        }\n" +
                "                      },\n" +
                "                      \"terms\": {\n" +
                "                        \"field\": \"span_attributes.n\", \n" +
                "                        \"size\":9999,\n" +
                "                        \"order\": {\n" +
                "                          \"_key\": \"desc\"\n" +
                "                        },\n" +
                "                        \"min_doc_count\": 1\n" +
                "                      }\n" +
                "                    }\n" +
                "                  },\n" +
                "                  \"terms\": {\n" +
                "                    \"field\": \"span_attributes.n\", \n" +
                "                    \"size\":9999,\n" +
                "                    \"order\": {\n" +
                "                      \"_key\": \"desc\"\n" +
                "                    },\n" +
                "                    \"min_doc_count\": 1\n" +
                "                  }\n" +
                "                }\n" +
                "              },\n" +
                "              \"date_histogram\": {\n" +
                "                \"field\": \"span_start_timestamp_nanos\",\n" +
                "                \"fixed_interval\": \"1d\",\n" +
                "                \"min_doc_count\": 1\n" +
                "              }\n" +
                "            }\n" +
                "          },\n" +
                "          \"query\": \"*\",\n" +
                "          \"max_hits\": 0,\n" +
                "            \"start_timestamp\": 1725019120,\n" +
                "            \"end_timestamp\": 1725019420 \n" +
                "        }".toLowerCase());

        params.setQuery(replaceAll(params.getQuery(),"|"," "));
        params.setQuery(replaceAll(params.getQuery()," not "," NOT "));
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
                            return  QwUtil.select(q , s[0], s[1]);
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
