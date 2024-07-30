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

import com.facebook.presto.influxdb.DBType;
import com.facebook.presto.influxdb.InfluxdbConnector;
import com.facebook.presto.influxdb.InfluxdbQueryParameters;
import com.facebook.presto.influxdb.InfluxdbRow;
import com.github.opendevl.JFlat;
import com.google.common.base.Strings;
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
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.util.*;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.Scriptable;

import static com.facebook.presto.influxdb.InfluxdbQueryParameters.replaceAll;
import static com.facebook.presto.influxdb.InfluxdbUtil.*;
//import static com.facebook.presto.influxdb.RedisCacheWorker.addOneStat;

public class QwUtil {

    private static Logger logger = LoggerFactory.getLogger(QwUtil.class);

    private static ApiClient defaultClient = null;

    public static final String DOC_COUNT = "doc_count";
    public static final String KEY = "key";
    public static final String KEY_AS_STRING = "key_as_string";
    public static final String SUM_OTHER_DOC_COUNT = "sum_other_doc_count";
    public static final String VALUE = "value";
    public static final String BUCKETS = "buckets";

    public static ApiClient getDefaultClient(InfluxdbConnector c,
                                             InfluxdbQueryParameters influxdbQueryParameters) {
        String qwUrl = influxdbQueryParameters!=null?influxdbQueryParameters.getQwUrl():null;
        if(StringUtils.isBlank(qwUrl))
        {
            qwUrl = c!=null?c.getQwUrl():null;
        }
        if (StringUtils.isBlank(qwUrl) ) {
            return null;
        }
        if (defaultClient == null) {
            defaultClient = Configuration.getDefaultApiClient();
            defaultClient.setBasePath(qwUrl);

        }
        return defaultClient;
    }

    private QwUtil() {
    }

//    public static void instance(InfluxdbConnector c, String qwUrl, String qwIndex) {
//        c.qwUrl = qwUrl;
//        c.qwIndex = qwIndex;
//    }

    public static List<String> getSchemas(InfluxdbConnector c) throws ApiException {
        logger.debug("getSchemas");
        logger.debug("QwUtil-getSchemas");
        List<String> res = new ArrayList<>();
        IndexesApi indexesApi = new IndexesApi(getDefaultClient(c,null));
        List<VersionedIndexMetadata> indexesMetadatas = indexesApi.getIndexesMetadatas();

        for (VersionedIndexMetadata bucket1 : indexesMetadatas) {
            String schemaName = bucket1.getVersionedIndexMetadataOneOf().getIndexConfig().getVersionedIndexConfigOneOf().getIndexId();
            res.add(schemaName);
            logger.debug(schemaName);
        }
        return res;
    }

    public static List<String> getTableNames(InfluxdbConnector c,String schema) throws ApiException {
        logger.debug("getSchemas");
        logger.debug("QwUtil-getSchemas");
        List<String> res = new ArrayList<>();
        IndexesApi indexesApi = new IndexesApi(getDefaultClient(c,null));
        List<VersionedIndexMetadata> indexesMetadatas = indexesApi.getIndexesMetadatas();

        for (VersionedIndexMetadata bucket1 : indexesMetadatas) {
            String schemaName = bucket1.getVersionedIndexMetadataOneOf().getIndexConfig().getVersionedIndexConfigOneOf().getIndexId();
            res.add(schemaName);
            logger.debug(schemaName);
        }
        return res;
    }
//
//    public static Iterator<InfluxdbRow> select(InfluxdbConnector c,String tableName,
//                                               boolean forceRefresh) throws IOException,  ApiException {
//
//        InfluxdbQueryParameters influxdbQueryParameters = InfluxdbQueryParameters.getQueryParameters(c,tableName);
//        return select(c,influxdbQueryParameters, forceRefresh);
//    }

    public static Iterator<InfluxdbRow> select(InfluxdbConnector c,
                                               InfluxdbQueryParameters influxdbQueryParameters,
                                               boolean forceRefresh) throws IOException, ApiException {
        String q = influxdbQueryParameters.getQuery();
        influxdbQueryParameters.setQuery(replaceAll(q,"|"," "));
        influxdbQueryParameters.setQuery(replaceAll(q," not "," NOT "));
        influxdbQueryParameters.setDbType(DBType.QW);
//        influxdbQueryParameters.setQwUrl(c.qwUrl);
//        influxdbQueryParameters.setQwIndex(c.qwIndex);

        int hash = influxdbQueryParameters.getHash();
        influxdbQueryParameters.setStart(System.currentTimeMillis());

        JedisPool pool = getJedisPool();
        Jedis jedis = null;
        if (pool != null) {
            jedis = pool.getResource();
        }
        try {
            List<InfluxdbRow> fromCache = getCacheResultAsList(forceRefresh, jedis, hash);
            if (fromCache != null) {
                influxdbQueryParameters.setRows(fromCache);
                setCacheItem(jedis, influxdbQueryParameters);
                return fromCache.iterator();
            }
            synchronized (inProgressLock) {
                fromCache = getCacheResultAsList(forceRefresh, jedis, hash);
                if (fromCache != null) {
                    influxdbQueryParameters.setRows(fromCache);
                    setCacheItem(jedis, influxdbQueryParameters);
                    return fromCache.iterator();
                }
                if (!inProgressLocks.containsKey(hash)) {
                    inProgressLocks.put(hash, hash);
                }
            }
            try {
                synchronized (inProgressLocks.get(hash)) {

                    fromCache = getCacheResultAsList(forceRefresh, jedis, hash);
                    if (fromCache != null) {
                        influxdbQueryParameters.setRows(fromCache);
                        setCacheItem(jedis, influxdbQueryParameters);
                        return fromCache.iterator();
                    } else {
                        if (influxdbQueryParameters.isEagerCached() && !forceRefresh) {
                            LinkedList<InfluxdbRow> rows = new LinkedList<>();
                            influxdbQueryParameters.setRows(rows);
                            setCacheItem(jedis, influxdbQueryParameters);
                            return rows.iterator();
                        }
                    }
                    influxdbQueryParameters.setError("");
                    String query = influxdbQueryParameters.getQuery();//"from(bucket: " + "\"" + bucket + "\"" + ")\n" + "|> range(start:" + time_interval + ")\n" + "|> filter(fn : (r) => r._measurement == " + "\"" + tableName + "\"" + ")";

                    List<InfluxdbRow> ret = executeOneQuery( c,influxdbQueryParameters,influxdbQueryParameters.getQwIndex(),query);

                    if (jedis != null) {
                        influxdbQueryParameters.setRows(ret);
                        influxdbQueryParameters.setFinish(System.currentTimeMillis());
                        setCacheItem(jedis, influxdbQueryParameters);
                    }
//                    addOneStat(hash, 1);
                    return ret.iterator();
                }
            } finally {
                synchronized (inProgressLock) {
                    if (inProgressLocks.containsKey(hash)) {
                        inProgressLocks.remove(hash);
                    }
                }
            }
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
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
            logger.debug(Context.toString(result));
            return (String) result;
        } finally {
            // Exit from the context.
            Context.exit();
        }
    }
    public static List<InfluxdbRow> executeOneQuery(InfluxdbConnector c,
                                                     InfluxdbQueryParameters influxdbQueryParameters,
                                                     String qwIndex,
                                                     String query) throws ApiException {

        query=replaceAll(query,"|"," ");
        query=replaceAll(query," not "," NOT ");
        long unixTime = System.currentTimeMillis() / 1000L;
//        query=replaceAll(query,"now()", String.valueOf(unixTime));
//        ScriptEngineManager scriptEngineManager = new ScriptEngineManager();

//        ScriptEngineManager sem = new ScriptEngineManager();
//        List<ScriptEngineFactory> factories = sem.getEngineFactories();
//        for (ScriptEngineFactory factory : factories)
//            logger.debug(factory.getEngineName() + " " + factory.getEngineVersion() + " " + factory.getNames());
//        if (factories.isEmpty())
//            logger.debug("No Script Engines found");

//        ScriptEngine scriptEngine = scriptEngineManager.getEngineByName("JavaScript");
//        try {
        if(influxdbQueryParameters.isHasJs()) {
            query =   executeScript(
                    "var now = " + unixTime + ";" +
                            "var d = 24*60*60 /*number of seconds in a day*/;" +
                            "var h = 60*60 /*number of seconds in an hour*/;" +
                            "var m = 60 /*number of seconds in a minute*/;" +
                            "var s = 1 /*number of seconds in a second*/;" +
                            "var a = " + query + ";" +
                            "JSON.stringify(a);");
        }
//        } catch (ScriptException e) {
//            logger.error("No Script Engines found", e);
//            throw e;
//        }


        SearchApi searchApi = new SearchApi(getDefaultClient(c,influxdbQueryParameters));

        SearchRequestQueryString toQuery = getGson().fromJson(query, SearchRequestQueryString.class);
        logger.debug("Running on {}/{}: {}", influxdbQueryParameters.getQwUrl(), qwIndex, query);
        SearchResponseRest ret = searchApi.searchPostHandler(qwIndex, toQuery);
        List<InfluxdbRow> parsed = parseResponse(influxdbQueryParameters,ret);
        //parseColumns( influxdbQueryParameters, parsed);
        return parsed;
    }
//    private static void parseColumns(SearchRequestQueryString toQuery, List<InfluxdbRow> parsed) {
//        Map<String,Object> aggs = (Map<String, Object>) toQuery.getAggs();
//
//        for (InfluxdbRow row : parsed) {
//            for (Map.Entry<String, Object> c:row.getColumnMap().entrySet()){
//                String col = c.getKey();
//                String[] splits = col.split("/");
//                Map<String,Object> currAgg = aggs;
//                for (int i = 0; i < splits.length; i++) {
//                    String split = splits[i];
//                    if(StringUtils.isNotBlank(split)) {
//                        if (split.equals("buckets")) {
//                            currAgg = (Map<String, Object>) currAgg.get("aggs");
//                        }else{
//                            currAgg = (Map<String, Object>) currAgg.get(split);
//                        }
//                    }
//                }
//            }
//        }
//    }

    public static Gson getGson() {
        GsonBuilder gsonBuilder = new GsonBuilder().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE);
        Gson gson = gsonBuilder.create();
        return gson;
    }

    public static List<InfluxdbRow> parseResponse(InfluxdbQueryParameters influxdbQueryParameters,
                                                  SearchResponseRest ret) {
        Object g = ret.getAggregations();
        if (g == null) {
            return parseResponseHits(influxdbQueryParameters,ret);
        }
        parseResponseAggregations(ret);
        return parseResponseHits(influxdbQueryParameters, ret);
    }
    public static List<InfluxdbRow> parseResponseAggregations(SearchResponseRest ret) {
        Object g = ret.getAggregations();
        if (g == null) {
            return null;
        }

        arrangeAggregations((Map<String,Object>)g,null);
        return null;
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
//                    if(bucket.containsKey(BUCKETS) || bucket.containsKey(VALUE)) {
                    arrangeAggregations(bucket, currentValues);
//                    }
                }
            }
        }
        if(buckets==null && value==null&aggValue.size()==0){
            aggValue.putAll(currentValues);
            return null;
        }
        return currentValues;
    }
    public static List<InfluxdbRow> parseResponseHits(InfluxdbQueryParameters influxdbQueryParameters,
                                                      SearchResponseRest ret) {
        Object g = ret.getAggregations();
        if (g == null) {
            g = ret.getHits();
        }
        JFlat flatMe = new JFlat(getGson().toJson(g));
        List<Object[]> flatted = flatMe.json2Sheet().getJsonAsSheet();
        Map<String, Integer> headerIndexes = new HashMap<>();
        Object[] headers = flatted.get(0);
        for (int i = 0; i < headers.length; i++) {
            headerIndexes.put((String) headers[i], i);
        }
        List<InfluxdbRow> toRet = new ArrayList<>();
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
                String toReplace =influxdbQueryParameters.getReplaceFromColumns();
                if(StringUtils.isNotBlank(toReplace)){
                    k=StringUtils.replace((String) k, toReplace,"");
                }
                if(value!=null){
                    allNulls=false;
                }
                r.put(k, value);
            }
            if(!allNulls) {
                toRet.add(new InfluxdbRow(r));
            };
        }
        return toRet;
    }
}
