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
import com.facebook.presto.influxdb.InfluxdbQueryParameters;
import com.facebook.presto.influxdb.InfluxdbRow;
import com.facebook.presto.pg.DBCPDataSource;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.opendevl.JFlat;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import com.quickwit.javaclient.ApiClient;
import com.quickwit.javaclient.ApiException;
import com.quickwit.javaclient.Configuration;
import com.quickwit.javaclient.JSON;
import com.quickwit.javaclient.api.IndexesApi;
import com.quickwit.javaclient.api.SearchApi;
import com.quickwit.javaclient.models.SearchRequestQueryString;
import com.quickwit.javaclient.models.SearchResponseRest;
import com.quickwit.javaclient.models.VersionedIndexMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.sql.*;
import java.util.*;

import static com.facebook.presto.influxdb.InfluxdbQueryParameters.replaceAll;
import static com.facebook.presto.influxdb.InfluxdbUtil.*;
import static com.facebook.presto.influxdb.RedisCacheWorker.addOneStat;

public class QwUtil {

    private static Logger logger = LoggerFactory.getLogger(QwUtil.class);
    public static String qwUrl;
    public static String qwIndex;
    private static ApiClient defaultClient = null;


    public static ApiClient getDefaultClient() {
        if (qwUrl == null) {
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

    public static void instance(String qwUrl, String qwIndex) {
        QwUtil.qwUrl = qwUrl;
        QwUtil.qwIndex = qwIndex;
    }

    public static List<String> getSchemas() throws ApiException {
        logger.debug("getSchemas");
        logger.debug("QwUtil-getSchemas");
        List<String> res = new ArrayList<>();
        IndexesApi indexesApi = new IndexesApi(getDefaultClient());
        List<VersionedIndexMetadata> indexesMetadatas = indexesApi.getIndexesMetadatas();

        for (VersionedIndexMetadata bucket1 : indexesMetadatas) {
            String schemaName = bucket1.getVersionedIndexMetadataOneOf().getIndexConfig().getVersionedIndexConfigOneOf().getIndexId();
            res.add(schemaName);
            logger.debug(schemaName);
        }
        return res;
    }

    public static List<String> getTableNames(String schema) throws ApiException {
        logger.debug("getSchemas");
        logger.debug("QwUtil-getSchemas");
        List<String> res = new ArrayList<>();
        IndexesApi indexesApi = new IndexesApi(getDefaultClient());
        List<VersionedIndexMetadata> indexesMetadatas = indexesApi.getIndexesMetadatas();

        for (VersionedIndexMetadata bucket1 : indexesMetadatas) {
            String schemaName = bucket1.getVersionedIndexMetadataOneOf().getIndexConfig().getVersionedIndexConfigOneOf().getIndexId();
            res.add(schemaName);
            logger.debug(schemaName);
        }
        return res;
    }

    public static Iterator<InfluxdbRow> select(String tableName,
                                               boolean forceRefresh) throws IOException,  ApiException {

        InfluxdbQueryParameters influxdbQueryParameters = InfluxdbQueryParameters.getQueryParameters(tableName);
        String q = influxdbQueryParameters.getQuery();
        influxdbQueryParameters.setQuery(replaceAll(q,"\\|"," "));
        influxdbQueryParameters.setDbType(DBType.QW);
        return select(influxdbQueryParameters, forceRefresh);
    }

    public static Iterator<InfluxdbRow> select(InfluxdbQueryParameters influxdbQueryParameters,
                                               boolean forceRefresh) throws IOException,  ApiException {
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

                    List<InfluxdbRow> ret = executeOneQuery(query);

                    if (jedis != null) {
                        influxdbQueryParameters.setRows(ret);
                        influxdbQueryParameters.setFinish(System.currentTimeMillis());
                        setCacheItem(jedis, influxdbQueryParameters);
                    }
                    addOneStat(hash, 1);
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

    private static List<InfluxdbRow> executeOneQuery(String query) throws ApiException {

        SearchApi searchApi = new SearchApi(getDefaultClient());

        SearchRequestQueryString toQuery = getGson().fromJson(query, SearchRequestQueryString.class);
        logger.debug("Running: {}", query);
        SearchResponseRest ret = searchApi.searchPostHandler(qwIndex, toQuery);
        return parseResponse(ret);
    }

    public static Gson getGson() {
        GsonBuilder gsonBuilder = new GsonBuilder().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE);
        Gson gson = gsonBuilder.create();
        return gson;
    }

    public static List<InfluxdbRow> parseResponse(SearchResponseRest ret) {
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
            for (int j = 0; j < headers.length; j++) {
                Object val = c[j];
                if (val == null && i + 1 < flatted.size()) {
                    val = flatted.get(i + 1)[j];
                }
                r.put((String) headers[j], String.valueOf(val));
            }
            toRet.add(new InfluxdbRow(r));
        }
        return toRet;
    }
}
