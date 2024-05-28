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

package com.facebook.presto.pg;

import com.facebook.presto.influxdb.DBType;
import com.facebook.presto.influxdb.InfluxdbQueryParameters;
import com.facebook.presto.influxdb.InfluxdbRow;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.sql.*;
import java.util.*;

import static com.facebook.presto.influxdb.InfluxdbUtil.*;
import static com.facebook.presto.influxdb.RedisCacheWorker.addOneStat;

public class PGUtil {

    private static Logger logger = LoggerFactory.getLogger(PGUtil.class);
    public static String pgUrl;
    public static String pgUser;
    public static String pgPwd;
    private static DBCPDataSource dbcpDataSource = null;

    public static DBCPDataSource getPool() {
        if (pgUrl == null) {
            return null;
        }
        if (dbcpDataSource == null) {
            dbcpDataSource = new DBCPDataSource(pgUrl, pgUser, pgPwd);

        }
        return dbcpDataSource;
    }

    private PGUtil() {
    }

    public static void instance(String pgUrl, String user, String pwd)
            throws IOException {
        PGUtil.pgUser = user;
        PGUtil.pgPwd = pwd;
        PGUtil.pgUrl = pgUrl;
        getPool();
    }

    public static List<String> getSchemas() throws SQLException, JsonProcessingException {
        logger.debug("getSchemas");
        logger.debug("PGUtil-getSchemas");
        List<String> res = new ArrayList<>();
        List<InfluxdbRow> rows = executeOneQuery("select schema_name from information_schema.schemata");
        for (InfluxdbRow bucket1 : rows) {
            String schemaName = (String) bucket1.getColumnMap().get("schema_name");
            res.add(schemaName);
            logger.debug(schemaName);
        }
        return res;
    }

    public static List<String> getTableNames(String schema) throws SQLException, JsonProcessingException {
        logger.debug("PGUtil- bucket->tableNames:" + schema);
        List<String> res = new ArrayList<>();
        List<InfluxdbRow> rows = executeOneQuery("SELECT table_name\n" +
                "  FROM information_schema.tables\n" +
                " WHERE table_schema='"+ schema +"'\n" +
                "   AND table_type='BASE TABLE'");
        for (InfluxdbRow bucket1 : rows) {
            String schemaName = (String) bucket1.getColumnMap().get("table_name");
            res.add(schemaName);
            logger.debug(schemaName);
        }
        return res;
    }

    public static Iterator<InfluxdbRow> select(String tableName,
                                               boolean forceRefresh) throws IOException, ClassNotFoundException, SQLException {

        InfluxdbQueryParameters influxdbQueryParameters = InfluxdbQueryParameters.getQueryParameters(tableName);
        influxdbQueryParameters.setDbType(DBType.PG);
        return select(influxdbQueryParameters, forceRefresh);
    }

    public static Iterator<InfluxdbRow> select(InfluxdbQueryParameters influxdbQueryParameters,
                                               boolean forceRefresh) throws IOException, ClassNotFoundException, SQLException {
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

                    List<InfluxdbRow> ret =  executeOneQuery(query);

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

    private static List<InfluxdbRow> executeOneQuery(String query) throws SQLException, JsonProcessingException {
        DBCPDataSource dbPool = getPool();
        try (Connection connection = dbPool.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                //logger.debug("select all rows in table: {}", tableName);
                ArrayList<InfluxdbRow> list = new ArrayList<InfluxdbRow>();
                logger.debug("Running: " + query);
                try (ResultSet tables = statement.executeQuery(query)) {
                    ResultSetMetaData rsmd = tables.getMetaData();
                    while (tables.next()) {
                        Map<String, Object> newRow = new HashMap<>();
                        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                            newRow.put(rsmd.getColumnName(i), tables.getString(i));
                        }
                        list.add(new InfluxdbRow(newRow));
                    }
                    return list;
                }
            }
        }
    }
}
