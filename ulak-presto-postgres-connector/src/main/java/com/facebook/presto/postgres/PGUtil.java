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

package com.facebook.presto.postgres;

import com.facebook.presto.ulak.DBType;
import com.facebook.presto.ulak.QueryParameters;
import com.facebook.presto.ulak.UlakRow;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.dbcp.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.*;

public class PGUtil {

    private static Logger logger = LoggerFactory.getLogger(PGUtil.class);
    private static Map<String, BasicDataSource> dbcpDataSources = null;

    public static BasicDataSource getPool(String pgUrl,String pgUser,String pgPwd) {
        if (pgUrl == null) {
            return null;
        }
        if (dbcpDataSources == null) {
            dbcpDataSources =  new LinkedHashMap<>();

        }
        BasicDataSource  dbcpDataSource = null;
        if (!dbcpDataSources.containsKey(pgUrl)) {
            dbcpDataSource = new BasicDataSource();
            dbcpDataSource.setUrl(pgUrl);
            dbcpDataSource.setUsername(pgUser);
            dbcpDataSource.setPassword(pgPwd);
            dbcpDataSource.setMinIdle(20);
            dbcpDataSource.setMaxIdle(30);
            dbcpDataSource.setMaxOpenPreparedStatements(128);
            dbcpDataSources.put(pgUrl, dbcpDataSource);
        }else {
            dbcpDataSource = dbcpDataSources.get(pgUrl);
        }
        return dbcpDataSource;
    }

    private PGUtil() {
    }

    public static List<String> getSchemas(String pgUrl, String pgUser, String pgPwd) throws SQLException, JsonProcessingException {
        logger.debug("getSchemas");
        logger.debug("PGUtil-getSchemas");
        List<String> res = new ArrayList<>();
        List<UlakRow> rows = executeOneQuery("select schema_name from information_schema.schemata",  pgUrl,   pgUser,   pgPwd);
        for (UlakRow bucket1 : rows) {
            String schemaName = (String) bucket1.getColumnMap().get("schema_name");
            res.add(schemaName);
            logger.debug(schemaName);
        }
        return res;
    }

    public static List<String> getTableNames(String schema,String   pgUrl,   String  pgUser,   String  pgPwd) throws SQLException, JsonProcessingException {
        logger.debug("PGUtil- bucket->tableNames: {}", schema);
        List<String> res = new ArrayList<>();
        List<UlakRow> rows = executeOneQuery("SELECT table_name\n" +
                "  FROM information_schema.tables\n" +
                " WHERE table_schema='"+ schema +"'\n" +
                "   AND table_type='BASE TABLE'",  pgUrl,   pgUser,   pgPwd);
        for (UlakRow bucket1 : rows) {
            String schemaName = (String) bucket1.getColumnMap().get("table_name");
            res.add(schemaName);
            logger.debug(schemaName);
        }
        return res;
    }


    public static List<UlakRow> select(String tableName ,String   pgUrl,   String  pgUser,   String  pgPwd) throws IOException, SQLException {
        QueryParameters influxdbQueryParameters = QueryParameters.getQueryParameters(tableName);
        return select(influxdbQueryParameters,  pgUrl,   pgUser,   pgPwd);
    }

    public static List<UlakRow> select(QueryParameters influxdbQueryParameters,String   pgUrl,   String  pgUser,   String  pgPwd) throws IOException, SQLException {
        influxdbQueryParameters.setDbType(DBType.PG);
        influxdbQueryParameters.setStart(System.currentTimeMillis());

        influxdbQueryParameters.setError("");
        String query = influxdbQueryParameters.getQuery();//"from(bucket: " + "\"" + bucket + "\"" + ")\n" + "|> range(start:" + time_interval + ")\n" + "|> filter(fn : (r) => r._measurement == " + "\"" + tableName + "\"" + ")";

        List<UlakRow> ret = executeOneQuery(query,  pgUrl,   pgUser,   pgPwd);
//                    addOneStat(hash, 1);
        return ret ;

    }

    private static List<UlakRow> executeOneQuery(String query,String pgUrl, String pgUser, String pgPwd) throws SQLException, JsonProcessingException {
        BasicDataSource dbPool = getPool(pgUrl,   pgUser,   pgPwd);
        try (Connection connection = dbPool.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                ArrayList<UlakRow> list = new ArrayList<>();
                logger.debug("Running: {}",query);
                try (ResultSet tables = statement.executeQuery(query)) {
                    ResultSetMetaData rsmd = tables.getMetaData();
                    while (tables.next()) {
                        Map<String, Object> newRow = new HashMap<>();
                        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                            newRow.put(rsmd.getColumnName(i), tables.getString(i));
                        }
                        list.add(new UlakRow(newRow));
                    }
                    return list;
                }
            }
        } catch (NullPointerException e) {
            throw new NullPointerException(e.toString());
        }
    }
}
