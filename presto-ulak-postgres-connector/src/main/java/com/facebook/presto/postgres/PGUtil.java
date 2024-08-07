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

import com.facebook.presto.ulak.caching.DBType;
import com.facebook.presto.ulak.caching.QueryParameters;
import com.facebook.presto.ulak.UlakRow;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.*;
//import static com.facebook.presto.influxdb.RedisCacheWorker.addOneStat;

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
        List<UlakRow> rows = executeOneQuery("select schema_name from information_schema.schemata");
        for (UlakRow bucket1 : rows) {
            String schemaName = (String) bucket1.getColumnMap().get("schema_name");
            res.add(schemaName);
            logger.debug(schemaName);
        }
        return res;
    }

    public static List<String> getTableNames(String schema) throws SQLException, JsonProcessingException {
        logger.debug("PGUtil- bucket->tableNames:" + schema);
        List<String> res = new ArrayList<>();
        List<UlakRow> rows = executeOneQuery("SELECT table_name\n" +
                "  FROM information_schema.tables\n" +
                " WHERE table_schema='"+ schema +"'\n" +
                "   AND table_type='BASE TABLE'");
        for (UlakRow bucket1 : rows) {
            String schemaName = (String) bucket1.getColumnMap().get("table_name");
            res.add(schemaName);
            logger.debug(schemaName);
        }
        return res;
    }

    public static List<UlakRow> select(String tableName ) throws IOException, SQLException {
        QueryParameters influxdbQueryParameters = QueryParameters.getQueryParameters(tableName);
        return select(influxdbQueryParameters);
    }

    public static List<UlakRow> select(QueryParameters influxdbQueryParameters) throws IOException, SQLException {
        influxdbQueryParameters.setDbType(DBType.PG);
        influxdbQueryParameters.setStart(System.currentTimeMillis());

        influxdbQueryParameters.setError("");
        String query = influxdbQueryParameters.getQuery();//"from(bucket: " + "\"" + bucket + "\"" + ")\n" + "|> range(start:" + time_interval + ")\n" + "|> filter(fn : (r) => r._measurement == " + "\"" + tableName + "\"" + ")";

        List<UlakRow> ret = executeOneQuery(query);
//                    addOneStat(hash, 1);
        return ret ;

    }

    private static List<UlakRow> executeOneQuery(String query) throws SQLException, JsonProcessingException {
        DBCPDataSource dbPool = getPool();
        try (Connection connection = dbPool.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                //logger.debug("select all rows in table: {}", tableName);
                ArrayList<UlakRow> list = new ArrayList<UlakRow>();
                logger.debug("Running: " + query);
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
        }
    }
}
