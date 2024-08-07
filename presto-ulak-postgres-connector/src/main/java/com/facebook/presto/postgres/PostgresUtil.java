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

import com.facebook.presto.ulak.caching.QueryParameters;
import com.facebook.presto.ulak.UlakRow;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.quickwit.javaclient.ApiException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.VarcharType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.*;

//import static com.facebook.presto.influxdb.RedisCacheWorker.addOneStat;
public class PostgresUtil {
    private static Logger logger = LoggerFactory.getLogger(PostgresUtil.class);


    private PostgresUtil() {
    }

    public static void instance(String url, String org, String token, String bucket)
            throws IOException {
    }

    public static List<ColumnMetadata> getColumns(String bucket, String tableName) throws IOException, ClassNotFoundException {
        logger.debug("influxdbUtil bucket:" + bucket + "table:" + tableName + " columnsMetadata");
        List<ColumnMetadata> res = new ArrayList<>();

        Iterator<UlakRow> tables = null;
        try {
            tables = PostgresUtil.select(tableName);
        } catch (IOException e) {
            logger.error("IOException", e);
        } catch (ClassNotFoundException e) {
            logger.error("ClassNotFoundException", e);
        } catch (SQLException e) {
            logger.error("SQLException", e);
        } catch (ApiException e) {
            logger.error("ApiException", e);
        }

        if (tables.hasNext()) {
            for (Iterator<UlakRow> it = tables; it.hasNext(); ) {
                UlakRow fluxTable = it.next();
                Map<String, Object> records = fluxTable.getColumnMap();
                for (String record : records.keySet()) {
                    if (!res.stream().anyMatch(t -> t.getName().equals(record))) {
                        res.add(new ColumnMetadata(record, VarcharType.VARCHAR));
                    }
                }
            }
        } else {
            String[] cols = QueryParameters.getQueryParameters(tableName).getColumns();
            if (cols.length > 0) {
                for (String record : cols) {
                    if (!res.stream().anyMatch(t -> t.getName().equals(record))) {
                        res.add(new ColumnMetadata(record, VarcharType.VARCHAR));
                    }
                }
            }
        }
        for (ColumnMetadata columnMetadata : res) {
            logger.debug(columnMetadata.getName() + ":" + columnMetadata.getType().getDisplayName());
        }
        return res;
    }

     public static Iterator<UlakRow> select(String tableName ) throws IOException, ClassNotFoundException, SQLException, ApiException  {

        QueryParameters influxdbQueryParameters = QueryParameters.getQueryParameters(tableName);
        return select(influxdbQueryParameters );
    }
    public static Iterator<UlakRow> select(QueryParameters influxdbQueryParameters ) throws IOException, ClassNotFoundException, SQLException, ApiException  {
        return PGUtil.select(influxdbQueryParameters);
    }

}
