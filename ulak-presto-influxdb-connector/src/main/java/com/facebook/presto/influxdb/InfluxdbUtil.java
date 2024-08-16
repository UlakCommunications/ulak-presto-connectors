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

package com.facebook.presto.influxdb;

import com.facebook.presto.ulak.UlakRow;
import com.facebook.presto.ulak.QueryParameters;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.influxdb.client.BucketsApi;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.QueryApi;
import com.influxdb.client.domain.Bucket;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import com.quickwit.javaclient.ApiException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.VarcharType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

public class InfluxdbUtil {

    private static Logger logger = LoggerFactory.getLogger(InfluxdbUtil.class);
    private static InfluxDBClient influxDBClient;

    public static void instance(String url, String org, String token)
            throws
            IOException {
        influxDBClient = InfluxDBClientFactory.create(url, token.toCharArray(), org);
    }

    public static List<String> getSchemas() {
        logger.debug("getSchemas");
        logger.debug("influxdbUtil-getSchemas");
        List<String> res = new ArrayList<>();
        BucketsApi bucketsApi = influxDBClient.getBucketsApi();
        for (Bucket bucket1 : bucketsApi.findBuckets()) {
            res.add(bucket1.getName());
            logger.debug(bucket1.getName());
        }
        return res;
    }

    public static List<String> getTableNames(String bucket) {
        logger.debug("influxdbUtil- bucket->tableNames: {}", bucket);
        List<String> res = new ArrayList<>();
        QueryApi queryApi = influxDBClient.getQueryApi();
        String flux = "import  \"influxdata/influxdb/schema\"\n" + "import \"strings\"\n" + "schema.measurements(bucket: \"" + bucket + "\")\n" + "|> filter(fn : (r) => not strings.hasPrefix(v: r._value, prefix: \"task\"))\n" + "|> filter(fn : (r) => not strings.hasPrefix(v: r._value, prefix: \"storage\"))\n" + "|> filter(fn : (r) => not strings.hasPrefix(v: r._value, prefix: \"service\"))\n" + "|> filter(fn : (r) => not strings.hasPrefix(v: r._value, prefix: \"query\"))\n" + "|> filter(fn : (r) => not strings.hasPrefix(v: r._value, prefix: \"qc\"))\n" + "|> filter(fn : (r) => not strings.hasPrefix(v: r._value, prefix: \"influxdb\"))\n" + "|> filter(fn : (r) => not strings.hasPrefix(v: r._value, prefix: \"http\"))\n" + "|> filter(fn : (r) => not strings.hasPrefix(v: r._value, prefix: \"go\"))\n" + "|> filter(fn : (r) => not strings.hasPrefix(v: r._value, prefix: \"boltdb\"))";
        List<FluxTable> tables = queryApi.query(flux);
        for (FluxTable fluxTable : tables) {
            List<FluxRecord> records = fluxTable.getRecords();
            for (FluxRecord fluxRecord : records) {
                res.add((String) fluxRecord.getValue());
                logger.debug("{}",fluxRecord.getValue());
            }
        }
        return res;
    }

    public static List<ColumnMetadata> columnMetadataAdder (List<UlakRow> tables) {
        List<ColumnMetadata> res = new ArrayList<>();
        for (UlakRow fluxTable : tables) {
            Map<String, Object> records = fluxTable.getColumnMap();
            for (String rec : records.keySet()) {
                if (res.stream().noneMatch(t -> t.getName().equals(rec))) {
                    res.add(new ColumnMetadata(rec, VarcharType.VARCHAR));
                }
            }
        }
        return res;
    }


    public static List<ColumnMetadata> getColumns(String bucket, String tableName) throws Exception {
        logger.debug("influxdbUtil bucket: {} table: {} columnsMetadata",bucket, tableName);
        List<ColumnMetadata> res;
        List<UlakRow> tables = null;
        try {
            tables = InfluxdbUtil.exec(tableName);
        } catch (IOException e) {
            logger.error("IOException", e);
        } catch (ClassNotFoundException e) {
            logger.error("ClassNotFoundException", e);
        } catch (SQLException e) {
            logger.error("SQLException", e);
        } catch (ApiException e) {
            logger.error("ApiException", e);
        }

        if (tables!=null) {
            res = columnMetadataAdder(tables);
        } else {
            throw new Exception("Empty Query");
        }
        for (ColumnMetadata columnMetadata : res) {
            logger.debug("{}:{}", columnMetadata.getName(), columnMetadata.getType().getDisplayName());
        }
        return res;
    }

    public static List<UlakRow> exec(String tableName) throws IOException, ClassNotFoundException, SQLException, ApiException {
        QueryParameters influxdbQueryParameters = QueryParameters.getQueryParameters(tableName);
        return exec(influxdbQueryParameters);
    }

    public static List<UlakRow> exec(QueryParameters influxdbQueryParameters) throws IOException, ClassNotFoundException, SQLException, ApiException {
        influxdbQueryParameters.setError("");
        ArrayList<UlakRow> list = new ArrayList<>();
        QueryApi queryApi = influxDBClient.getQueryApi();
        String flux = influxdbQueryParameters.getQuery();
        List<FluxTable> tables = queryApi.query(flux);
        List<Map<String, Object>> resMap = new LinkedList<>();
        for (FluxTable fluxTable : tables) {
            List<FluxRecord> records = fluxTable.getRecords();
            for (FluxRecord fluxRecord : records) {
                Map<String, Object> curRow = fluxRecord.getValues();
                Map<String, Object> newRow = new HashMap<>();
                for (Map.Entry<String, Object> entry : curRow.entrySet()) {
                    newRow.put(entry.getKey(), entry.getValue());
                }
                resMap.add(newRow);
            }
        }
        for (Map<String, Object> entry : resMap) {
            list.add(new UlakRow(entry));
        }
        return list;

    }
}
