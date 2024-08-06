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
import com.facebook.presto.ulak.caching.QueryParameters;
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
    public static boolean isCoordinator;
    public static String workerId;
    public static String workerIndexToRunIn;
    private final static Map<String, String> const_keywords = new HashMap<String, String>() {
        {
            put("aggregatewindow", "aggregateWindow");
            put("createempty", "createEmpty");
            put("columnkey", "columnKey");
            put("nonnegative", "nonNegative");
            put("rowkey", "rowKey");
            put("useprevious", "usePrevious");
            put("valuecolumn", "valueColumn");
            put("windowperiod", "windowPeriod");
            put("timesrc", "timeSrc");
            put("tolower", "toLower");
            put("toupper", "toUpper");
            put("\\:in \\[", "\\:IN \\[");
            put(" and ", " AND ");
        }
    };
    private static Map<String, String> keywords = new LinkedHashMap<>(const_keywords);

    public static String redisUrl = null;
    private static String token = "zNBXClD-3rbf82GiGNGNxx0lZsJeJ3RCc7ViONhffoKfp5tfbv1UtLGFFcw7IU9i4ebllttDWzaD3899LaRQKg==";
    private static String org = "ulak";
    private static String bucket = "collectd";
    private static final String time_interval = "-5m";
    private static Logger logger = LoggerFactory.getLogger(InfluxdbUtil.class);
    private static InfluxDBClient influxDBClient;
    private static ObjectMapper objectMapper = null;

    private InfluxdbUtil() {
    }

    public static void instance(String url, String org, String token, String bucket)
            throws IOException {
        InfluxdbUtil.org = org;
        InfluxdbUtil.token = token;
        InfluxdbUtil.bucket = bucket;
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
        logger.debug("influxdbUtil- bucket->tableNames:" + bucket);
        //logger.debug("getTableNames in bucket: {}", bucket);
        List<String> res = new ArrayList<>();
        QueryApi queryApi = influxDBClient.getQueryApi();
        String flux = "import  \"influxdata/influxdb/schema\"\n" + "import \"strings\"\n" + "schema.measurements(bucket: \"" + bucket + "\")\n" + "|> filter(fn : (r) => not strings.hasPrefix(v: r._value, prefix: \"task\"))\n" + "|> filter(fn : (r) => not strings.hasPrefix(v: r._value, prefix: \"storage\"))\n" + "|> filter(fn : (r) => not strings.hasPrefix(v: r._value, prefix: \"service\"))\n" + "|> filter(fn : (r) => not strings.hasPrefix(v: r._value, prefix: \"query\"))\n" + "|> filter(fn : (r) => not strings.hasPrefix(v: r._value, prefix: \"qc\"))\n" + "|> filter(fn : (r) => not strings.hasPrefix(v: r._value, prefix: \"influxdb\"))\n" + "|> filter(fn : (r) => not strings.hasPrefix(v: r._value, prefix: \"http\"))\n" + "|> filter(fn : (r) => not strings.hasPrefix(v: r._value, prefix: \"go\"))\n" + "|> filter(fn : (r) => not strings.hasPrefix(v: r._value, prefix: \"boltdb\"))";
        List<FluxTable> tables = queryApi.query(flux);
        for (FluxTable fluxTable : tables) {
            List<FluxRecord> records = fluxTable.getRecords();
            for (FluxRecord fluxRecord : records) {
                res.add((String) fluxRecord.getValue());
                logger.debug(String.valueOf(fluxRecord.getValue()));
            }
        }
        return res;
    }

    public static String arrangeCase(String query) {
        Map<String, String> ktr = keywords;
        if (ktr == null || ktr.size() == 0) {
            ktr = const_keywords;
        }
        for (Map.Entry<String, String> kv : ktr.entrySet()) {
            query = query.replaceAll(kv.getKey(), kv.getValue());
            logger.debug("Replacing keyword :" + kv.getKey() + " with " + kv.getValue() + " : Resulting in :" + query);
        }
        return query;
    }

    public static List<ColumnMetadata> getColumns(String bucket, String tableName) throws IOException, ClassNotFoundException {
        logger.debug("influxdbUtil bucket:" + bucket + "table:" + tableName + " columnsMetadata");
        List<ColumnMetadata> res = new ArrayList<>();

        Iterator<UlakRow> tables = null;
//        switch (dbType){
//            case  INFLUXDB2:
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
//            case  PG:
//                try {
//                    tables = PGUtil.select(tableName,false);;
//                } catch (IOException e) {
//                    logger.error("IOException", e);
//                } catch (ClassNotFoundException e) {
//                    logger.error("ClassNotFoundException", e);
//                } catch (SQLException e) {
//                    logger.error("ClassNotFoundException", e);
//                    throw new RuntimeException(e);
//                }
//        }


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
//        // all tags
//        flux = "import \"influxdata/influxdb/schema\"\n" + "schema.measurementTagKeys(\n"
//                + "    bucket : \"" + bucket + "\",\n" + "    measurement : \"" + tableName + "\",\n" + "    start : " + time_interval + ")";
//        tables = queryApi.query(flux);
//        for (FluxTable fluxTable : tables) {
//            List<FluxRecord> records = fluxTable.getRecords();
//            for (FluxRecord record : records) {
//                if (!((String) record.getValue()).startsWith("_")) {
//                    res.add(new ColumnMetadata((String) record.getValue(), VarcharType.VARCHAR));
//                }
//            }
//        }
//        res.add(new ColumnMetadata("_time", VarcharType.VARCHAR));
        for (ColumnMetadata columnMetadata : res) {
            logger.debug(columnMetadata.getName() + ":" + columnMetadata.getType().getDisplayName());
        }
        return res;
    }

    public static Iterator<UlakRow> exec(String tableName) throws IOException, ClassNotFoundException, SQLException, ApiException {
        QueryParameters influxdbQueryParameters = QueryParameters.getQueryParameters(tableName);
        return exec(influxdbQueryParameters);
    }

    public static Iterator<UlakRow> exec(QueryParameters influxdbQueryParameters) throws IOException, ClassNotFoundException, SQLException, ApiException {
        try {
            influxdbQueryParameters.setError("");
            ArrayList<UlakRow> list = new ArrayList<UlakRow>();
            QueryApi queryApi = influxDBClient.getQueryApi();
            String flux = influxdbQueryParameters.getQuery();
            List<FluxTable> tables = queryApi.query(flux, org);
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
            return list.iterator();
        } finally {

        }
    }
}
