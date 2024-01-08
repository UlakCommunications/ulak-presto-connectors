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

import com.influxdb.client.BucketsApi;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.QueryApi;
import com.influxdb.client.domain.Bucket;
import com.influxdb.query.FluxColumn;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.VarcharType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InfluxdbUtil
{
    private static  String token = "zNBXClD-3rbf82GiGNGNxx0lZsJeJ3RCc7ViONhffoKfp5tfbv1UtLGFFcw7IU9i4ebllttDWzaD3899LaRQKg==";
    private static  String org = "ulak";
    private static  String bucket = "collectd";
    private static final String time_interval = "-5m";
    private static Logger logger = LoggerFactory.getLogger(InfluxdbUtil.class);
    private static InfluxDBClient influxDBClient;

    private InfluxdbUtil()
    {
    }

    public static void instance(String url, String org, String token, String bucket)
            throws IOException
    {
        InfluxdbUtil.org = org;
        InfluxdbUtil.token = token;
        InfluxdbUtil.bucket = bucket;
        influxDBClient = InfluxDBClientFactory.create(url, token.toCharArray(), org);
    }

    public static List<String> getSchemas()
    {
        //logger.debug("getSchemas");
        System.out.println("influxdbUtil-获取schemas");
        List<String> res = new ArrayList<>();
        BucketsApi bucketsApi = influxDBClient.getBucketsApi();
        for (Bucket bucket1 : bucketsApi.findBuckets()) {
            res.add(bucket1.getName());
            //System.out.println(bucket1.getName());
        }
        return res;
    }

    public static List<String> getTableNames(String bucket)
    {
        System.out.println("influxdbUtil- 获取指定bucket中的所有tableNames" + bucket);
        //logger.debug("getTableNames in bucket: {}", bucket);
        List<String> res = new ArrayList<>();
        QueryApi queryApi = influxDBClient.getQueryApi();
        String flux = "import  \"influxdata/influxdb/schema\"\n" + "import \"strings\"\n" + "schema.measurements(bucket: \"" + bucket + "\")\n" + "|> filter(fn : (r) => not strings.hasPrefix(v: r._value, prefix: \"task\"))\n" + "|> filter(fn : (r) => not strings.hasPrefix(v: r._value, prefix: \"storage\"))\n" + "|> filter(fn : (r) => not strings.hasPrefix(v: r._value, prefix: \"service\"))\n" + "|> filter(fn : (r) => not strings.hasPrefix(v: r._value, prefix: \"query\"))\n" + "|> filter(fn : (r) => not strings.hasPrefix(v: r._value, prefix: \"qc\"))\n" + "|> filter(fn : (r) => not strings.hasPrefix(v: r._value, prefix: \"influxdb\"))\n" + "|> filter(fn : (r) => not strings.hasPrefix(v: r._value, prefix: \"http\"))\n" + "|> filter(fn : (r) => not strings.hasPrefix(v: r._value, prefix: \"go\"))\n" + "|> filter(fn : (r) => not strings.hasPrefix(v: r._value, prefix: \"boltdb\"))";
        List<FluxTable> tables = queryApi.query(flux);
        for (FluxTable fluxTable : tables) {
            List<FluxRecord> records = fluxTable.getRecords();
            for (FluxRecord fluxRecord : records) {
                res.add((String) fluxRecord.getValue());
                //System.out.println(fluxRecord.getValue());
            }
        }
        return res;
    }

    public static List<ColumnMetadata> getColumns(String bucket, String tableName)
    {
        System.out.println("influxdbUtil 获取bucket:" + bucket + "table:" + tableName + "中的所有columnsMetadata");
        //logger.debug("getColumns in bucket: {}, table : {}", bucket, tableName);
        List<ColumnMetadata> res = new ArrayList<>();
        QueryApi queryApi = influxDBClient.getQueryApi();
        // all fields
//        String flux = "from(bucket: \"" + bucket + "\")\n" +
//                "  |> range(start: " + time_interval + ")\n" +
//                "  |> filter(fn: (r) => r[\"_measurement\"] == \"" + tableName + "\")";
        String flux = tableName + "\n |> group() |> limit(n:1)\n ";
        List<FluxTable> tables = queryApi.query(flux);
        for (FluxTable fluxTable : tables) {
            List<FluxColumn> records = fluxTable.getColumns();
            for (FluxColumn record : records) {
                String label = record.getLabel();
                if(!res.stream().anyMatch(t->t.getName().equals(label))) {
                    res.add(new ColumnMetadata((String) label, VarcharType.VARCHAR));
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
            System.out.println(columnMetadata.getName() + ":" + columnMetadata.getType().getDisplayName());
        }
        return res;
    }

    public static Iterator<InfluxdbRow> select(String tableName)
    {
        System.out.println("influxdbUtil-查询一个表" + tableName + "中的所有行，返回迭代器");
        //logger.debug("select all rows in table: {}", tableName);
        List<InfluxdbRow> list = new ArrayList<>();
        QueryApi queryApi = influxDBClient.getQueryApi();
        String flux = tableName;//"from(bucket: " + "\"" + bucket + "\"" + ")\n" + "|> range(start:" + time_interval + ")\n" + "|> filter(fn : (r) => r._measurement == " + "\"" + tableName + "\"" + ")";
        List<FluxTable> tables = queryApi.query(flux, org);
        Map<Instant, Map<String, Object>> resMap = new HashMap<>();
        for (FluxTable fluxTable : tables) {
            List<FluxRecord> records = fluxTable.getRecords();
            for (FluxRecord fluxRecord : records) {
                Map<String, Object> curRow = resMap.get(fluxRecord.getTime());
                if (curRow == null) {
                    curRow = fluxRecord.getValues();
                    Map<String, Object> newRow = new HashMap<>();
                    for (Map.Entry<String, Object> entry : curRow.entrySet()) {
//                        if (!Objects.equals(entry.getKey(), "_field") && !Objects.equals(entry.getKey(), "_value")) {
                            newRow.put(entry.getKey(), entry.getValue());
//                        }
                    }
                    newRow.put(fluxRecord.getField(), fluxRecord.getValue());
                    resMap.put(fluxRecord.getTime(), newRow);
                }
                else {
                    curRow.put(fluxRecord.getField(), fluxRecord.getValue());
                }
            }
        }
        // for debug
        for (Map.Entry<Instant, Map<String, Object>> entry : resMap.entrySet()) {
            for (Map.Entry<String, Object> entry1 : entry.getValue().entrySet()) {
                System.out.println("k-v pair " + entry1.getKey() + ":" + entry1.getValue().toString() + ", " + entry1.getValue().getClass());
            }
            list.add(new InfluxdbRow(entry.getValue()));
        }
        return list.iterator();
    }
}
