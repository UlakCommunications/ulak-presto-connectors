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

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class InfluxdbRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private static Logger logger = LoggerFactory.getLogger(InfluxdbRecordSetProvider.class);
    private static InfluxdbRecordSetProvider single;
    private final Function<String, ArrayList<InfluxdbRow>> exec1;

    public InfluxdbRecordSetProvider(Function<String, ArrayList<InfluxdbRow>> exec1){
        this.exec1 = exec1;
    }
    public static InfluxdbRecordSetProvider getInstance(Function<String, ArrayList<InfluxdbRow>> exec1)
    {
        if (single == null) {
            single = new InfluxdbRecordSetProvider(exec1);
        }
        return single;
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<? extends ColumnHandle> columns) {
        InfluxdbSplit influxdbSplit = (InfluxdbSplit) split;
        ImmutableList.Builder<InfluxdbColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : columns) {
            InfluxdbColumnHandle influxdbColumnHandle = (InfluxdbColumnHandle) handle;
            handles.add(influxdbColumnHandle);
           logger.debug(influxdbColumnHandle.getColumnName() + ":" + influxdbColumnHandle.getColumnType());
        }
        return new InfluxdbRecordSet(influxdbSplit, handles.build(),exec1);
    }

}
