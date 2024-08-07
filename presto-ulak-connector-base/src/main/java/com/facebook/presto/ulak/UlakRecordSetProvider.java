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
package com.facebook.presto.ulak;

import com.facebook.presto.ulak.UlakColumnHandle;
import com.facebook.presto.ulak.UlakRecordSet;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

public class UlakRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private static Logger logger = LoggerFactory.getLogger(UlakRecordSetProvider.class);
    private static UlakRecordSetProvider single;
    private final Function<String, List<UlakRow>> exec1;

    public UlakRecordSetProvider(Function<String, List<UlakRow>> exec1){
        this.exec1 = exec1;
    }
    public static UlakRecordSetProvider getInstance(Function<String, List<UlakRow>> exec1)
    {
        if (single == null) {
            single = new UlakRecordSetProvider(exec1);
        }
        return single;
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<? extends ColumnHandle> columns) {
        UlakSplit influxdbSplit = (UlakSplit) split;
        ImmutableList.Builder<UlakColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : columns) {
            UlakColumnHandle influxdbColumnHandle = (UlakColumnHandle) handle;
            handles.add(influxdbColumnHandle);
            logger.debug(influxdbColumnHandle.getColumnName() + ":" + influxdbColumnHandle.getColumnType());
        }
        return new UlakRecordSet(influxdbSplit, handles.build(),exec1);
    }

}
