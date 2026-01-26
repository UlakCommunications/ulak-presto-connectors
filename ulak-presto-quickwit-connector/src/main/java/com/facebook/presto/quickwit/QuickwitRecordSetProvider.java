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

import com.facebook.presto.ulak.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.BiFunction;

import static com.facebook.presto.ulak.caching.ConnectorBaseUtil.getObjectMapper;

public class QuickwitRecordSetProvider extends UlakRecordSetProvider {
    private static Logger logger = LoggerFactory.getLogger(QuickwitRecordSetProvider.class);
    private static QuickwitRecordSetProvider single;

    public QuickwitRecordSetProvider(BiFunction<QueryParameters,String[], List<UlakRow>> exec1, String[] defaultParams){
        super(exec1,defaultParams);
    }
    public static QuickwitRecordSetProvider getInstance(BiFunction<QueryParameters,String[], List<UlakRow>> exec1, String[] defaultParams)
    {
        if (single == null) {
            single = new QuickwitRecordSetProvider(exec1,defaultParams);
        }
        return single;
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transaction,
                                  ConnectorSession session,
                                  ConnectorSplit split,
                                  ConnectorTableHandle table,
                                  List<? extends ColumnHandle> columns) {
        UlakSplit ulakSplit = (UlakSplit) split;
        if (table instanceof RawQuickwitQueryTableHandle ) {
            RawQuickwitQueryTableHandle raw = (RawQuickwitQueryTableHandle) table;
            ulakSplit.setTableName(buildSearchRequestJson(raw));
        }
        ImmutableList.Builder<UlakColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : columns) {
            UlakColumnHandle influxdbColumnHandle = (UlakColumnHandle) handle;
            handles.add(influxdbColumnHandle);
            logger.debug("{}:{}", influxdbColumnHandle.getColumnName(), influxdbColumnHandle.getColumnType());
        }
        return new UlakRecordSet(ulakSplit, handles.build(),this.exec1,this.defaultParams);
    }

    private String buildSearchRequestJson(RawQuickwitQueryTableHandle h)
    {
        ObjectNode root = getObjectMapper().createObjectNode();
        root.put("query", h.getQuery());

        h.getMaxHits().ifPresent(v -> root.put("max_hits", v));
        h.getStartTimestamp().ifPresent(v -> root.put("start_timestamp", v));
        h.getEndTimestamp().ifPresent(v -> root.put("end_timestamp", v));

        h.getAggsJson().ifPresent(aggs -> {
            try {
                root.set("aggs", getObjectMapper().readTree(aggs));
            }
            catch (Exception e) {
                throw new RuntimeException("Invalid aggs JSON", e);
            }
        });

        return root.toString();
    }

}
