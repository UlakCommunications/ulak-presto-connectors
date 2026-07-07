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

import static com.facebook.presto.quickwit.AggsDslCompiler.normalizeAggs;
import static com.facebook.presto.ulak.caching.ConnectorBaseUtil.getObjectMapper;

import java.time.Instant;
import java.time.temporal.ChronoUnit;


public class QuickwitRecordSetProvider extends UlakRecordSetProvider {
    private static final Logger logger = LoggerFactory.getLogger(QuickwitRecordSetProvider.class);

    public QuickwitRecordSetProvider(BiFunction<QueryParameters,String[], List<UlakRow>> exec1, String[] defaultParams){
        super(exec1,defaultParams);
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

    public static String buildSearchRequestJson(RawQuickwitQueryTableHandle h)
    {

        Instant now = Instant.now();                 // UTC now
        Instant start = now.minus(15, ChronoUnit.MINUTES);

        long startTs = start.getEpochSecond();       // veya toEpochMilli()
        long endTs   = now.getEpochSecond();
        if( h.getStartTimestamp().isPresent()){
            startTs = h.getStartTimestamp().get();
        }
        if( h.getEndTimestamp().isPresent()){
            endTs = h.getEndTimestamp().get();
        }
        ObjectNode root = getObjectMapper().createObjectNode();
        root.put("query", h.getQuery());
        int maxHits = 1000;
        if(h.getMaxHits().isPresent())
        {
            maxHits = h.getMaxHits().get();
        }
        root.put("max_hits", maxHits);

        if (h.getTimestampEnabled().orElse(true)) {
            root.put("start_timestamp", startTs);
            root.put("end_timestamp", endTs);
        }


        h.getAggsJson().ifPresent(aggs -> {
            try {
                root.set("aggs", getObjectMapper().readTree(normalizeAggs(aggs)));
            }
            catch (Exception e) {
                throw new RuntimeException("Invalid aggs JSON", e);
            }
        });

        String initialString =  root.toString();

        initialString = "//qwindex=" + h.getIndex() + "\n" + initialString;
        initialString = "//cache=" + (h.isCache().isPresent()?h.isCache().get():"false") + "\n" + initialString;
        initialString = "//name=" + (h.getName().isPresent()?h.getName().get():"<no_name>") + "\n" + initialString;
        initialString = "//columns=" + (h.getColumns().isPresent()?h.getColumns().get():"") + "\n" + initialString;
        initialString = "//dbtype=" + (h.getDbtype().isPresent()?h.getDbtype().get():"qw") + "\n" + initialString;
        initialString = "//replacefromcolumns=" + (h.getReplacefromcolumns().isPresent()?h.getReplacefromcolumns().get():"") + "\n" + initialString;
        initialString = "//hasjs=" + (h.getHasjs().isPresent()?h.getHasjs().get():"false") + "\n" + initialString;
        initialString = "//sqlversion=" + (h.getSqlversion().isPresent()?h.getSqlversion().get():"0") + "\n" + initialString;
        initialString = "//from=" + h.getStartTimestamp().orElse(startTs) + "\n" + initialString;
        initialString = "//to=" + h.getEndTimestamp().orElse(endTs) + "\n" + initialString;

        return initialString;
    }

}
