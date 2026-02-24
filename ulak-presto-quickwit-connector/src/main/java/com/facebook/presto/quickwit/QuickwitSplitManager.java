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


import com.facebook.presto.ulak.UlakSplit;
import com.facebook.presto.ulak.UlakTableHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.*;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.quickwit.QuickwitRecordSetProvider.buildSearchRequestJson;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public class QuickwitSplitManager
        implements ConnectorSplitManager
{
    public static QuickwitSplitManager single;

    private QuickwitSplitManager()
    {
        super();
    }

    public static QuickwitSplitManager getInstance()
    {
        if (single == null) {
            single = new QuickwitSplitManager();
        }
        return single;
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction,
                                          ConnectorSession session,
                                          ConnectorTableHandle table,
                                          DynamicFilter dynamicFilter,
                                          Constraint constraint) {

        List<ConnectorSplit> splits = new ArrayList<>();
        if (table instanceof RawQuickwitQueryTableHandle ) {
            RawQuickwitQueryTableHandle raw = (RawQuickwitQueryTableHandle) table;
            UlakSplit ulakSplit = new UlakSplit("test", buildSearchRequestJson(raw));
            ulakSplit.setTableName(buildSearchRequestJson(raw));
            splits.add(ulakSplit);
        }else{
            UlakTableHandle tableHandle = (UlakTableHandle) table;
            splits.add(new UlakSplit(tableHandle.getSchemaName(), tableHandle.getTableName()));
        }
        return new FixedSplitSource(splits);
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableFunctionHandle function)
    {
        if (function instanceof RawQuery.RawQueryFunction.RawQueryFunctionHandle  ) {
            RawQuery.RawQueryFunction.RawQueryFunctionHandle raw = (RawQuery.RawQueryFunction.RawQueryFunctionHandle) function;
            UlakSplit split = new UlakSplit("system", "raw_query");
            split.setTableName(buildSearchRequestJson((RawQuickwitQueryTableHandle) raw.getTableHandle())); // (rename this field later; it’s not really tableName)
            return new FixedSplitSource(List.of(split));
        }else{
            return ConnectorSplitManager.super.getSplits(transaction,
                    session,
                    function);
        }
//        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unknown table function handle: " + function);
    }

}
