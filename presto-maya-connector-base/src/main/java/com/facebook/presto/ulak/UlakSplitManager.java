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


import io.trino.spi.connector.*;

import java.util.ArrayList;
import java.util.List;

public class UlakSplitManager
        implements ConnectorSplitManager
{
    public static UlakSplitManager single;

    private UlakSplitManager()
    {
    }

    public static UlakSplitManager getInstance()
    {
        if (single == null) {
            single = new UlakSplitManager();
        }
        return single;
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction,
                                          ConnectorSession session,
                                          ConnectorTableHandle table,
                                          DynamicFilter dynamicFilter,
                                          Constraint constraint) {
        UlakTableHandle tableHandle = (UlakTableHandle) table;
        List<ConnectorSplit> splits = new ArrayList<>();
        splits.add(new UlakSplit(tableHandle.getSchemaName(), tableHandle.getTableName()));
        return new FixedSplitSource(splits);
    }

}
