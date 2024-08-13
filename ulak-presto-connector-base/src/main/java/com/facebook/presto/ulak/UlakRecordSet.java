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

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class UlakRecordSet
        implements RecordSet
{
    private final List<UlakColumnHandle> columnHandles;
    private final Function<String, List<UlakRow>> exec1;
    private final List<Type> columnTypes;
    private final UlakSplit split;

    public UlakRecordSet(UlakSplit split,
                             List<UlakColumnHandle> columnHandles,
                             Function<String, List<UlakRow>> exec1)
    {
        this.split = requireNonNull(split, "split is null");
        this.columnHandles = requireNonNull(columnHandles, "column handles is null");
        this.exec1 = exec1;
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (UlakColumnHandle column : columnHandles) {
            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new UlakRecordCursor(columnHandles, split,exec1);
    }
}
