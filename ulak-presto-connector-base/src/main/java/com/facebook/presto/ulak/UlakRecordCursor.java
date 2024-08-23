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

import com.facebook.presto.ulak.caching.ConnectorBaseUtil;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.VarcharType;
import io.trino.spi.type.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;

public class UlakRecordCursor
        implements RecordCursor
{
    private static Logger logger = LoggerFactory.getLogger(UlakRecordCursor.class);
    private final List<UlakColumnHandle> columnHandles;
    private String[] defaultParams = null;

    private final Iterator<UlakRow> iterator;

    private UlakRow row;

    public UlakRecordCursor(List<UlakColumnHandle> columnHandles,
                                UlakSplit split,
                                String[] defaultParams,
                                BiFunction<QueryParameters,String[], List<UlakRow>> exec1)
    {
        this.columnHandles = columnHandles;
        this.defaultParams = defaultParams;
        try {
            this.iterator = ConnectorBaseUtil.select(QueryParameters.getQueryParameters(split.getTableName()),
                    false,
                    this.defaultParams,
                    (s,y)->  exec1.apply(s,y)).iterator();
        } catch (IOException e) {
            logger.error("Error getting cursor: " , e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (!iterator.hasNext()) {
            return false;
        }
        this.row = iterator.next();
        return true;
    }

    @Override
    public void close()
    {
        // Empty method
    }

    private void checkFieldType(int field, Type expected)
    {
        Type actual = getType(field);
        checkArgument(actual.equals(expected), "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, BooleanType.BOOLEAN);
        String columnName = columnHandles.get(field).getColumnName();
        Object value = row.getColumnMap().get(columnName);
        return Boolean.parseBoolean(value == null ? "false" : value.toString());
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, DoubleType.DOUBLE);
        String columnName = columnHandles.get(field).getColumnName();
        Object value = row.getColumnMap().get(columnName);
        if(value==null){
            value = 0.0;
        }
        logger.debug("{}",value);
        return Double.parseDouble(value == null ? "1" : value.toString());
    }

    @Override
    public long getLong(int field)
    {
        String columnName = columnHandles.get(field).getColumnName();
        Object value = row.getColumnMap().get(columnName);
        if (getType(field).equals(TimestampType.TIMESTAMP_MILLIS)) { // timestamp type, especially
            return ((Instant)value).toEpochMilli();
        }
        return Long.parseLong(value == null ? "-1" : value.toString()); // normal int/long
    }

    @Override
    public Object getObject(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        String columnName = columnHandles.get(field).getColumnName();
        return Objects.isNull(row.getColumnMap().get(columnName));
    }

    @Override
    public Slice getSlice(int field)
    {
        checkFieldType(field, VarcharType.createUnboundedVarcharType());
        String columnName = columnHandles.get(field).getColumnName();
        Object value = row.getColumnMap().get(columnName);
        if (value instanceof String)
            return Slices.utf8Slice((String) value);
        else
            return Slices.utf8Slice(String.valueOf(value));
    }
}
