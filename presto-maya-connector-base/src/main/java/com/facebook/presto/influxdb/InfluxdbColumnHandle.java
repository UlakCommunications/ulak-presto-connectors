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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;

import io.trino.spi.type.Type;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class InfluxdbColumnHandle
        implements ColumnHandle
{
    private final String columnName;
    private final Type columnType;
    private final int ordinalPosition;
    private final String connectorId;

    @JsonCreator
    public InfluxdbColumnHandle(@JsonProperty("connectorId") String connectorId, @JsonProperty("columnName") String columnName, @JsonProperty("columnType") Type columnType, @JsonProperty("ordinalPosition") int ordinalPosition)
    {
        this.ordinalPosition = ordinalPosition;
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.columnName = requireNonNull(columnName, "columnName is null");
    }

    @JsonProperty
    public int getOrdinalPosition()
    {
        return ordinalPosition;
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public Type getColumnType()
    {
        return columnType;
    }

    public ColumnMetadata getColumnMetadata()
    {
        return new ColumnMetadata(columnName, columnType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, columnName);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        InfluxdbColumnHandle other = (InfluxdbColumnHandle) obj;
        return Objects.equals(this.connectorId, other.connectorId) && Objects.equals(this.columnName, other.columnName);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this).add("connectorId", connectorId).add("columnName", columnName).add("columnType", columnType).add("ordinalPosition", ordinalPosition).toString();
    }
}
