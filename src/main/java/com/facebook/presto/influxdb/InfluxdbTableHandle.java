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

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class InfluxdbTableHandle
        implements ConnectorTableHandle
{
    private static Logger logger = LoggerFactory.getLogger(InfluxdbTableHandle.class);
    private String schemaName;
    private String tableName;
    private String connectorId;

    @JsonCreator
    public InfluxdbTableHandle(@JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schemaName, @JsonProperty("tableName") String tableName)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getSchemaName()
    {
        logger.debug("schemaName: {}", schemaName);
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        logger.debug("tableName: {}", tableName);
        return tableName;
    }

    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, schemaName, tableName);
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

        InfluxdbTableHandle other = (InfluxdbTableHandle) obj;
        return Objects.equals(this.connectorId, other.connectorId) && Objects.equals(this.schemaName, other.schemaName)
                && Objects.equals(this.tableName, other.tableName);
    }

    @Override
    public String toString()
    {
        return Joiner.on(":").join(connectorId, schemaName, tableName);
    }
}
