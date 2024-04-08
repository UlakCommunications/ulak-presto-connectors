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

import com.facebook.presto.pg.PGUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class InfluxdbMetadata
        implements ConnectorMetadata
{
    private static Logger logger = LoggerFactory.getLogger(InfluxdbMetadata.class);
    private static InfluxdbMetadata single;
    private static String connectorId;
    private final DBType dbType;

    private InfluxdbMetadata(DBType dbType, String catalogName)
    {
        this.dbType = dbType;
        connectorId = new InfluxdbConnectorId(catalogName).toString();
    }

    public static InfluxdbMetadata getInstance(DBType dbType, String catalogName)
    {
        if (single == null) {
            single = new InfluxdbMetadata(dbType, catalogName);
        }
        return single;
    }

    // list all bucket names
    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        logger.debug("influxdbMetadata--bucket names");
        switch (dbType){
            case  INFLUXDB2:
                return InfluxdbUtil.getSchemas();
            case  PG:
                try {
                    return PGUtil.getSchemas();
                } catch (SQLException e) {
                    logger.error("InfluxdbConnector", e);
                    throw new RuntimeException(e);
                } catch (JsonProcessingException e) {
                    logger.error("InfluxdbConnector", e);
                    throw new RuntimeException(e);
                }
        }
        throw new RuntimeException("Invalid dbType: " + dbType);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        return new InfluxdbTableHandle(connectorId, tableName.getSchemaName(), tableName.getTableName());
    }

//    @Override
//    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
//    {
//        InfluxdbTableHandle tableHandle = (InfluxdbTableHandle) table;
//        ConnectorTableLayout layout = new ConnectorTableLayout(new InfluxdbTableLayoutHandle(tableHandle));
//        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
//    }

    // list all measurements in a bucket
    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        List<SchemaTableName> listTable = new ArrayList<>();
        if (!schemaName.isPresent()) {
            return null;
        }
        List<String> tables = null;
        String schema = schemaName.get();
        switch (dbType){
            case  INFLUXDB2:
                tables = InfluxdbUtil.getTableNames(schema);
            case  PG:
                try {
                    tables =  PGUtil.getTableNames(schema);
                } catch (SQLException e) {
                    logger.error("InfluxdbConnector", e);
                    throw new RuntimeException(e);
                } catch (JsonProcessingException e) {
                    logger.error("InfluxdbConnector", e);
                    throw new RuntimeException(e);
                }
        }
        for (String table : tables) {
            listTable.add(new SchemaTableName(schema, table));
        }
        return listTable;
    }

//    @Override
//    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
//    {
//        return new ConnectorTableLayout(handle);
//    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        InfluxdbTableHandle influxdbTableHandle = (InfluxdbTableHandle) table;
        List<ColumnMetadata> list = null;
        try {
            list = InfluxdbUtil.getColumns(dbType, influxdbTableHandle.getSchemaName(), influxdbTableHandle.getTableName());
        } catch (IOException e) {
            logger.error("IOException", e);
        } catch (ClassNotFoundException e) {
            logger.error("ClassNotFoundException", e);
        }
        SchemaTableName tableName = new SchemaTableName(influxdbTableHandle.getSchemaName(), influxdbTableHandle.getTableName());

        return new ConnectorTableMetadata(tableName, list);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        InfluxdbTableHandle influxdbTableHandle = (InfluxdbTableHandle) tableHandle;
        Map<String, ColumnHandle> res = new HashMap<>();
        List<ColumnMetadata> list = null;
        try {
            list = InfluxdbUtil.getColumns(dbType, influxdbTableHandle.getSchemaName(), influxdbTableHandle.getTableName());

            for (int i = 0; i < list.size(); ++i) {
                ColumnMetadata metadata = list.get(i);
                res.put(metadata.getName(), new InfluxdbColumnHandle(connectorId, metadata.getName(), metadata.getType(), i));
            }
        } catch (IOException e) {
            logger.error("IOException", e);
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            logger.error("ClassNotFoundException", e);
        }
        return res;
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        Map<SchemaTableName, List<ColumnMetadata>> columns = new HashMap<>();
        List<SchemaTableName> list = listTables(session, session.getSource());
        for (SchemaTableName tableName : list) {
            if (tableName.getTableName().startsWith(prefix.getTable().get())) {
                try {
                    columns.put(tableName, InfluxdbUtil.getColumns(dbType, session.getSource().get(), tableName.getTableName()));
                } catch (IOException e) {
                    logger.error("IOException", e);
                    throw new RuntimeException(e);
                } catch (ClassNotFoundException e) {
                    logger.error("ClassNotFoundException", e);
                }
            }
        }
        return columns;
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((InfluxdbColumnHandle) columnHandle).getColumnMetadata();
    }
}
