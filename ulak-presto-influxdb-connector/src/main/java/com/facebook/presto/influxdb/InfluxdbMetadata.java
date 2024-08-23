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

import com.facebook.presto.ulak.UlakColumnHandle;
import com.facebook.presto.ulak.UlakConnectorId;
import com.facebook.presto.ulak.UlakTableHandle;
import io.trino.spi.connector.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.util.*;

public class InfluxdbMetadata
        implements ConnectorMetadata
{
    private static Logger logger = LoggerFactory.getLogger(InfluxdbMetadata.class);
    private static InfluxdbMetadata single;
    private final String connectorId;
    private String url;
    private String org;
    private String token;

    InfluxdbMetadata(String catalogName, String url, String org, String token)
    {
        connectorId = new UlakConnectorId(catalogName).toString();
        this.url = url;
        this.org = org;
        this.token = token;
    }


    // list all bucket names
    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        logger.debug("influxdbMetadata--bucket names");
        try {
            return InfluxdbUtil.getSchemas(this.url,this.org,this.token);
        } catch (IOException e) {
            logger.error("listSchemaNames",e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        return new UlakTableHandle(connectorId, tableName.getSchemaName(), tableName.getTableName());
    }

    // list all measurements in a bucket
    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        List<SchemaTableName> listTable = new ArrayList<>();
        if (!schemaName.isPresent()) {
            return Collections.emptyList();
        }
        List<String> tables = null;
        String schema = schemaName.get();
        for (String table : tables) {
            listTable.add(new SchemaTableName(schema, table));
        }
        return listTable;
    }


    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        UlakTableHandle influxdbTableHandle = (UlakTableHandle) table;
        List<ColumnMetadata> list = null;
        try {
            list = InfluxdbUtil.getColumns(influxdbTableHandle.getSchemaName(), influxdbTableHandle.getTableName(),this.url,this.org,this.token);
        } catch (IOException e) {
            logger.error("InfluxdbMetadata getTableMetadata IOException", e);
        } catch (ClassNotFoundException e) {
            logger.error("InfluxdbMetadata getTableMetadata ClassNotFoundException", e);
        }catch (Exception e) {
            logger.error("InfluxdbMetadata getTableMetadata Exception - Empty query", e);
        }
        SchemaTableName tableName = new SchemaTableName(influxdbTableHandle.getSchemaName(), influxdbTableHandle.getTableName());

        return new ConnectorTableMetadata(tableName, list);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        UlakTableHandle influxdbTableHandle = (UlakTableHandle) tableHandle;
        Map<String, ColumnHandle> res = new HashMap<>();
        List<ColumnMetadata> list = null;
        try {
            list = InfluxdbUtil.getColumns(influxdbTableHandle.getSchemaName(), influxdbTableHandle.getTableName(),this.url,this.org,this.token);

            for (int i = 0; i < list.size(); ++i) {
                ColumnMetadata metadata = list.get(i);
                res.put(metadata.getName(), new UlakColumnHandle(connectorId, metadata.getName(), metadata.getType(), i));
            }
        } catch (IOException e) {
            logger.error("InfluxdbMetadata getColumnHandles IOException", e);
        } catch (ClassNotFoundException e) {
            logger.error("InfluxdbMetadata getColumnHandles ClassNotFoundException", e);
        }catch (Exception e) {
            logger.error("InfluxdbMetadata getColumnHandles Exception - Empty query", e);
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
                    columns.put(tableName, InfluxdbUtil.getColumns(session.getSource().get(), tableName.getTableName(),this.url,this.org,this.token));
                } catch (IOException e) {
                    logger.error("InfluxdbMetadata listTableColumns IOException", e);
                } catch (ClassNotFoundException e) {
                    logger.error("InfluxdbMetadata listTableColumns ClassNotFoundException", e);
                }catch (Exception e) {
                    logger.error("InfluxdbMetadata listTableColumns Exception - Empty query", e);
                }
            }
        }
        return columns;
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((UlakColumnHandle) columnHandle).getColumnMetadata();
    }
}
