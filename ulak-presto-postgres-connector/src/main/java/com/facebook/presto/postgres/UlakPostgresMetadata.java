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
package com.facebook.presto.postgres;

import com.facebook.presto.ulak.UlakColumnHandle;
import com.facebook.presto.ulak.UlakConnectorId;
import com.facebook.presto.ulak.UlakTableHandle;
import com.facebook.presto.ulak.caching.ConnectorBaseUtil;
import com.facebook.presto.ulak.QueryParameters;
import io.trino.spi.connector.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

import static com.facebook.presto.ulak.caching.ConnectorBaseUtil.getColumnsBase;

public class UlakPostgresMetadata
        implements ConnectorMetadata
{
    private static Logger logger = LoggerFactory.getLogger(UlakPostgresMetadata.class);
    private static UlakPostgresMetadata single;
    private static String connectorId;
    private static final String ERRORSTRING = "UlakPostgresConnector Error: {}";
    private UlakPostgresMetadata(String catalogName)
    {
        connectorId = new UlakConnectorId(catalogName).toString();
    }

    public static UlakPostgresMetadata getInstance(String catalogName)
    {
        if (single == null) {
            single = new UlakPostgresMetadata(catalogName);
        }
        return single;
    }

    // list all bucket names
    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
         return Collections.emptyList();//TODO:
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
        return Collections.emptyList(); //TODO:
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        UlakTableHandle influxdbTableHandle = (UlakTableHandle) table;
        List<ColumnMetadata> list = null;
        try {
            String tableName = influxdbTableHandle.getTableName();
            list =  getColumnsBase(ConnectorBaseUtil.select(QueryParameters.getQueryParameters(tableName),
                    false,new String[]{}, (q,s)-> {
                try {
                    return  PGUtil.select(q.getQuery());
                } catch (IOException | SQLException e) {
                    logger.error(ERRORSTRING, e);
                    throw new RuntimeException(e);
                }
            }));
        } catch (Exception e) {
            logger.error(ERRORSTRING, e);
            throw new RuntimeException(e);
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
            String tableName = influxdbTableHandle.getTableName();
            list = getColumnsBase(ConnectorBaseUtil.select(QueryParameters.getQueryParameters(tableName),false,new String[]{}, (q,s)-> {
                try {
                    return PGUtil.select(q.getQuery());
                } catch (SQLException | IOException e) {
                    logger.error(ERRORSTRING, e);
                    throw new RuntimeException(e);
                }
            }));

            for (int i = 0; i < list.size(); ++i) {
                ColumnMetadata metadata = list.get(i);
                res.put(metadata.getName(), new UlakColumnHandle(connectorId, metadata.getName(), metadata.getType(), i));
            }
        } catch (Exception e) {
            logger.error(ERRORSTRING, e);
            throw new RuntimeException(e);
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
                    columns.put(tableName, getColumnsBase(ConnectorBaseUtil.select(QueryParameters.getQueryParameters(tableName.getTableName()),
                            false,new String[]{}, (q,s)-> {
                        try {
                            return PGUtil.select(q.getQuery());
                        } catch (SQLException | IOException e) {
                            logger.error(ERRORSTRING, e);
                            throw new RuntimeException(e);
                        }
                    })));
                } catch (Exception e) {
                    logger.error(ERRORSTRING, e);
                    throw new RuntimeException(e);
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
