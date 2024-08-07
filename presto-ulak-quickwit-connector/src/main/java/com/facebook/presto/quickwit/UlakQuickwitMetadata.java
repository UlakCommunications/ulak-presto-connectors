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

import com.facebook.presto.ulak.UlakColumnHandle;
import com.facebook.presto.ulak.UlakConnectorId;
import com.facebook.presto.ulak.UlakRow;
import com.facebook.presto.ulak.UlakTableHandle;
import com.facebook.presto.ulak.caching.ConnectorBaseUtil;
import com.quickwit.javaclient.ApiException;
import io.trino.spi.connector.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

import static com.facebook.presto.ulak.caching.ConnectorBaseUtil.getColumnsBase;


public class UlakQuickwitMetadata
        implements ConnectorMetadata
{
    private static Logger logger = LoggerFactory.getLogger(UlakQuickwitMetadata.class);
    private static UlakQuickwitMetadata single;
    private static String connectorId;
    private String qwIndex;
    private String qwUrl;

    private UlakQuickwitMetadata(String catalogName, String qwUrl, String qwIndex)
    {
        this.setQwUrl(qwUrl);
        this.setQwIndex(qwIndex);
        connectorId = new UlakConnectorId(catalogName).toString();
    }

    public static UlakQuickwitMetadata getInstance(String catalogName, String qwUrl, String qwIndex)
    {
        if (single == null) {
            single = new UlakQuickwitMetadata(catalogName,qwUrl,qwIndex);
        }
        return single;
    }


    // list all bucket names
    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return null; //TODO:
//        logger.debug("influxdbMetadata--bucket names");
//        switch (dbType){
//            case  INFLUXDB2:
//                return QwUtil.getSchemas();
//            case  PG:
//                try {
//                    return PGUtil.getSchemas();
//                } catch (SQLException e) {
//                    logger.error("InfluxdbConnector", e);
//                    throw new RuntimeException(e);
//                } catch (JsonProcessingException e) {
//                    logger.error("InfluxdbConnector", e);
//                    throw new RuntimeException(e);
//                }
//        }
//        throw new RuntimeException("Invalid dbType: " + dbType);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        return new UlakTableHandle(connectorId, tableName.getSchemaName(), tableName.getTableName());
    }

//    @Override
//    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns)
//    {
//        UlakTableHandle tableHandle = (UlakTableHandle) table;
//        ConnectorTableLayout layout = new ConnectorTableLayout(new UlakTableLayoutHandle(tableHandle));
//        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
//    }

    // list all measurements in a bucket
    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return null; //TODO
    }

//    @Override
//    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
//    {
//        return new ConnectorTableLayout(handle);
//    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        UlakTableHandle influxdbTableHandle = (UlakTableHandle) table;
        List<ColumnMetadata> list = null;
        try {
            String tableName = influxdbTableHandle.getTableName();
            list = getColumnsBase(ConnectorBaseUtil.select(tableName,false, s-> {
                            try {
                                return  QwUtil.select(s, qwUrl, qwIndex);
                            } catch (ApiException e) {
                                logger.error("ApiException", e);
                                throw new RuntimeException(e);
                            }
                        }));
        } catch (IOException e) {
            logger.error("IOException", e);
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            logger.error("ClassNotFoundException", e);
            throw new RuntimeException(e);
        }catch (Exception e) {
            logger.error("Exception - Empty query", e);
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
            list = getColumnsBase(ConnectorBaseUtil.select(tableName,false, s-> {
                        try {
                            return QwUtil.select(s, qwUrl, qwIndex);
                        } catch (ApiException e) {
                            logger.error("ApiException", e);
                            throw new RuntimeException(e);
                        }
                    }));

        } catch (IOException e) {
            logger.error("IOException", e);
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            logger.error("ClassNotFoundException", e);
            throw new RuntimeException(e);
        }catch (Exception e) {
            logger.error("Exception - Empty query", e);
        }
        for (int i = 0; i < list.size(); ++i) {
            ColumnMetadata metadata = list.get(i);
            res.put(metadata.getName(), new UlakColumnHandle(connectorId, metadata.getName(), metadata.getType(), i));
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
                    columns.put(tableName,
                            getColumnsBase(ConnectorBaseUtil.select(tableName.getTableName(),false, s-> {
                                        try {
                                            return QwUtil.select(s, qwUrl, qwIndex);
                                        } catch (ApiException e) {
                                            logger.error("ClassNotFoundException", e);
                                            throw new RuntimeException(e);
                                        }
                                    })));
                } catch (IOException e) {
                    logger.error("IOException", e);
                    throw new RuntimeException(e);
                } catch (ClassNotFoundException e) {
                    logger.error("ClassNotFoundException", e);
                    throw new RuntimeException(e);
                }catch (Exception e) {
                    logger.error("Exception - Empty query", e);
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

    public String getQwUrl() {
        return qwUrl;
    }

    public void setQwUrl(String qwUrl) {
        this.qwUrl = qwUrl;
    }

    public String getQwIndex() {
        return qwIndex;
    }

    public void setQwIndex(String qwIndex) {
        this.qwIndex = qwIndex;
    }
}
