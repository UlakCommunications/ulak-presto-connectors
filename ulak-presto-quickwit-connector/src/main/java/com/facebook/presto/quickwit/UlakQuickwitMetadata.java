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

import com.facebook.presto.ulak.QueryParameters;
import com.facebook.presto.ulak.UlakColumnHandle;
import com.facebook.presto.ulak.UlakConnectorId;
import com.facebook.presto.ulak.UlakTableHandle;
import com.facebook.presto.ulak.caching.ConnectorBaseUtil;
import com.google.common.collect.ImmutableList;
import com.quickwit.javaclient.ApiException;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.*;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static com.facebook.presto.quickwit.QuickwitRecordSetProvider.buildSearchRequestJson;
import static com.facebook.presto.ulak.caching.ConnectorBaseUtil.getColumnsBase;


public class UlakQuickwitMetadata
        implements ConnectorMetadata {
    public static final String DEFAULT_SCHEMA = "default_schema";
    public static final String DEFAULT_TABLE = "default_Table";
    private static final Logger logger = LoggerFactory.getLogger(UlakQuickwitMetadata.class);
    private final String connectorId;
    private String qwIndex;
    private final Integer connectTimeout;
    private final Integer readTimeout;
    private final Integer writeTimeout;
    private String qwUrl;
    private static final String ERRORSTRING = "UlakQuickwitMetadata.java Error: {}";

    public UlakQuickwitMetadata(String catalogName, String qwUrl, String qwIndex,
                                Integer connectTimeout,
                                Integer readTimeout,
                                Integer writeTimeout) {
        this.qwUrl = qwUrl;
        this.qwIndex = qwIndex;
        this.connectTimeout = connectTimeout;
        this.readTimeout = readTimeout;
        this.writeTimeout = writeTimeout;
        this.setQwUrl(qwUrl);
        this.setQwIndex(qwIndex);
        this.connectorId = new UlakConnectorId(catalogName).toString();
    }

    public String getConnectorId() {
        return connectorId;
    }


    // list all bucket names
    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return Collections.singletonList(DEFAULT_SCHEMA); //TODO:
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion) {
        return new UlakTableHandle(connectorId, tableName.getSchemaName(), tableName.getTableName());
    }

//    //    @Override
//    public ConnectorTableHandle getTableHandle2(ConnectorSession session, SchemaTableName tableName)
//    {
//        return new UlakTableHandle(connectorId, tableName.getSchemaName(), tableName.getTableName());
//    }

    // list all measurements in a bucket
    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
        return Collections.singletonList(new SchemaTableName(DEFAULT_SCHEMA, DEFAULT_TABLE)); //TODO
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        logger.debug("getTableMetadata: url:{}\n\n\nindex:{}",
                this.qwUrl,
                this.qwIndex);
        List<ColumnMetadata> list = null;
        RawQuickwitQueryTableHandle raw = null;
        UlakTableHandle influxdbTableHandle = null;
        String tableName =null;
        try {
            if (table instanceof RawQuickwitQueryTableHandle) {
                raw = (RawQuickwitQueryTableHandle) table;
                tableName = buildSearchRequestJson(raw);
            }else {
                influxdbTableHandle = (UlakTableHandle) table;
                tableName =  influxdbTableHandle.getTableName();
            }
            logger.debug("getTableMetadata tableName: tableName:{}",
                    tableName);
            QueryParameters qp = QueryParameters.getQueryParameters(tableName);

            if (StringUtils.isBlank(qp.getQwUrl())) {
                qp.setQwUrl(this.qwUrl);
            }
            if (StringUtils.isBlank(qp.getQwIndex())) {
                qp.setQwIndex(this.qwIndex);
            }
            list = getColumnsBase(ConnectorBaseUtil.select(qp,
                    false, new String[]{this.qwUrl, this.qwIndex}, (q, s) -> {
                        try {
                            java.util.List<com.facebook.presto.ulak.UlakRow> ret =  QwUtil.select(q, s[0], s[1], connectTimeout, readTimeout, writeTimeout);
                            return ret;
                        } catch (ApiException e) {
                            logger.error(ERRORSTRING, e);
                            throw new RuntimeException(e);
                        }
                    }));
        } catch (IOException e) {
            logger.error(ERRORSTRING, e);
            throw new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, e);
        } catch (Exception e) {
            // Do not swallow — returning a null `list` from getTableMetadata
            // makes Trino fail with the opaque "columns is null". Surface the
            // real cause as a TrinoException (NOT RuntimeException) so the
            // Trino transaction is aborted cleanly instead of being left in
            // a "committed" state that breaks subsequent metadata calls with
            // "Current transaction already committed".
            logger.error(ERRORSTRING, e);
            throw new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, e);
        }
        SchemaTableName tableNameSchema = new SchemaTableName((raw==null?influxdbTableHandle.getSchemaName():"test"), (raw==null?influxdbTableHandle.getTableName():tableName));

        return new ConnectorTableMetadata(tableNameSchema, list);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
        logger.debug("getColumnHandles: url:{}\n\n\nindex:{}",
                this.qwUrl,
                this.qwIndex);
        Map<String, ColumnHandle> res = new HashMap<>();
        List<ColumnMetadata> list = null;
        try {
            String tableName = null;
            if (tableHandle instanceof RawQuickwitQueryTableHandle) {
                RawQuickwitQueryTableHandle raw = (RawQuickwitQueryTableHandle) tableHandle;
                tableName = buildSearchRequestJson(raw);
            }else{
                UlakTableHandle influxdbTableHandle = (UlakTableHandle) tableHandle;
                tableName = influxdbTableHandle.getTableName();
            }

            list = getColumnsInternal(tableName, this.qwUrl, this.qwIndex,   connectTimeout,   readTimeout,   writeTimeout);

            logger.debug("getColumnHandles: num columns:{}", list.size());

        } catch (IOException e) {
            logger.error(ERRORSTRING, e);
            throw new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, e);
        } catch (Exception e) {
            // Do not swallow — returning a null `list` from getTableMetadata
            // makes Trino fail with the opaque "columns is null". Surface the
            // real cause as a TrinoException (NOT RuntimeException) so the
            // Trino transaction is aborted cleanly instead of being left in
            // a "committed" state that breaks subsequent metadata calls with
            // "Current transaction already committed".
            logger.error(ERRORSTRING, e);
            throw new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, e);
        }
        for (int i = 0; i < list.size(); ++i) {
            ColumnMetadata metadata = list.get(i);
            res.put(metadata.getName(), new UlakColumnHandle(connectorId, metadata.getName(), metadata.getType(), i));
        }
        return res;
    }
    public  static  List<ColumnMetadata> getColumnsInternal(String tableName, String qwUrl, String qwIndex, Integer connectTimeout, Integer readTimeout, Integer writeTimeout) throws IOException {
        logger.debug("getColumnHandles: tableName:{}", tableName);
        QueryParameters qp = QueryParameters.getQueryParameters(tableName);

        if (StringUtils.isBlank(qp.getQwUrl())) {
            qp.setQwUrl(qwUrl);
        }
        if (StringUtils.isBlank(qp.getQwIndex())) {
            qp.setQwIndex(qwIndex);
        }
        return getColumnsBase(ConnectorBaseUtil.select(qp,
                false, new String[]{qwUrl, qwIndex}, (q, s) -> {
                    try {
                        logger.debug("From UlakQuickwitMetadata getColumnsInternal in exec: {}\n\n\nurl:{}\n\n\nindex:{}",
                                q.getQuery(),
                                s[0],
                                s[1]);
                        java.util.List<com.facebook.presto.ulak.UlakRow> ret =  QwUtil.select(q, s[0], s[1], connectTimeout, readTimeout, writeTimeout);
                        return ret;
                    } catch (ApiException e) {
                        logger.error(ERRORSTRING, e);
                        throw new RuntimeException(e);
                    }
                }));
    }
    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix) {
        logger.debug("listTableColumns: url:{}\n\n\nindex:{}",
                this.qwUrl,
                this.qwIndex);
        Map<SchemaTableName, List<ColumnMetadata>> columns = new HashMap<>();
        List<SchemaTableName> list = listTables(session, session.getSource());
        for (SchemaTableName tableName : list) {
            if (tableName.getTableName().startsWith(prefix.getTable().get())) {
                try {

                    QueryParameters qp = QueryParameters.getQueryParameters(tableName.getTableName());

                    if (StringUtils.isBlank(qp.getQwUrl())) {
                        qp.setQwUrl(this.qwUrl);
                    }
                    if (StringUtils.isBlank(qp.getQwIndex())) {
                        qp.setQwIndex(this.qwIndex);
                    }
                    columns.put(tableName,
                            getColumnsBase(
                                    ConnectorBaseUtil.select(
                                            qp,
                                            false,
                                            new String[]{this.qwUrl, this.qwIndex},
                                            (q, s) -> {
                                                try {
                                                    java.util.List<com.facebook.presto.ulak.UlakRow> ret =  QwUtil.select(q, s[0], s[1], connectTimeout, readTimeout, writeTimeout);
                                                    return ret;
                                                } catch (ApiException e) {
                                                    logger.error(ERRORSTRING, e);
                                                    throw new RuntimeException(e);
                                                }
                                            })));
                } catch (IOException e) {
                    logger.error(ERRORSTRING, e);
                    throw new RuntimeException(e);
                } catch (Exception e) {
                    logger.error(ERRORSTRING, e);
                }
            }
        }
        return columns;
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session,
                                            ConnectorTableHandle tableHandle,
                                            ColumnHandle columnHandle) {
        logger.debug("getColumnMetadata: url:{}\n\n\nindex:{}",
                this.qwUrl,
                this.qwIndex);
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

    //    @Override
//    public Optional<TableFunctionApplicationResult<ConnectorTableHandle>> applyTableFunction(
//            ConnectorSession session,
//            ConnectorTableFunctionHandle handle)
//    {
//        if (!(handle instanceof RawQuery.RawQueryFunction.RawQueryFunctionHandle)) {
//            return Optional.empty();
//        }
//        RawQuery.RawQueryFunction.RawQueryFunctionHandle h = (RawQuery.RawQueryFunction.RawQueryFunctionHandle) handle;
//        // Convert function invocation to a special table handle you already support in getSplits()
//        ConnectorTableHandle tableHandle = h.getTableHandle();
//
//        return Optional.of(new TableFunctionApplicationResult<>(tableHandle, List.of()));
//    }
    @Override
    public Optional<TableFunctionApplicationResult<ConnectorTableHandle>> applyTableFunction(ConnectorSession session, ConnectorTableFunctionHandle handle) {
        if (!(handle instanceof RawQuery.RawQueryFunction.RawQueryFunctionHandle)) {
            return Optional.empty();
        }
        RawQuery.RawQueryFunction.RawQueryFunctionHandle rawQueryFunctionHandle = (RawQuery.RawQueryFunction.RawQueryFunctionHandle) handle;
        ConnectorTableHandle tableHandle = rawQueryFunctionHandle.getTableHandle();
        List<ColumnHandle> columnHandles = getColumnHandles(session, tableHandle).values().stream()
                .sorted(Comparator.comparingInt(h -> ((UlakColumnHandle) h).getOrdinalPosition()))
                .collect(ImmutableList.toImmutableList());
        return Optional.of(new TableFunctionApplicationResult<>(tableHandle, columnHandles));
    }

    public Integer getConnectTimeout() {
        return connectTimeout;
    }

    public Integer getReadTimeout() {
        return readTimeout;
    }

    public Integer getWriteTimeout() {
        return writeTimeout;
    }
}
