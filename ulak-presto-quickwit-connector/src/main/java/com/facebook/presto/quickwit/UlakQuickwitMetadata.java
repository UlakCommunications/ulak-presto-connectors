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
import io.airlift.slice.Slice;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.*;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
    private final Set<String> allowedQwUrls;
    private static final String ERRORSTRING = "UlakQuickwitMetadata.java Error: {}";

    public UlakQuickwitMetadata(String catalogName, String qwUrl, String qwIndex,
                                Integer connectTimeout,
                                Integer readTimeout,
                                Integer writeTimeout) {
        this(catalogName, qwUrl, qwIndex, connectTimeout, readTimeout, writeTimeout, null);
    }

    public UlakQuickwitMetadata(String catalogName, String qwUrl, String qwIndex,
                                Integer connectTimeout,
                                Integer readTimeout,
                                Integer writeTimeout,
                                String allowedUrlsCsv) {
        this.qwUrl = qwUrl;
        this.qwIndex = qwIndex;
        this.connectTimeout = connectTimeout;
        this.readTimeout = readTimeout;
        this.writeTimeout = writeTimeout;
        this.setQwUrl(qwUrl);
        this.setQwIndex(qwIndex);
        this.connectorId = new UlakConnectorId(catalogName).toString();
        Set<String> allowed = new HashSet<>();
        if (qwUrl != null && !qwUrl.isBlank()) allowed.add(qwUrl.stripTrailing().replaceAll("/+$", ""));
        if (allowedUrlsCsv != null && !allowedUrlsCsv.isBlank() && !"*".equals(allowedUrlsCsv.trim())) {
            Arrays.stream(allowedUrlsCsv.split(","))
                  .map(String::trim)
                  .filter(s -> !s.isEmpty())
                  .forEach(allowed::add);
        }
        this.allowedQwUrls = allowedUrlsCsv != null && "*".equals(allowedUrlsCsv.trim())
                ? Collections.emptySet()
                : Collections.unmodifiableSet(allowed);
    }

    String validateQwUrl(String url) {
        if (StringUtils.isBlank(url)) return this.qwUrl;
        String normalized = url.stripTrailing().replaceAll("/+$", "");
        if (!allowedQwUrls.isEmpty() && !allowedQwUrls.contains(normalized)) {
            throw new TrinoException(StandardErrorCode.PERMISSION_DENIED,
                    "qwurl '" + url + "' is not in the catalog allowlist. " +
                    "Set qw-allowed-urls in the catalog config to permit additional URLs.");
        }
        return url;
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
        // Trino 479 base32-encodes long table names that contain special characters
        // (newlines, curly braces, double-quotes from embedded JSON/params).
        // Decode back to the original query-in-table-name string so that downstream
        // code (isPlainTableMode, QueryParameters parsing) sees the real content.
        String rawName = decodeBase32IfNeeded(tableName.getTableName());
        return new UlakTableHandle(connectorId, tableName.getSchemaName(), rawName);
    }

    // Standard base32 alphabet (RFC 4648), lowercase — used to detect Trino-encoded table names.
    private static final String BASE32_ALPHA = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

    /**
     * Trino 479 base32-encodes long table names containing special characters
     * (newlines, quotes, braces from embedded JSON or //param= directives).
     * Decode back to the original string so downstream code sees real content.
     * Heuristic: if the name is ≥64 chars and composed only of base32 chars + '=',
     * attempt decode; return original if decode fails or doesn't look like a query.
     */
    private static String decodeBase32IfNeeded(String name) {
        if (name == null || name.length() < 64) return name;
        if (name.contains("/") || name.contains("{") || name.contains("\n")) return name;
        // Quick charset check — every char must be in base32 alphabet or '='
        String stripped = name.endsWith("=") ? name.replaceAll("=+$", "") : name;
        for (int i = 0; i < stripped.length(); i++) {
            if (BASE32_ALPHA.indexOf(Character.toUpperCase(stripped.charAt(i))) < 0) return name;
        }
        try {
            byte[] decoded = base32Decode(stripped.toUpperCase());
            String result = new String(decoded, java.nio.charset.StandardCharsets.UTF_8);
            // Sanity check: decoded result should look like a query-in-table-name
            return result.contains("//") || result.contains("{") ? result : name;
        } catch (Exception ignored) {
            return name;
        }
    }

    private static byte[] base32Decode(String input) {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        int buf = 0, bitsLeft = 0;
        for (int i = 0; i < input.length(); i++) {
            int val = BASE32_ALPHA.indexOf(input.charAt(i));
            if (val < 0) throw new IllegalArgumentException("Invalid base32: " + input.charAt(i));
            buf = (buf << 5) | val;
            bitsLeft += 5;
            if (bitsLeft >= 8) {
                bitsLeft -= 8;
                out.write((buf >> bitsLeft) & 0xFF);
            }
        }
        return out.toByteArray();
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
                // L14: use frozen column list if available — avoids a second live search
                // that could return different columns and trigger "returned table mismatch"
                if (raw.getComputedColumns().isPresent() && !raw.getComputedColumns().get().isBlank()) {
                    list = columnsFromCsv(raw.getComputedColumns().get());
                    return new ConnectorTableMetadata(new SchemaTableName(DEFAULT_SCHEMA, buildSearchRequestJson(raw)), list);
                }
                tableName = buildSearchRequestJson(raw);
            }else {
                influxdbTableHandle = (UlakTableHandle) table;
                tableName =  influxdbTableHandle.getTableName();
            }
            logger.debug("getTableMetadata tableName: tableName:{}",
                    tableName);

            // J56: plain table mode — schema from DocMapping, skip live query
            if (QwUtil.isPlainTableMode(tableName)) {
                try {
                    list = QwUtil.getColumnsFromDocMapping(tableName, this.qwUrl, connectTimeout, readTimeout, writeTimeout);
                } catch (ApiException e) {
                    throw new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, e);
                }
                String schema = (raw == null && influxdbTableHandle != null)
                        ? influxdbTableHandle.getSchemaName() : DEFAULT_SCHEMA;
                return new ConnectorTableMetadata(new SchemaTableName(schema, tableName), list);
            }

            QueryParameters qp = QueryParameters.getQueryParameters(tableName);

            qp.setQwUrl(validateQwUrl(qp.getQwUrl()));
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
                // L14: use frozen column list if available
                if (raw.getComputedColumns().isPresent() && !raw.getComputedColumns().get().isBlank()) {
                    list = columnsFromCsv(raw.getComputedColumns().get());
                    logger.debug("getColumnHandles: using computedColumns ({} cols)", list.size());
                } else {
                    tableName = buildSearchRequestJson(raw);
                }
            }else{
                UlakTableHandle influxdbTableHandle = (UlakTableHandle) tableHandle;
                tableName = influxdbTableHandle.getTableName();
            }

            if (list == null) {
                list = getColumnsInternal(tableName, this.qwUrl, this.qwIndex, connectTimeout, readTimeout, writeTimeout);
            }

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

        // J56: plain table mode — schema from DocMapping, skip live query
        if (QwUtil.isPlainTableMode(tableName)) {
            try {
                return QwUtil.getColumnsFromDocMapping(tableName, qwUrl, connectTimeout, readTimeout, writeTimeout);
            } catch (ApiException e) {
                throw new IOException("Failed to get DocMapping for index " + tableName, e);
            }
        }

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
            if (!prefix.getTable().isPresent() || tableName.getTableName().startsWith(prefix.getTable().get())) {
                try {

                    QueryParameters qp = QueryParameters.getQueryParameters(tableName.getTableName());

                    qp.setQwUrl(validateQwUrl(qp.getQwUrl()));
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
        RawQuickwitQueryTableHandle rawTableHandle = (RawQuickwitQueryTableHandle) rawQueryFunctionHandle.getTableHandle();

        List<ColumnHandle> columnHandles;
        // L14: use frozen column list from analyze() to avoid a diverging second live search
        if (rawTableHandle.getComputedColumns().isPresent() && !rawTableHandle.getComputedColumns().get().isBlank()) {
            List<ColumnMetadata> cols = columnsFromCsv(rawTableHandle.getComputedColumns().get());
            columnHandles = IntStream.range(0, cols.size())
                    .mapToObj(i -> (ColumnHandle) new UlakColumnHandle(
                            connectorId, cols.get(i).getName(), cols.get(i).getType(), i))
                    .sorted(Comparator.comparingInt(h -> ((UlakColumnHandle) h).getOrdinalPosition()))
                    .collect(ImmutableList.toImmutableList());
        } else {
            columnHandles = getColumnHandles(session, rawTableHandle).values().stream()
                    .sorted(Comparator.comparingInt(h -> ((UlakColumnHandle) h).getOrdinalPosition()))
                    .collect(ImmutableList.toImmutableList());
        }
        return Optional.of(new TableFunctionApplicationResult<>(rawTableHandle, columnHandles));
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

    // -----------------------------------------------------------------------
    // L14 — helper: reconstruct ColumnMetadata list from frozen CSV string
    // -----------------------------------------------------------------------

    private static List<ColumnMetadata> columnsFromCsv(String csv) {
        AtomicInteger noDataIndex = new AtomicInteger(0);
        return Arrays.stream(csv.split(","))
                .map(t -> StringUtils.isBlank(t)
                        ? new ColumnMetadata("no-data-" + noDataIndex.getAndIncrement(), VarcharType.VARCHAR)
                        : new ColumnMetadata(t, VarcharType.VARCHAR))
                .collect(Collectors.toList());
    }

    // -----------------------------------------------------------------------
    // J56b — filter and limit pushdown for plain table mode
    // -----------------------------------------------------------------------

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session, ConnectorTableHandle handle, Constraint constraint) {
        if (!(handle instanceof UlakTableHandle)) return Optional.empty();
        UlakTableHandle tableHandle = (UlakTableHandle) handle;
        String tableName = tableHandle.getTableName();
        if (!QwUtil.isPlainTableMode(tableName)) return Optional.empty();

        TupleDomain<ColumnHandle> summary = constraint.getSummary();
        if (summary.isAll()) return Optional.empty();
        if (summary.isNone()) return Optional.empty();

        // Convert predicates we understand; skip the rest (Trino will re-apply them)
        TupleDomain<ColumnHandle> pushed = TupleDomain.all();
        TupleDomain<ColumnHandle> remaining = summary;
        String qwFilter = buildQwFilter(summary);
        if (qwFilter == null) return Optional.empty();

        pushed = summary;
        remaining = TupleDomain.all();

        String newTableName = PlainTableQuery.buildFilteredQuery(tableName, qwFilter, 1000);
        UlakTableHandle newHandle = new UlakTableHandle(
                tableHandle.getConnectorId(), tableHandle.getSchemaName(), newTableName);
        return Optional.of(new ConstraintApplicationResult<>(
                newHandle, remaining, constraint.getExpression(), false));
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(
            ConnectorSession session, ConnectorTableHandle handle, long limit) {
        if (!(handle instanceof UlakTableHandle)) return Optional.empty();
        UlakTableHandle tableHandle = (UlakTableHandle) handle;
        String tableName = tableHandle.getTableName();

        int maxHits = (int) Math.min(limit, Integer.MAX_VALUE);
        String newTableName;
        if (QwUtil.isPlainTableMode(tableName)) {
            newTableName = PlainTableQuery.buildFilteredQuery(tableName, "*", maxHits);
        } else if (tableName.contains("//") && tableName.contains("max_hits")) {
            newTableName = PlainTableQuery.withMaxHits(tableName, maxHits);
        } else {
            return Optional.empty();
        }

        UlakTableHandle newHandle = new UlakTableHandle(
                tableHandle.getConnectorId(), tableHandle.getSchemaName(), newTableName);
        return Optional.of(new LimitApplicationResult<>(newHandle, true, false));
    }

    /**
     * Converts a TupleDomain to a Quickwit query string (Lucene-like syntax).
     * Returns null if any predicate cannot be expressed in Quickwit syntax.
     * Simple equality and single-range predicates are supported.
     */
    private static String buildQwFilter(TupleDomain<ColumnHandle> tupleDomain) {
        if (!tupleDomain.getDomains().isPresent()) return null;
        Map<ColumnHandle, Domain> domains = tupleDomain.getDomains().get();
        if (domains.isEmpty()) return null;

        List<String> clauses = new ArrayList<>();
        for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
            UlakColumnHandle col = (UlakColumnHandle) entry.getKey();
            String clause = domainToQwClause(col.getColumnName(), entry.getValue());
            if (clause == null) return null; // can't push down this predicate
            clauses.add(clause);
        }
        return clauses.isEmpty() ? null : String.join(" AND ", clauses);
    }

    private static String domainToQwClause(String field, Domain domain) {
        if (domain.isAll()) return null;
        if (domain.isNone()) return null;
        if (domain.isSingleValue()) {
            return field + ":" + qwLiteral(domain.getSingleValue(), domain.getType());
        }
        ValueSet valueSet = domain.getValues();
        if (valueSet instanceof SortedRangeSet) {
            List<Range> ranges = ((SortedRangeSet) valueSet).getOrderedRanges();
            if (ranges.size() == 1) {
                Range range = ranges.get(0);
                if (range.isSingleValue()) {
                    return field + ":" + qwLiteral(range.getSingleValue(), range.getType());
                }
                String low = range.getLowValue()
                        .map(v -> qwLiteral(v, range.getType())).orElse("*");
                String high = range.getHighValue()
                        .map(v -> qwLiteral(v, range.getType())).orElse("*");
                return field + ":[" + low + " TO " + high + "]";
            }
        }
        return null; // multi-range or other complex predicate — don't push down
    }

    private static String qwLiteral(Object val, Type type) {
        if (val instanceof Slice) {
            String s = ((Slice) val).toStringUtf8();
            return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
        }
        return String.valueOf(val);
    }
}
