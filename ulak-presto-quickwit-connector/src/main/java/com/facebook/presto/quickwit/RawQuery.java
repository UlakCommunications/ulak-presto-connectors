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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.connector.*;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

import static com.facebook.presto.quickwit.QuickwitRecordSetProvider.buildSearchRequestJson;
import static com.facebook.presto.quickwit.UlakQuickwitMetadata.getColumnsInternal;
import static io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class RawQuery
        implements Provider<ConnectorTableFunction> {

    private static Logger logger = LoggerFactory.getLogger(QwUtil.class);
    public static final String SCHEMA_NAME = "system";
    public static final String NAME = "raw_query";

    private final UlakQuickwitMetadata metadata;

    @Inject
    public RawQuery(UlakQuickwitMetadata metadata) {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public ConnectorTableFunction get() {
        return new RawQueryFunction(metadata);
    }

    public static class RawQueryFunction
            extends AbstractConnectorTableFunction {
        private final UlakQuickwitMetadata metadata;

        public RawQueryFunction(UlakQuickwitMetadata metadata) {
            super(
                    SCHEMA_NAME,
                    NAME,
                    List.of(
                            ScalarArgumentSpecification.builder().name("query").type(VARCHAR).defaultValue(Slices.utf8Slice("*")).build(),
                            ScalarArgumentSpecification.builder().name("qwindex").type(VARCHAR).build(),
                            ScalarArgumentSpecification.builder().name("start_timestamp").type(VARCHAR).defaultValue(Slices.utf8Slice("")).build(),
                            ScalarArgumentSpecification.builder().name("end_timestamp").type(VARCHAR).defaultValue(Slices.utf8Slice("")).build(),
                            ScalarArgumentSpecification.builder().name("max_hits").type(VARCHAR).defaultValue(Slices.utf8Slice("1000")).build(),
                            ScalarArgumentSpecification.builder().name("aggs").type(VARCHAR).defaultValue(Slices.utf8Slice("")).build(),
                            ScalarArgumentSpecification.builder().name("cache").type(VARCHAR).defaultValue(Slices.utf8Slice("false")).build(),

                            ScalarArgumentSpecification.builder().name("name").type(VARCHAR).defaultValue(Slices.utf8Slice("no-name")).build(),
                            ScalarArgumentSpecification.builder().name("columns").type(VARCHAR).defaultValue(Slices.utf8Slice("no-data")).build(),
                            ScalarArgumentSpecification.builder().name("dbtype").type(VARCHAR).defaultValue(Slices.utf8Slice("qw")).build(),
                            ScalarArgumentSpecification.builder().name("replacefromcolumns").type(VARCHAR).defaultValue(Slices.utf8Slice("no-data")).build(),
                            ScalarArgumentSpecification.builder().name("hasjs").type(VARCHAR).defaultValue(Slices.utf8Slice("false")).build(),
                            ScalarArgumentSpecification.builder().name("sqlversion").type(VARCHAR).defaultValue(Slices.utf8Slice("0")).build()
                    ),
                    GENERIC_TABLE
            );
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        @Override
        public TableFunctionAnalysis analyze(
                ConnectorSession session,
                ConnectorTransactionHandle transaction,
                Map<String, Argument> arguments,
                ConnectorAccessControl accessControl) {

            Instant now = Instant.now();                 // UTC now
            Instant start = now.minus(15, ChronoUnit.MINUTES);

            long startTsDefault = start.getEpochSecond();       // veya toEpochMilli()
            long endTsDefault   = now.getEpochSecond();

            String index = getRequiredVarchar(arguments, "qwindex");
            String query = getOptionalVarchar(arguments, "query").orElse("*");

            OptionalLong startTs = getOptionalVarchar(arguments, "start_timestamp").map(RawQueryFunction::parseLongSafely).orElse(OptionalLong.of(startTsDefault));
            OptionalLong endTs = getOptionalVarchar(arguments, "end_timestamp").map(RawQueryFunction::parseLongSafely).orElse(OptionalLong.of(endTsDefault));

            OptionalInt maxHits = getOptionalVarchar(arguments, "max_hits").map(RawQueryFunction::parseIntSafely).orElse(OptionalInt.empty());
            Optional<String> aggsJson = getOptionalVarchar(arguments, "aggs");
            boolean cache = getOptionalVarchar(arguments, "cache").map(Boolean::parseBoolean).orElse(false);

            String name = getOptionalVarchar(arguments, "name").orElse("*");
            String columns = getOptionalVarchar(arguments, "columns").orElse("");
            String dbtype = getOptionalVarchar(arguments, "dbtype").orElse("");
            String replacefromcolumns = getOptionalVarchar(arguments, "replacefromcolumns").orElse("");
            String hasjs = getOptionalVarchar(arguments, "hasjs").orElse("false");
            String sqlversion = getOptionalVarchar(arguments, "sqlversion").orElse("0");

            // Your own ConnectorTableHandle that stores raw-query params
            RawQuickwitQueryTableHandle tableHandle = new RawQuickwitQueryTableHandle(
                    UlakQuickwitMetadata.connectorId,
                    index,
                    query,
                    startTs.isPresent() ? Optional.of(startTs.getAsLong()) : Optional.empty(),
                    endTs.isPresent() ? Optional.of(endTs.getAsLong()) : Optional.empty(),
                    maxHits.isPresent() ? Optional.of(maxHits.getAsInt()) : Optional.empty(),
                    aggsJson,
                    Optional.of(cache),
                    Optional.of(name),
                    Optional.of(columns),
                    Optional.of(dbtype),
                    Optional.of(replacefromcolumns),
                    Optional.of(hasjs),
                    Optional.of(sqlversion));

            // Stable return type (recommended)
            String tmpCls = null;

            try {
                tmpCls = String.join(",", getColumnsInternal(buildSearchRequestJson(tableHandle),
                        metadata.getQwUrl(),
                        metadata.getQwIndex(),
                        metadata.getConnectTimeout(),
                        metadata.getConnectTimeout(),
                        metadata.getConnectTimeout()).stream().map(t->t.getName()).collect(Collectors.toList()));
            } catch (IOException e) {
                tmpCls = columns;
            }
            int noDataIndex=0;
            Descriptor returnedType = new Descriptor(Arrays.stream(tmpCls.split(",")).map(t->new Descriptor.Field(StringUtils.isEmpty(t) || StringUtils.isBlank(t) ? "no-data-" + noDataIndex : t, Optional.of(VARCHAR))).collect(Collectors.toList()));


            RawQueryFunctionHandle handle = new RawQueryFunctionHandle(tableHandle);

            return TableFunctionAnalysis.builder()
                    .returnedType(returnedType)
                    .handle(handle)
                    .build();
        }

        private static String getRequiredVarchar(Map<String, Argument> args, String name) {
            return getOptionalVarchar(args, name)
                    .orElseThrow(() -> new IllegalArgumentException("Missing required argument: " + name));
        }

        private static Optional<String> getOptionalVarchar(Map<String, Argument> args, String name) {
            Argument arg = args.get(name);
            if (arg == null) {
                return Optional.empty();
            }
            Slice slice = (Slice) ((ScalarArgument) arg).getValue();
            if (slice == null) {
                return Optional.empty();
            }
            String s = slice.toStringUtf8().trim();
            if (s.isEmpty() || s.equalsIgnoreCase("null")) {
                return Optional.empty();
            }
            return Optional.of(s);
        }

        private static OptionalLong parseLongSafely(String s) {
            try {
                return OptionalLong.of(Long.parseLong(s.trim()));
            } catch (RuntimeException e) {
                // treat parse failures as "not provided"
                return OptionalLong.empty();
            }
        }

        private static OptionalInt parseIntSafely(String s) {
            try {
                return OptionalInt.of(Integer.parseInt(s.trim()));
            } catch (RuntimeException e) {
                return OptionalInt.empty();
            }
        }


        public static class RawQueryFunctionHandle
                implements ConnectorTableFunctionHandle {
            private final RawQuickwitQueryTableHandle tableHandle;

            @JsonCreator
            public RawQueryFunctionHandle(@JsonProperty("tableHandle") RawQuickwitQueryTableHandle tableHandle) {
                this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
            }

            @JsonProperty
            public ConnectorTableHandle getTableHandle() {
                return tableHandle;
            }
        }
    }
}
