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
import com.facebook.presto.ulak.UlakTableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.airlift.slice.Slice;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnSchema;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableSchema;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;

import java.util.*;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class RawQuery
        implements Provider<ConnectorTableFunction> {
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
                            ScalarArgumentSpecification.builder().name("qwindex").type(VARCHAR).build(),
                            ScalarArgumentSpecification.builder().name("query").type(VARCHAR).build(),

                            // optional-ish: you can still define them and allow nulls by not providing them in SQL
                            ScalarArgumentSpecification.builder().name("start_timestamp").type(VARCHAR).build(),
                            ScalarArgumentSpecification.builder().name("end_timestamp").type(VARCHAR).build(),
                            ScalarArgumentSpecification.builder().name("max_hits").type(VARCHAR).build(),

                            ScalarArgumentSpecification.builder().name("aggs").type(VARCHAR).build(),
                            ScalarArgumentSpecification.builder().name("cache").type(VARCHAR).build()
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
            // NOTE: use argument names that match your SQL: qwindex, query, max_hits, start_timestamp, end_timestamp, aggs, cache
            String index = getRequiredVarchar(arguments, "qwindex");
            String query = getOptionalVarchar(arguments, "query").orElse("*");

            OptionalLong startTs = getOptionalVarchar(arguments, "start_timestamp").map(RawQueryFunction::parseLongSafely).orElse(OptionalLong.empty());
            OptionalLong endTs = getOptionalVarchar(arguments, "end_timestamp").map(RawQueryFunction::parseLongSafely).orElse(OptionalLong.empty());

            OptionalInt maxHits = getOptionalVarchar(arguments, "max_hits").map(RawQueryFunction::parseIntSafely).orElse(OptionalInt.empty());
            Optional<String> aggsJson = getOptionalVarchar(arguments, "aggs");
            boolean cache = getOptionalVarchar(arguments, "cache").map(Boolean::parseBoolean).orElse(false);

            // Your own ConnectorTableHandle that stores raw-query params
            RawQuickwitQueryTableHandle tableHandle = new RawQuickwitQueryTableHandle(
                    UlakQuickwitMetadata.connectorId,
                    index,
                    query,
                    startTs.isPresent() ? Optional.of(startTs.getAsLong()) : Optional.empty(),
                    endTs.isPresent() ? Optional.of(endTs.getAsLong()) : Optional.empty(),
                    maxHits.isPresent() ? Optional.of(maxHits.getAsInt()) : Optional.empty(),
                    aggsJson,
                    cache);

            // Stable return type (recommended)
            Descriptor returnedType = new Descriptor(List.of(
                    new Descriptor.Field("hits_json", Optional.of(VARCHAR)),
                    new Descriptor.Field("aggs_json", Optional.of(VARCHAR)),
                    new Descriptor.Field("elapsed_ms", Optional.of(io.trino.spi.type.BigintType.BIGINT))
            ));

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
