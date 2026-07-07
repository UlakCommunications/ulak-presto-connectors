package com.facebook.presto.quickwit;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTableHandle;

import java.util.Objects;
import java.util.Optional;

/**
 * Table handle for qw.system.raw_query(...)
 *
 * Represents a single Quickwit search request.
 * One logical "table" == one request == typically one output row.
 */
public final class RawQuickwitQueryTableHandle implements ConnectorTableHandle
{
    private final String connectorId;
    private final String index;
    private final String query;

    private final Optional<Long> startTimestamp;
    private final Optional<Long> endTimestamp;
    private final Optional<Integer> maxHits;

    private final Optional<String> aggsJson;
    private final Optional<Boolean> cache;
    private final Optional<String> name;
    private final Optional<String> columns;
    private final Optional<String> dbtype;
    private final Optional<String> replacefromcolumns;
    private final Optional<String> hasjs;
    private final Optional<String> sqlversion;
    private final Optional<Boolean> timestampEnabled;
    /**
     * L14: column names frozen at {@code analyze()} time (comma-separated).
     * When present, {@code getTableMetadata()} and {@code getColumnHandles()} use this
     * instead of running another Quickwit search, preventing "returned table does not
     * match the node's output" caused by diverging responses between planning calls.
     * Absent on handles created before this fix (old serialised plans) — falls back to
     * the live-query path.
     */
    private final Optional<String> computedColumns;

    @JsonCreator
    public RawQuickwitQueryTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("index") String index,
            @JsonProperty("query") String query,
            @JsonProperty("startTimestamp") Optional<Long> startTimestamp,
            @JsonProperty("endTimestamp") Optional<Long> endTimestamp,
            @JsonProperty("maxHits") Optional<Integer> maxHits,
            @JsonProperty("aggsJson") Optional<String> aggsJson,
            @JsonProperty("cache") Optional<Boolean> cache,
            @JsonProperty("name") Optional<String> name,
            @JsonProperty("columns") Optional<String> columns,
            @JsonProperty("dbtype") Optional<String> dbtype,
            @JsonProperty("replacefromcolumns") Optional<String> replacefromcolumns,
            @JsonProperty("hasjs") Optional<String> hasjs,
            @JsonProperty("sqlversion") Optional<String> sqlversion,
            @JsonProperty("computedColumns") Optional<String> computedColumns,
            @JsonProperty("timestampEnabled") Optional<Boolean> timestampEnabled)
    {
        this.connectorId = Objects.requireNonNull(connectorId, "connectorId is null");
        this.index = Objects.requireNonNull(index, "index is null");
        this.query = Objects.requireNonNull(query, "query is null");
        this.startTimestamp = Objects.requireNonNull(startTimestamp, "startTimestamp is null");
        this.endTimestamp = Objects.requireNonNull(endTimestamp, "endTimestamp is null");
        this.maxHits = Objects.requireNonNull(maxHits, "maxHits is null");
        this.aggsJson = Objects.requireNonNull(aggsJson, "aggsJson is null");
        this.cache = cache;
        this.name = name;
        this.columns = columns;
        this.dbtype = dbtype;
        this.replacefromcolumns = replacefromcolumns;
        this.hasjs = hasjs;
        this.sqlversion = sqlversion;
        this.computedColumns = computedColumns != null ? computedColumns : Optional.empty();
        this.timestampEnabled = timestampEnabled != null ? timestampEnabled : Optional.empty();
    }

    // ---------------- getters ----------------

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getIndex()
    {
        return index;
    }

    @JsonProperty
    public String getQuery()
    {
        return query;
    }

    @JsonProperty
    public Optional<Long> getStartTimestamp()
    {
        return startTimestamp;
    }

    @JsonProperty
    public Optional<Long> getEndTimestamp()
    {
        return endTimestamp;
    }

    @JsonProperty
    public Optional<Integer> getMaxHits()
    {
        return maxHits;
    }

    @JsonProperty
    public Optional<String> getAggsJson()
    {
        return aggsJson;
    }

    @JsonProperty
    public Optional<Boolean> isCache()
    {
        return cache;
    }

    // ---------------- identity & debug ----------------

    @Override
    public int hashCode()
    {
        return Objects.hash(
                connectorId, index, query,
                startTimestamp, endTimestamp, maxHits, aggsJson,
                cache, name, columns, dbtype, replacefromcolumns, hasjs, sqlversion,
                computedColumns, timestampEnabled);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        RawQuickwitQueryTableHandle other = (RawQuickwitQueryTableHandle) obj;
        return cache.equals(other.cache)
                && connectorId.equals(other.connectorId)
                && index.equals(other.index)
                && query.equals(other.query)
                && startTimestamp.equals(other.startTimestamp)
                && endTimestamp.equals(other.endTimestamp)
                && maxHits.equals(other.maxHits)
                && aggsJson.equals(other.aggsJson)
                && name.equals(other.name)
                && columns.equals(other.columns)
                && dbtype.equals(other.dbtype)
                && replacefromcolumns.equals(other.replacefromcolumns)
                && hasjs.equals(other.hasjs)
                && sqlversion.equals(other.sqlversion)
                && computedColumns.equals(other.computedColumns)
                && timestampEnabled.equals(other.timestampEnabled);
    }

    @Override
    public String toString()
    {
        return "RawQuickwitQueryTableHandle{" +
                "index='" + index + '\'' +
                ", query='" + query + '\'' +
                ", startTimestamp=" + startTimestamp +
                ", endTimestamp=" + endTimestamp +
                ", maxHits=" + maxHits +
                ", aggsJsonPresent=" + aggsJson.isPresent() +
                ", cache=" + cache +
                ", name=" + name +
                ", columns=" + columns +
                ", dbtype=" + dbtype +
                ", replacefromcolumns=" + replacefromcolumns +
                ", hasjs=" + hasjs +
                ", sqlversion=" + sqlversion +
                ", computedColumnsPresent=" + computedColumns.isPresent() +
                ", timestampEnabled=" + timestampEnabled +
                '}';
    }

    @JsonProperty
    public Optional<String> getName() {
        return name;
    }

    @JsonProperty
    public Optional<String> getColumns() {
        return columns;
    }

    @JsonProperty
    public Optional<String> getDbtype() {
        return dbtype;
    }

    @JsonProperty
    public Optional<String> getReplacefromcolumns() {
        return replacefromcolumns;
    }

    @JsonProperty
    public Optional<String> getHasjs() {
        return hasjs;
    }

    @JsonProperty
    public Optional<String> getSqlversion() {
        return sqlversion;
    }

    @JsonProperty
    public Optional<String> getComputedColumns() {
        return computedColumns;
    }

    @JsonProperty
    public Optional<Boolean> getTimestampEnabled() {
        return timestampEnabled;
    }
}
