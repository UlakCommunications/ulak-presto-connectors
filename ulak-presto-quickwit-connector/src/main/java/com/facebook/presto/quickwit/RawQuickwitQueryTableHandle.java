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
    private final boolean cache;

    @JsonCreator
    public RawQuickwitQueryTableHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("index") String index,
            @JsonProperty("query") String query,
            @JsonProperty("startTimestamp") Optional<Long> startTimestamp,
            @JsonProperty("endTimestamp") Optional<Long> endTimestamp,
            @JsonProperty("maxHits") Optional<Integer> maxHits,
            @JsonProperty("aggsJson") Optional<String> aggsJson,
            @JsonProperty("cache") boolean cache)
    {
        this.connectorId = Objects.requireNonNull(connectorId, "connectorId is null");
        this.index = Objects.requireNonNull(index, "index is null");
        this.query = Objects.requireNonNull(query, "query is null");
        this.startTimestamp = Objects.requireNonNull(startTimestamp, "startTimestamp is null");
        this.endTimestamp = Objects.requireNonNull(endTimestamp, "endTimestamp is null");
        this.maxHits = Objects.requireNonNull(maxHits, "maxHits is null");
        this.aggsJson = Objects.requireNonNull(aggsJson, "aggsJson is null");
        this.cache = cache;
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
    public boolean isCache()
    {
        return cache;
    }

    // ---------------- identity & debug ----------------

    @Override
    public int hashCode()
    {
        return Objects.hash(
                connectorId,
                index,
                query,
                startTimestamp,
                endTimestamp,
                maxHits,
                aggsJson,
                cache);
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
        return cache == other.cache
                && connectorId.equals(other.connectorId)
                && index.equals(other.index)
                && query.equals(other.query)
                && startTimestamp.equals(other.startTimestamp)
                && endTimestamp.equals(other.endTimestamp)
                && maxHits.equals(other.maxHits)
                && aggsJson.equals(other.aggsJson);
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
                '}';
    }
}
