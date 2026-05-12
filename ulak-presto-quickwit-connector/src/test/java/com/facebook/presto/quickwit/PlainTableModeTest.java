package com.facebook.presto.quickwit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * J56 — Plain Table Query Mode unit tests.
 *
 * Calls PlainTableQuery directly (not QwUtil) to avoid loading trino-spi-479
 * (compiled for Java 25) on a Java 24 test JVM.
 */
@DisplayName("J56 — Plain Table Query Mode")
class PlainTableModeTest {

    // -----------------------------------------------------------------------
    // isPlainMode
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("bare index name is detected as plain mode")
    void bareIndexNameIsPlainMode() {
        assertThat(PlainTableQuery.isPlainMode("metrics3")).isTrue();
        assertThat(PlainTableQuery.isPlainMode("my-index")).isTrue();
        assertThat(PlainTableQuery.isPlainMode("index_with_underscores")).isTrue();
    }

    @Test
    @DisplayName("table name with // directives is NOT plain mode")
    void tableNameWithDirectivesIsNotPlainMode() {
        assertThat(PlainTableQuery.isPlainMode("//qwindex=metrics3\n{\"query\":\"*\"}")).isFalse();
        assertThat(PlainTableQuery.isPlainMode("\n//qwurl=http://host:7280\n{}")).isFalse();
    }

    @Test
    @DisplayName("null and empty are not plain mode")
    void nullAndEmptyAreNotPlainMode() {
        assertThat(PlainTableQuery.isPlainMode(null)).isFalse();
        assertThat(PlainTableQuery.isPlainMode("")).isFalse();
    }

    // -----------------------------------------------------------------------
    // buildMatchAllQuery
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("buildMatchAllQuery embeds the index name")
    void buildMatchAllQueryEmbedsIndexName() {
        String q = PlainTableQuery.buildMatchAllQuery("metrics3");
        assertThat(q).contains("//qwindex=metrics3");
    }

    @Test
    @DisplayName("buildMatchAllQuery produces a match-all JSON body")
    void buildMatchAllQueryMatchAll() {
        String q = PlainTableQuery.buildMatchAllQuery("my-index");
        assertThat(q).contains("\"query\":\"*\"");
        assertThat(q).contains("max_hits");
    }

    @Test
    @DisplayName("buildMatchAllQuery uses sqlversion=0.2")
    void buildMatchAllQueryUsesSqlversion02() {
        String q = PlainTableQuery.buildMatchAllQuery("any");
        assertThat(q).contains("//sqlversion=0.2");
    }

    @Test
    @DisplayName("buildMatchAllQuery result is recognised as non-plain-mode")
    void buildMatchAllQueryResultIsNotPlainMode() {
        String q = PlainTableQuery.buildMatchAllQuery("metrics3");
        assertThat(PlainTableQuery.isPlainMode(q))
                .as("generated query string contains // so must not be flagged as plain mode")
                .isFalse();
    }

    @Test
    @DisplayName("index name with hyphens is handled correctly")
    void indexNameWithHyphenIsHandledCorrectly() {
        String q = PlainTableQuery.buildMatchAllQuery("my-index-v2");
        assertThat(q).contains("//qwindex=my-index-v2");
        assertThat(PlainTableQuery.isPlainMode(q)).isFalse();
    }

    // -----------------------------------------------------------------------
    // J56b — filter and limit helpers
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("buildFilteredQuery embeds the custom filter expression")
    void buildFilteredQueryEmbedsFilter() {
        String q = PlainTableQuery.buildFilteredQuery("metrics3", "status:ok", 500);
        assertThat(q).contains("//qwindex=metrics3");
        assertThat(q).contains("status:ok");
        assertThat(q).contains("\"max_hits\":500");
    }

    @Test
    @DisplayName("buildFilteredQuery escapes double-quotes in filter value")
    void buildFilteredQueryEscapesQuotes() {
        String q = PlainTableQuery.buildFilteredQuery("idx", "field:\"val\"", 100);
        assertThat(q).contains("field:\\\"val\\\"");
    }

    @Test
    @DisplayName("withMaxHits replaces the max_hits value in an existing query string")
    void withMaxHitsReplaces() {
        String original = PlainTableQuery.buildMatchAllQuery("metrics3");
        assertThat(original).contains("\"max_hits\":1000");

        String updated = PlainTableQuery.withMaxHits(original, 250);
        assertThat(updated).contains("\"max_hits\":250");
        assertThat(updated).doesNotContain("\"max_hits\":1000");
    }

    @Test
    @DisplayName("withMaxHits works on a filtered query string")
    void withMaxHitsOnFilteredQuery() {
        String filtered = PlainTableQuery.buildFilteredQuery("idx", "status:ok", 1000);
        String limited  = PlainTableQuery.withMaxHits(filtered, 50);
        assertThat(limited).contains("\"max_hits\":50");
        assertThat(limited).contains("status:ok");
    }

    @Test
    @DisplayName("withMaxHits preserves the filter expression and directives")
    void withMaxHitsPreservesDirectives() {
        String q = PlainTableQuery.buildFilteredQuery("my-idx", "host:web1", 1000);
        String limited = PlainTableQuery.withMaxHits(q, 10);
        assertThat(limited).contains("//qwindex=my-idx");
        assertThat(limited).contains("//sqlversion=0.2");
        assertThat(limited).contains("host:web1");
    }
}
