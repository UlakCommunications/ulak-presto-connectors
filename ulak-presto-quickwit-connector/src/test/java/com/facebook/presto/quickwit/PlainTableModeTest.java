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
}
