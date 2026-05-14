package com.facebook.presto.quickwit;

import com.google.common.io.BaseEncoding;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * J56 + QW9-03 — Plain Table Query Mode unit tests.
 *
 * Calls PlainTableQuery directly (not QwUtil) to avoid loading trino-spi-479
 * (compiled for Java 25) on a Java 24 test JVM.
 */
@DisplayName("J56 + QW9-03 — Plain Table Query Mode + base32 decode")
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

    // -----------------------------------------------------------------------
    // QW9-03 — decodeIfBase32Encoded (Grafana plugin base32 regression fix)
    // -----------------------------------------------------------------------

    /** Simulate what hi-base32 (Grafana plugin) produces for a query string. */
    private static String hiBase32Encode(String plain) {
        // hi-base32 uses RFC 4648: uppercase A-Z 2-7 with = padding → always length%8==0
        return BaseEncoding.base32().encode(plain.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("QW9-03: encoded query-in-table-name is decoded back to original")
    void encodedQueryIsDecoded() {
        String original = "\n    //ttl=172800\n    //columns=/buckets/2/a/key\n    {\"query\":\"*\"}";
        String encoded  = hiBase32Encode(original);

        assertThat(encoded).doesNotContain("//");
        assertThat(encoded.length() % 8).isZero();

        String decoded = PlainTableQuery.decodeIfBase32Encoded(encoded);
        assertThat(decoded).isEqualTo(original);
    }

    @Test
    @DisplayName("QW9-03: decoded name is NOT plain mode (contains //)")
    void decodedNameIsNotPlainMode() {
        String original = "\n    //ttl=172800\n    //columns=/buckets/2/a/key\n    {\"query\":\"*\"}";
        String encoded  = hiBase32Encode(original);

        String decoded = PlainTableQuery.decodeIfBase32Encoded(encoded);
        assertThat(PlainTableQuery.isPlainMode(decoded))
                .as("decoded query contains // so must not be flagged as plain mode")
                .isFalse();
    }

    @Test
    @DisplayName("QW9-03: already-decoded name with // is returned unchanged")
    void alreadyDecodedNameReturnedUnchanged() {
        String query = "//qwindex=otlp_metric\n{\"query\":\"*\"}";
        assertThat(PlainTableQuery.decodeIfBase32Encoded(query)).isEqualTo(query);
    }

    @Test
    @DisplayName("QW9-03: bare index name (not base32 query) is returned unchanged")
    void bareIndexNameReturnedUnchanged() {
        assertThat(PlainTableQuery.decodeIfBase32Encoded("metrics3")).isEqualTo("metrics3");
        assertThat(PlainTableQuery.decodeIfBase32Encoded("my-index")).isEqualTo("my-index");
    }

    @Test
    @DisplayName("QW9-03: null and empty are returned unchanged")
    void nullAndEmptyReturnedUnchanged() {
        assertThat(PlainTableQuery.decodeIfBase32Encoded(null)).isNull();
        assertThat(PlainTableQuery.decodeIfBase32Encoded("")).isEmpty();
    }

    @Test
    @DisplayName("QW9-03: string whose length is not multiple of 8 is not decoded")
    void nonMultipleOf8NotDecoded() {
        // 7 chars: length%8 != 0 → skip decode even if chars look like base32
        String s = "ABCDEFG";
        assertThat(PlainTableQuery.decodeIfBase32Encoded(s)).isEqualTo(s);
    }

    @Test
    @DisplayName("QW9-03: decoded bytes that don't contain // or { are not substituted")
    void decodedGarbageNotSubstituted() {
        // Encode a plain word that happens to be valid base32 length but decodes to no-// content
        String plain = "HELLO!!!";  // 8 chars, but canDecode will fail on '!'
        assertThat(PlainTableQuery.decodeIfBase32Encoded(plain)).isEqualTo(plain);
    }

    @Test
    @DisplayName("QW9-03: real view_interface_with_site_filter payload round-trips correctly")
    void viewInterfaceWithSiteFilterPayloadRoundTrips() {
        // Representative subset of the actual Postgres setting value
        String original =
            "\n    //ttl=172800\n" +
            "    //refresh=10\n" +
            "    //cache=false\n" +
            "    //name=Host Interface\n" +
            "    //sqlversion=0\n" +
            "    //columns=/buckets/2/a/key,/buckets/2/6/key_as_string\n" +
            "    {\"query\":\"*\",\"max_hits\":0}";
        String encoded = hiBase32Encode(original);

        assertThat(PlainTableQuery.decodeIfBase32Encoded(encoded)).isEqualTo(original);
        assertThat(PlainTableQuery.isPlainMode(
                PlainTableQuery.decodeIfBase32Encoded(encoded))).isFalse();
    }
}
