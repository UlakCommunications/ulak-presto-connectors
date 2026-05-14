package com.facebook.presto.quickwit;

import com.google.common.io.BaseEncoding;

import java.nio.charset.StandardCharsets;

/**
 * J56 — plain-table-mode string utilities.
 *
 * Kept in a separate class with no Trino SPI dependency so that unit tests
 * can load and call these methods on Java 24 without triggering
 * UnsupportedClassVersionError from trino-spi-479 (compiled for Java 25).
 */
final class PlainTableQuery {

    private PlainTableQuery() {}

    /**
     * Decodes a Grafana-plugin-encoded table name back to the original query string.
     *
     * The Grafana Trino plugin (datasource.ts encodeQuickwitQuery) base32-encodes the
     * content of double-quoted table names to survive Trino's identifier case-normalization.
     * It uses hi-base32 (RFC 4648, uppercase A-Z + 2-7, padded to multiples of 8 chars).
     * QueryParameters.getQueryParameters() decoded these correctly until J56 added the
     * isPlainMode() check before QueryParameters was called (QW9-03 regression fix).
     *
     * Conditions (same as QueryParameters.getQueryParameters()):
     *   - length % 8 == 0  (hi-base32 always pads to multiples of 8)
     *   - no "//", "\n", or " " in the encoded string
     *   - Guava canDecode() on uppercased input
     *   - decoded result contains "//" or "{" (sanity: it should look like a query)
     */
    static String decodeIfBase32Encoded(String tableName) {
        if (tableName == null || tableName.isEmpty()) return tableName;
        if (tableName.contains("//") || tableName.contains("\n") || tableName.contains(" ")) return tableName;
        if (tableName.length() % 8 != 0) return tableName;
        try {
            String upper = tableName.toUpperCase();
            if (!BaseEncoding.base32().canDecode(upper)) return tableName;
            byte[] decoded = BaseEncoding.base32().decode(upper);
            String result = new String(decoded, StandardCharsets.UTF_8);
            return (result.contains("//") || result.contains("{")) ? result : tableName;
        } catch (Exception ignored) {
            return tableName;
        }
    }

    /** Returns true when tableName is a bare index name (no //param= directives). */
    static boolean isPlainMode(String tableName) {
        return tableName != null && !tableName.isEmpty() && !tableName.contains("//");
    }

    /**
     * Builds a match-all query string for a plain index name so it can be
     * processed by the existing QueryParameters + QwUtil.select() pipeline.
     * Uses sqlversion=0.2 (bare aggId column names) as the default.
     */
    static String buildMatchAllQuery(String indexName) {
        return buildFilteredQuery(indexName, "*", 1000);
    }

    /**
     * Builds a query string with a custom Quickwit filter expression.
     * {@code qwFilter} is a Quickwit query string clause, e.g. {@code status:"ok"}.
     */
    static String buildFilteredQuery(String indexName, String qwFilter, int maxHits) {
        String escaped = qwFilter.replace("\\", "\\\\").replace("\"", "\\\"");
        return "//qwindex=" + indexName + "\n" +
               "//dbtype=qw\n" +
               "//sqlversion=0.2\n" +
               "{\"query\":\"" + escaped + "\",\"max_hits\":" + maxHits + "}";
    }

    /**
     * Returns a copy of {@code queryString} with the {@code "max_hits"} value replaced.
     * Only replaces the first occurrence (there is exactly one per query string).
     */
    static String withMaxHits(String queryString, int maxHits) {
        return queryString.replaceFirst("\"max_hits\"\\s*:\\s*\\d+", "\"max_hits\":" + maxHits);
    }
}
