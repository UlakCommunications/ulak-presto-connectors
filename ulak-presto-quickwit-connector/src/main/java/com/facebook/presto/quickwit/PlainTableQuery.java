package com.facebook.presto.quickwit;

/**
 * J56 — plain-table-mode string utilities.
 *
 * Kept in a separate class with no Trino SPI dependency so that unit tests
 * can load and call these methods on Java 24 without triggering
 * UnsupportedClassVersionError from trino-spi-479 (compiled for Java 25).
 */
final class PlainTableQuery {

    private PlainTableQuery() {}

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
        return "//qwindex=" + indexName + "\n" +
               "//dbtype=qw\n" +
               "//sqlversion=0.2\n" +
               "{\"query\":\"*\",\"max_hits\":1000}";
    }
}
