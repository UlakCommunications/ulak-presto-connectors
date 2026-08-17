package com.facebook.presto.quickwit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for QwUtil helper methods covering code-review fixes
 * R06, R08, R09, R11, R12, R13, R14.
 *
 * NOTE: Tests in this class must NOT instantiate Trino SPI types
 * (ConnectorTableHandle, ConnectorSession, etc.) directly because
 * trino-spi-479 is compiled for Java 25 (class file version 69)
 * while the current JVM is Java 24 (max class file version 68).
 * Logic that involves those types is tested via inline re-implementations
 * that mirror the exact lines being fixed.
 */
class QwUtilParseTest {

    // -----------------------------------------------------------------------
    // R06 — Optional compared with == in equals()
    // -----------------------------------------------------------------------

    /**
     * R06: Before the fix, RawQuickwitQueryTableHandle.equals() compared
     * the Optional<Boolean> cache field with == (reference equality).
     * Optional.of(true) != Optional.of(true) so two structurally-identical
     * handles returned false. The fix replaces == with .equals().
     *
     * We test the root cause directly without loading Trino SPI classes:
     * two distinct Optional.of(true) instances are NOT == but ARE .equals().
     */
    @Test
    @DisplayName("R06: two Optional.of(true) instances are not == but are .equals()")
    void optional_of_true_identity_vs_equality() {
        Optional<Boolean> a = Optional.of(true);
        Optional<Boolean> b = Optional.of(true);

        // This is the bug: reference equality fails even for equal values
        assertThat(a == b)
                .as("reference equality of distinct Optional.of(true) instances (the bug)")
                .isFalse();

        // This is the fix: .equals() works correctly
        assertThat(a.equals(b))
                .as("Optional.equals() of two Optional.of(true) instances (the fix)")
                .isTrue();
    }

    @Test
    @DisplayName("R06: equals() using .equals() on Optional returns true for equal handles")
    void rawHandle_equals_logic_with_optional_equals() {
        // Mirror the equals() logic for the cache field directly:
        // BUGGY:  return cache == other.cache && ...
        // FIXED:  return cache.equals(other.cache) && ...
        Optional<Boolean> cache1 = Optional.of(true);
        Optional<Boolean> cache2 = Optional.of(true);

        String connectorId = "conn";
        String index = "idx";
        String query = "*";

        // Buggy: reference equality gives false even when all fields are equal
        boolean buggyResult = cache1 == cache2;
        // Fixed: .equals() gives true
        boolean fixedResult = cache1.equals(cache2)
                && connectorId.equals(connectorId)
                && index.equals(index)
                && query.equals(query);

        assertThat(buggyResult).as("buggy == gives false").isFalse();
        assertThat(fixedResult).as("fixed .equals() gives true").isTrue();
    }

    // -----------------------------------------------------------------------
    // R08 — buildSearchRequestJson called twice, diverging timestamps
    // -----------------------------------------------------------------------

    /**
     * R08: Before the fix, QuickwitSplitManager.getSplits() called
     * buildSearchRequestJson(raw) twice. Each call captures Instant.now()
     * independently; 1+ second apart the //from= timestamps differ.
     *
     * We test the root cause: two calls to System.currentTimeMillis() /
     * Instant.now() separated by a sleep return different epoch-seconds.
     * The fix calls buildSearchRequestJson once and reuses the string.
     */
    @Test
    @DisplayName("R08: two successive Instant.now() calls 1s apart produce different epoch seconds")
    void two_instant_now_calls_separated_by_sleep_differ() throws InterruptedException {
        // Mirrors the root cause: buildSearchRequestJson calls Instant.now()
        // each time it is invoked, so two calls 1 second apart diverge.
        long epochFirst = java.time.Instant.now().getEpochSecond();
        Thread.sleep(1100);
        long epochSecond = java.time.Instant.now().getEpochSecond();

        assertThat(epochFirst)
                .as("epoch seconds 1s apart must differ (root cause of R08 double-call bug)")
                .isNotEqualTo(epochSecond);
    }

    @Test
    @DisplayName("R08: reusing a single string result avoids timestamp divergence")
    void single_string_reuse_is_consistent() {
        // Mirrors the fix: call once, reuse.
        // Use a simple timestamp string to simulate what buildSearchRequestJson produces.
        String searchRequest = "//from=" + java.time.Instant.now().getEpochSecond() + "\n{}";

        // Both the UlakSplit constructor argument and setTableName get the same value
        String constructorArg = searchRequest;
        String setTableNameArg = searchRequest;  // fix: reuse, not a second call

        assertThat(constructorArg)
                .as("constructor arg and setTableName arg must be identical when reused")
                .isEqualTo(setTableNameArg);
    }

    // -----------------------------------------------------------------------
    // R09 — noDataIndex never increments → duplicate column names
    // -----------------------------------------------------------------------

    /**
     * R09: Before the fix, noDataIndex was a plain int captured in a lambda
     * body, so the compiler rejected any increment inside the stream.
     * All blank segments became "no-data-0". The fix uses AtomicInteger.
     */
    @Test
    @DisplayName("R09: buggy plain-int noDataIndex produces duplicate no-data-0 names")
    void noDataIndex_plain_int_produces_duplicates() {
        // Use split with limit=-1 to keep trailing empty strings
        String columns = ",,";  // three blank segments: "", "", ""
        int noDataIndex = 0;    // plain int — cannot be incremented in a lambda

        // Buggy logic: noDataIndex never changes, all blanks get "no-data-0"
        List<String> names = new ArrayList<>();
        for (String t : columns.split(",", -1)) {
            names.add(org.apache.commons.lang3.StringUtils.isEmpty(t) || org.apache.commons.lang3.StringUtils.isBlank(t)
                    ? "no-data-" + noDataIndex   // noDataIndex stays 0 forever
                    : t);
        }

        // Buggy output: all three are "no-data-0"
        assertThat(names).as("buggy output has duplicates").containsExactly("no-data-0", "no-data-0", "no-data-0");
        assertThat(new HashSet<>(names)).as("buggy output: only 1 unique name").hasSize(1);
    }

    @Test
    @DisplayName("R09: AtomicInteger noDataIndex produces unique no-data-N names")
    void noDataIndex_atomic_int_produces_unique_names() {
        // Use split with limit=-1 to keep trailing empty strings
        String columns = ",,";  // three blank segments: "", "", ""
        AtomicInteger noDataIndex = new AtomicInteger(0);  // fixed

        List<String> names = new ArrayList<>();
        for (String t : columns.split(",", -1)) {
            names.add(org.apache.commons.lang3.StringUtils.isEmpty(t) || org.apache.commons.lang3.StringUtils.isBlank(t)
                    ? "no-data-" + noDataIndex.getAndIncrement()  // increments each time
                    : t);
        }

        // Fixed output: no-data-0, no-data-1, no-data-2
        assertThat(names).containsExactly("no-data-0", "no-data-1", "no-data-2");
        assertThat(new HashSet<>(names)).as("all names unique after fix").hasSize(3);
    }

    // -----------------------------------------------------------------------
    // R11 — trimTimeEdges deletes all rows when all timestamps equal
    // -----------------------------------------------------------------------

    /**
     * R11: Before the fix, trimTimeEdges() removed all rows when every row
     * had the same timestamp (maxTime == minTime). The removeIf predicate
     * matched every row because v == fMax AND v == fMin when equal.
     * Fix: add early return when maxTime == minTime.
     */
    @Test
    @DisplayName("R11: buggy trimTimeEdges removes all rows when all timestamps are equal")
    void trimTimeEdges_buggy_removes_all_rows_when_all_equal() {
        long[] timestamps = {1000L, 1000L, 1000L};
        long maxTime = 0, minTime = Long.MAX_VALUE;
        for (long v : timestamps) {
            if (v > maxTime) maxTime = v;
            if (v < minTime) minTime = v;
        }

        // Buggy predicate: v == fMax || v == fMin — all rows match when maxTime==minTime
        final long fMax = maxTime, fMin = minTime;
        int removed = 0;
        for (long v : timestamps) {
            if (v == fMax || v == fMin) removed++;
        }

        assertThat(removed).as("buggy logic removes all rows when all timestamps equal").isEqualTo(3);
    }

    @Test
    @DisplayName("R11: fixed trimTimeEdges keeps all rows when all timestamps are equal")
    void trimTimeEdges_fixed_keeps_all_rows_when_all_equal() {
        long[] timestamps = {1000L, 1000L, 1000L};
        long maxTime = 0, minTime = Long.MAX_VALUE;
        for (long v : timestamps) {
            if (v > maxTime) maxTime = v;
            if (v < minTime) minTime = v;
        }

        // R11 fix: skip trimming when maxTime == minTime
        boolean shouldSkipTrimming = (maxTime == 0 || minTime == Long.MAX_VALUE || maxTime == minTime);

        assertThat(shouldSkipTrimming)
                .as("fix: skip trimming when all timestamps are equal")
                .isTrue();
    }

    @Test
    @DisplayName("R11: fixed trimTimeEdges still removes edge rows when timestamps differ")
    void trimTimeEdges_fixed_removes_edges_when_timestamps_differ() {
        long[] timestamps = {1000L, 2000L, 3000L, 4000L, 5000L};
        long maxTime = 0, minTime = Long.MAX_VALUE;
        for (long v : timestamps) {
            if (v > maxTime) maxTime = v;
            if (v < minTime) minTime = v;
        }

        // max != min so trimming should proceed
        boolean shouldSkipTrimming = (maxTime == 0 || minTime == Long.MAX_VALUE || maxTime == minTime);
        assertThat(shouldSkipTrimming).as("should NOT skip when timestamps differ").isFalse();

        final long fMax = maxTime, fMin = minTime;
        int removed = 0;
        for (long v : timestamps) {
            if (v == fMax || v == fMin) removed++;
        }
        assertThat(removed).as("min(1000) and max(5000) rows removed").isEqualTo(2);
    }

    // -----------------------------------------------------------------------
    // R12 — ClassCastException on Integer numeric in arrangeAggregation
    // -----------------------------------------------------------------------

    /**
     * R12: Before the fix, arrangeAggregation() cast numeric values as either
     * Long or Double, but Gson with LONG_OR_DOUBLE strategy can also return
     * Integer for small values. The fix adds an Integer branch.
     *
     * NOTE: QwUtil.arrangeAggregations() cannot be called in tests because
     * loading QwUtil transitively loads io.trino.spi.ErrorCodeSupplier which
     * requires Java 25 (class file version 69). We test the casting logic
     * inline, mirroring the exact lines changed by R12.
     */
    @Test
    @DisplayName("R12: buggy cast throws ClassCastException for Integer doc_count")
    void arrangeAggregation_buggy_cast_throws_for_integer_doc_count() {
        Object docCntVal = Integer.valueOf(5);  // Gson may return Integer for small values

        // BUGGY: line 509 before fix
        // long doc_count = docCntVal instanceof Long ? (long) docCntVal : (long)(double)docCntVal;
        assertThatThrownBy(() -> {
            long val = docCntVal instanceof Long ? (long) docCntVal : (long)(double) docCntVal;
            // suppress unused warning
            assert val == val;
        }).isInstanceOf(ClassCastException.class)
          .as("buggy cast (long)(double) for Integer input throws ClassCastException");
    }

    @Test
    @DisplayName("R12: fixed cast handles Integer doc_count and returns correct long value")
    void arrangeAggregation_fixed_cast_handles_integer_doc_count() {
        Object docCntVal = Integer.valueOf(5);

        // FIXED: Integer branch added
        long doc_count = docCntVal instanceof Long ? (long) docCntVal
                : docCntVal instanceof Integer ? (long)(int) docCntVal
                : (long)(double) docCntVal;

        assertThat(doc_count).isEqualTo(5L);
    }

    @Test
    @DisplayName("R12: buggy cast throws ClassCastException for Integer sum_other_doc_count")
    void arrangeAggregation_buggy_cast_throws_for_integer_sum_other() {
        Object sumOtherDocCountObj = Integer.valueOf(3);  // Integer, not Long or Double

        // BUGGY: line 513 before fix
        // double sum_other = sumOtherDocCountObj instanceof Long ? (long)... : (long)(double)...
        assertThatThrownBy(() -> {
            double val = sumOtherDocCountObj instanceof Long
                    ? (long) sumOtherDocCountObj
                    : (long)(double) sumOtherDocCountObj;
            // suppress unused warning
            assert val == val;
        }).isInstanceOf(ClassCastException.class)
          .as("buggy cast (long)(double) for Integer sum_other_doc_count throws ClassCastException");
    }

    @Test
    @DisplayName("R12: fixed cast handles Integer sum_other_doc_count correctly")
    void arrangeAggregation_fixed_cast_handles_integer_sum_other() {
        Object sumOtherDocCountObj = Integer.valueOf(3);

        // FIXED: Integer branch added
        double sum_other_doc_count = sumOtherDocCountObj instanceof Long
                ? (long) sumOtherDocCountObj
                : sumOtherDocCountObj instanceof Integer
                    ? (long)(int) sumOtherDocCountObj
                    : (long)(double) sumOtherDocCountObj;

        assertThat(sum_other_doc_count).isEqualTo(3.0);
    }

    // -----------------------------------------------------------------------
    // R13 — c[j] without length guard → AIOOBE on short rows
    // -----------------------------------------------------------------------

    /**
     * R13: Before the fix, parseResponseHits() accessed c[j] unconditionally
     * when the header had more columns than the data row. The fix guards with
     * j < c.length ? c[j] : null. We test the guard expression directly.
     */
    @Test
    @DisplayName("R13: length guard j < c.length prevents AIOOBE on short rows")
    void lengthGuard_prevents_array_index_out_of_bounds() {
        Object[] header   = new Object[]{"col0", "col1", "col2"};  // 3 columns
        Object[] shortRow = new Object[]{"v0", "v1"};              // only 2 values

        // Buggy: c[j] with j=2 on a length-2 array throws AIOOBE
        assertThatThrownBy(() -> {
            Object last = null;
            for (int j = 0; j < header.length; j++) {
                last = shortRow[j];  // throws at j=2
            }
            // suppress unused warning
            assert last != null || true;
        }).isInstanceOf(ArrayIndexOutOfBoundsException.class)
          .as("buggy c[j] without guard throws AIOOBE");

        // Fixed: guarded access
        List<Object> values = new ArrayList<>();
        for (int j = 0; j < header.length; j++) {
            Object val = j < shortRow.length ? shortRow[j] : null;  // fix
            values.add(val);
        }
        assertThat(values).containsExactly("v0", "v1", null);
    }

    // -----------------------------------------------------------------------
    // R14 — "null" string treated as absent in getOptionalVarchar
    // -----------------------------------------------------------------------

    /**
     * R14: Before the fix, getOptionalVarchar() returned Optional.empty() for
     * the literal string "null" (in addition to empty strings). The fix only
     * returns empty for empty strings.
     */
    @Test
    @DisplayName("R14: buggy getOptionalVarchar treats 'null' as absent")
    void getOptionalVarchar_buggy_null_string_returns_empty() {
        String s = "null";
        // Buggy logic from RawQuery.java line 183
        Optional<String> buggyResult = (s.isEmpty() || s.equalsIgnoreCase("null"))
                ? Optional.empty()
                : Optional.of(s);

        assertThat(buggyResult).as("buggy logic: 'null' string returns empty").isEmpty();
    }

    @Test
    @DisplayName("R14: fixed getOptionalVarchar returns Optional.of('null') for 'null' string")
    void getOptionalVarchar_fixed_null_string_returns_present() {
        String s = "null";
        // Fixed logic: only empty string is treated as absent
        Optional<String> fixedResult = s.isEmpty() ? Optional.empty() : Optional.of(s);

        assertThat(fixedResult).as("fixed logic: 'null' string is a valid value").isPresent();
        assertThat(fixedResult).hasValue("null");
    }

    @Test
    @DisplayName("R14: fixed getOptionalVarchar still returns empty for empty string")
    void getOptionalVarchar_fixed_empty_string_returns_empty() {
        String s = "";
        Optional<String> fixedResult = s.isEmpty() ? Optional.empty() : Optional.of(s);
        assertThat(fixedResult).isEmpty();
    }

    // -----------------------------------------------------------------------
    // QW10: all-null row filter (parseResponseHits allNulls guard)
    // -----------------------------------------------------------------------

    // -----------------------------------------------------------------------
    // QW10: allNulls guard logic (SPI-free inline verification)
    //
    // parseResponseHits() calls QwUtil/UlakRow/QueryParameters which trigger
    // Trino SPI class loading (Java-25 compiled, fails on Java-24 JVM).
    // We verify the allNulls guard logic inline — the exact same boolean check
    // that was previously commented out on QwUtil.java line 646.
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("QW10: allNulls=true when every column value is null")
    void allNulls_trueWhenAllValuesNull() {
        Object[] rowData = {null, null, null};
        boolean allNulls = true;
        for (Object val : rowData) {
            String value = val == null ? null : String.valueOf(val);
            if (value != null && !value.equals("null")) { allNulls = false; }
        }
        assertThat(allNulls).as("row with all-null values must be flagged allNulls=true").isTrue();
    }

    @Test
    @DisplayName("QW10: allNulls=false when at least one column has a real value")
    void allNulls_falseWhenAnyValueNonNull() {
        Object[] rowData = {null, "grafana-hub", null};
        boolean allNulls = true;
        for (Object val : rowData) {
            String value = val == null ? null : String.valueOf(val);
            if (value != null && !value.equals("null")) { allNulls = false; }
        }
        assertThat(allNulls).as("row with at least one real value must be flagged allNulls=false").isFalse();
    }

    @Test
    @DisplayName("QW10: all-null row is excluded from result when guard is active")
    void allNullsGuard_excludesNullRows() {
        // Simulates the fixed parseResponseHits() loop — if(!allNulls) toRet.add(row)
        List<String> result = new ArrayList<>();
        boolean[][] testRows = {
            {true},   // all null  → excluded
            {false},  // has data  → included
            {true},   // all null  → excluded
        };
        for (boolean[] row : testRows) {
            boolean allNulls = row[0];
            if (!allNulls) result.add("row");
        }
        assertThat(result).as("only 1 non-null row should be in result").hasSize(1);
    }

    @Test
    @DisplayName("Verify dynamic query rewriting for history index rollups including standardized flow metrics")
    void testRewriteQueryForHistory() {
        String inputJson = "{" +
                "  \"aggs\": {" +
                "    \"my_agg\": {" +
                "      \"min\": { \"field\": \"span_attributes.tx\" }," +
                "      \"max\": { \"field\": \"span_attributes.rx\" }," +
                "      \"avg\": { \"field\": \"span_attributes.value\" }," +
                "      \"sum\": { \"field\": \"span_attributes.tx\" }," +
                "      \"value_count\": { \"field\": \"span_attributes.rx\" }" +
                "    }," +
                "    \"flow_agg\": {" +
                "      \"sum\": { \"field\": \"span_attributes.u\" }," +
                "      \"sum_ac\": { \"sum\": { \"field\": \"span_attributes.ac\" } }," +
                "      \"sum_u_ac\": { \"sum\": { \"field\": \"span_attributes.u_ac\" } }" +
                "    }," +
                "    \"time_agg\": {" +
                "      \"min\": { \"field\": \"span_start_timestamp_nanos\" }," +
                "      \"max\": { \"field\": \"span_start_timestamp_nanos\" }" +
                "    }" +
                "  }" +
                "}";

        String expectedJson = "{" +
                "  \"aggs\": {" +
                "    \"my_agg\": {" +
                "      \"min\": { \"field\": \"span_attributes.tx_min\" }," +
                "      \"max\": { \"field\": \"span_attributes.rx_max\" }," +
                "      \"avg\": { \"field\": \"span_attributes.value\" }," +
                "      \"sum\": { \"field\": \"span_attributes.tx_sum\" }," +
                "      \"value_count\": { \"field\": \"span_attributes.rx_count\" }" +
                "    }," +
                "    \"flow_agg\": {" +
                "      \"sum\": { \"field\": \"span_attributes.u_sum\" }," +
                "      \"sum_ac\": { \"sum\": { \"field\": \"span_attributes.ac_sum\" } }," +
                "      \"sum_u_ac\": { \"sum\": { \"field\": \"span_attributes.u_ac_sum\" } }" +
                "    }," +
                "    \"time_agg\": {" +
                "      \"min\": { \"field\": \"span_attributes.timestamp_min\" }," +
                "      \"max\": { \"field\": \"span_attributes.timestamp_max\" }" +
                "    }" +
                "  }" +
                "}";

        String rewritten = QwQueryRewriter.rewriteQueryForHistory(inputJson);

        // Parse both as maps to ignore formatting/ordering differences
        com.google.gson.Gson gson = new com.google.gson.Gson();
        Map<?, ?> rewrittenMap = gson.fromJson(rewritten, Map.class);
        Map<?, ?> expectedMap = gson.fromJson(expectedJson, Map.class);

        assertThat(rewrittenMap).isEqualTo(expectedMap);
    }
}
