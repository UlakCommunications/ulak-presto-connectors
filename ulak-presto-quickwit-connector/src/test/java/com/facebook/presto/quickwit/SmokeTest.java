package com.facebook.presto.quickwit;

import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Sanity check that surefire + JUnit 5 + AssertJ are wired up, and that
 * the fixture files extracted from `backend/anomaly` Grafana dashboards
 * are reachable on the test classpath. Real test classes pull from these
 * fixtures (see TODO L03).
 */
class SmokeTest
{
    @Test
    void junit_jupiter_assertj_are_wired()
    {
        assertThat(1 + 1).isEqualTo(2);
    }

    @Test
    void anomaly_aggs_dsl_fixtures_are_on_classpath()
            throws Exception
    {
        try (InputStream in = getClass().getResourceAsStream("/fixtures/anomaly/aggs-dsl.json")) {
            assertThat(in).as("aggs-dsl.json fixture").isNotNull();
            byte[] bytes = readAll(in);
            assertThat(bytes.length).as("non-empty").isGreaterThan(100);
            String s = new String(bytes);
            // Sanity: 80+ aggs DSL strings, every one starts with '['
            assertThat(s).contains("histogram(");
            assertThat(s).contains("terms(");
        }
    }

    @Test
    void anomaly_raw_query_params_fixtures_are_on_classpath()
            throws Exception
    {
        try (InputStream in = getClass().getResourceAsStream("/fixtures/anomaly/raw-query-params.json")) {
            assertThat(in).as("raw-query-params.json fixture").isNotNull();
            String s = new String(readAll(in));
            assertThat(s).contains("\"qwindex\"");
            assertThat(s).contains("\"sqlversion\"");
        }
    }

    private static byte[] readAll(InputStream in)
            throws Exception
    {
        java.io.ByteArrayOutputStream buf = new java.io.ByteArrayOutputStream();
        byte[] chunk = new byte[8192];
        int n;
        while ((n = in.read(chunk)) > 0) {
            buf.write(chunk, 0, n);
        }
        return buf.toByteArray();
    }
}
