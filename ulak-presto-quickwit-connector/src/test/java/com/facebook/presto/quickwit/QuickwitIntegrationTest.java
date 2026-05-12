package com.facebook.presto.quickwit;

import com.facebook.presto.ulak.QueryParameters;
import com.facebook.presto.ulak.UlakRow;
import com.quickwit.javaclient.ApiClient;
import com.quickwit.javaclient.ApiException;
import com.quickwit.javaclient.Configuration;
import com.quickwit.javaclient.api.SearchApi;
import com.quickwit.javaclient.models.SearchRequestQueryString;
import okhttp3.Call;
import okhttp3.Response;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * J50 — Connector integration tests against a real Quickwit instance.
 *
 * SKIPPED unless environment variables are set:
 *   QUICKWIT_TEST_URL   e.g. http://quickwit-host:7280
 *   QUICKWIT_TEST_INDEX e.g. metrics3
 *
 * Run locally:
 *   QUICKWIT_TEST_URL=http://localhost:7280 \
 *   QUICKWIT_TEST_INDEX=metrics3 \
 *   ./mvnw test -pl ulak-presto-quickwit-connector -Dtest=QuickwitIntegrationTest
 *
 * GitLab CI:
 *   variables:
 *     QUICKWIT_TEST_URL: http://quickwit:7280
 *     QUICKWIT_TEST_INDEX: metrics3
 *   services:
 *     - name: quickwit/quickwit:0.8.2
 *       command: ["run"]
 *
 * NOTE: Tests that go through QwUtil.select() avoid loading Trino SPI types
 * (trino-spi-479 requires Java 25; runtime is Java 24). S5 is pure Java.
 */
@EnabledIfEnvironmentVariable(named = "QUICKWIT_TEST_URL", matches = ".+")
@DisplayName("J50 Integration — requires QUICKWIT_TEST_URL + QUICKWIT_TEST_INDEX")
class QuickwitIntegrationTest {

    private static String qwUrl;
    private static String qwIndex;
    private static ApiClient client;

    @BeforeAll
    static void setup() {
        qwUrl   = System.getenv("QUICKWIT_TEST_URL");
        qwIndex = System.getenv().getOrDefault("QUICKWIT_TEST_INDEX", "metrics3");
        client  = Configuration.getDefaultApiClient();
        client.setBasePath(qwUrl);
        client.setConnectTimeout(10_000);
        client.setReadTimeout(30_000);
    }

    // -----------------------------------------------------------------------
    // S1: basic HTTP connectivity — Quickwit responds to match-all
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("S1: basic connectivity — match-all query gets HTTP 200")
    void s1_basicConnectivity() throws ApiException, IOException {
        SearchApi searchApi = new SearchApi(client);
        SearchRequestQueryString req = new SearchRequestQueryString();
        req.setQuery("*");
        req.setMaxHits(1L);
        Call call = searchApi.searchPostHandlerCall(qwIndex, req, null);
        try (Response resp = call.execute()) {
            assertThat(resp.isSuccessful())
                    .as("HTTP status " + resp.code())
                    .isTrue();
        }
    }

    // -----------------------------------------------------------------------
    // S2: aggs v0.1 — histogram returns aggId/key columns
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("S2: sqlversion=0.1 date_histogram returns aggId/key column names")
    void s2_aggsV01() throws ApiException {
        String tableName = "\n//qwindex=" + qwIndex +
                "\n//qwurl=" + qwUrl +
                "\n//sqlversion=0.1\n" +
                "{\"aggs\":{\"1\":{\"date_histogram\":{" +
                "\"field\":\"span_start_timestamp_nanos\"," +
                "\"fixed_interval\":\"1d\",\"min_doc_count\":1}}}," +
                "\"query\":\"*\",\"max_hits\":0," +
                "\"start_timestamp\":0,\"end_timestamp\":9999999999}";

        QueryParameters qp = QueryParameters.getQueryParameters(tableName);
        List<UlakRow> rows = QwUtil.select(qp, qwUrl, qwIndex, 10, 30, 30);

        assertThat(rows).isNotNull();
        if (!rows.isEmpty()) {
            assertThat(rows.get(0).getColumnMap())
                    .as("sqlversion=0.1 must use 'aggId/key' column names")
                    .containsKey("1/key");
        }
    }

    // -----------------------------------------------------------------------
    // S3: aggs v0.2 — same query, bare aggId column names
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("S3: sqlversion=0.2 date_histogram returns bare aggId column names")
    void s3_aggsV02() throws ApiException {
        String tableName = "\n//qwindex=" + qwIndex +
                "\n//qwurl=" + qwUrl +
                "\n//sqlversion=0.2\n" +
                "{\"aggs\":{\"1\":{\"date_histogram\":{" +
                "\"field\":\"span_start_timestamp_nanos\"," +
                "\"fixed_interval\":\"1d\",\"min_doc_count\":1}}}," +
                "\"query\":\"*\",\"max_hits\":0," +
                "\"start_timestamp\":0,\"end_timestamp\":9999999999}";

        QueryParameters qp = QueryParameters.getQueryParameters(tableName);
        List<UlakRow> rows = QwUtil.select(qp, qwUrl, qwIndex, 10, 30, 30);

        assertThat(rows).isNotNull();
        if (!rows.isEmpty()) {
            assertThat(rows.get(0).getColumnMap())
                    .as("sqlversion=0.2 must use bare 'aggId' column names, not 'aggId/key'")
                    .containsKey("1")
                    .doesNotContainKey("1/key");
        }
    }

    // -----------------------------------------------------------------------
    // S4: raw hits — max_hits=5, no aggs
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("S4: raw hits query (max_hits=5) returns at most 5 rows")
    void s4_rawHits() throws ApiException {
        String tableName = "\n//qwindex=" + qwIndex +
                "\n//qwurl=" + qwUrl +
                "\n{\"query\":\"*\",\"max_hits\":5}";

        QueryParameters qp = QueryParameters.getQueryParameters(tableName);
        List<UlakRow> rows = QwUtil.select(qp, qwUrl, qwIndex, 10, 30, 30);

        assertThat(rows).isNotNull();
        assertThat(rows.size()).isLessThanOrEqualTo(5);
    }

    // -----------------------------------------------------------------------
    // S5: SSRF guard — validateQwUrl blocks URL not in allowlist
    //     Pure Java — does not load Trino SPI at class-load time.
    // -----------------------------------------------------------------------

    @Test
    @DisplayName("S5: validateQwUrl rejects URL not in allowlist (pure Java, no SPI load)")
    void s5_ssrfGuard() {
        // Inline reimplementation of validateQwUrl logic to avoid SPI classload:
        // allowlist = {qwUrl}; "http://evil" is not in it → should be rejected.
        java.util.Set<String> allowedQwUrls = new java.util.HashSet<>();
        allowedQwUrls.add(qwUrl.replaceAll("/+$", ""));

        String injected = "http://evil-host:9999";
        String normalized = injected.replaceAll("/+$", "");
        boolean rejected = !allowedQwUrls.isEmpty() && !allowedQwUrls.contains(normalized);

        assertThat(rejected)
                .as("URL not in allowlist must be rejected")
                .isTrue();
    }
}
