package com.facebook.presto.quickwit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Behaviour test for {@link AggsDslCompiler}.
 *
 * <p>Two layers of coverage:
 * <ol>
 *   <li>Hand-written cases for nullability / empty / JSON-passthrough /
 *       invalid-input branches — the bits not visible in fixtures.</li>
 *   <li>Fixture-driven round-trip over the 82 distinct Aggs DSL strings
 *       harvested from the {@code backend/anomaly} Grafana dashboards
 *       (see {@code fixtures/anomaly/aggs-dsl.json}). Every fixture must
 *       compile to syntactically-valid JSON without throwing — that is
 *       a regression test for any future refactor.</li>
 * </ol>
 */
class AggsDslCompilerTest
{
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    @DisplayName("null input is passed through")
    void normalizeAggs_null_returns_null()
    {
        assertThat(AggsDslCompiler.normalizeAggs(null)).isNull();
    }

    @Test
    @DisplayName("empty / whitespace input returns the trimmed empty string")
    void normalizeAggs_empty_input()
    {
        assertThat(AggsDslCompiler.normalizeAggs("")).isEmpty();
        assertThat(AggsDslCompiler.normalizeAggs("   ")).isEmpty();
    }

    @Test
    @DisplayName("JSON object passes through unchanged")
    void normalizeAggs_json_object_passthrough()
    {
        String json = "{\"date_histogram\":{\"field\":\"ts\",\"fixed_interval\":\"10s\"}}";
        assertThat(AggsDslCompiler.normalizeAggs(json)).isEqualTo(json);
    }

    @Test
    @DisplayName("JSON array-of-object passes through unchanged")
    void normalizeAggs_json_array_passthrough()
    {
        String json = "[{\"k\":\"v\"}]";
        assertThat(AggsDslCompiler.normalizeAggs(json)).isEqualTo(json);
    }

    @Test
    @DisplayName("non-JSON, non-DSL input is rejected")
    void normalizeAggs_garbage_throws()
    {
        assertThatThrownBy(() -> AggsDslCompiler.normalizeAggs("not-a-list"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("JSON or DSL");
    }

    @Test
    @DisplayName("simplest histogram-only DSL compiles to valid JSON")
    void normalizeAggs_simple_histogram()
            throws Exception
    {
        String dsl = "[histogram(field=ts, interval=10s, id=1)]";
        String out = AggsDslCompiler.normalizeAggs(dsl);
        // must parse as JSON
        JsonNode node = MAPPER.readTree(out);
        assertThat(node).isNotNull();
    }

    @Test
    @DisplayName("two histograms in one DSL is rejected")
    void normalizeAggs_two_histograms_throws()
    {
        String dsl = "[histogram(field=a, interval=10s, id=1), histogram(field=b, interval=20s, id=2)]";
        assertThatThrownBy(() -> AggsDslCompiler.normalizeAggs(dsl))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("histogram");
    }

    /**
     * Round-trip every distinct DSL string we found in production
     * dashboards. Each string must compile to syntactically-valid JSON.
     * Any failure here is a regression in the compiler — investigate
     * before relaxing the expectation.
     */
    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("anomalyAggsDslFixtures")
    void anomaly_dashboard_aggs_dsl_compiles_to_valid_json(String dsl)
            throws Exception
    {
        String out = AggsDslCompiler.normalizeAggs(dsl);
        assertThat(out).as("compiled output for: %s", abbreviate(dsl)).isNotNull();
        JsonNode node = MAPPER.readTree(out);
        assertThat(node).as("parses as JSON").isNotNull();
    }

    static Stream<String> anomalyAggsDslFixtures()
            throws Exception
    {
        try (InputStream in = AggsDslCompilerTest.class.getResourceAsStream("/fixtures/anomaly/aggs-dsl.json")) {
            if (in == null) {
                throw new IllegalStateException("fixtures/anomaly/aggs-dsl.json missing on classpath");
            }
            JsonNode arr = MAPPER.readTree(in);
            List<String> dsls = new ArrayList<String>();
            arr.forEach(n -> dsls.add(n.asText()));
            return dsls.stream();
        }
    }

    private static String abbreviate(String s)
    {
        if (s == null) return "<null>";
        return s.length() <= 80 ? s : s.substring(0, 77) + "...";
    }
}
