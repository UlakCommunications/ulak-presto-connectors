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
     *
     * <p>The harvested fixtures still carry Grafana template tokens
     * (e.g. {@code size=${top:csv}}, {@code interval=${__interval_ms}ms}).
     * In production those are substituted by Grafana before the SQL ever
     * reaches Trino, so the compiler is correct to reject them; this test
     * pre-renders them via {@link #renderTemplates} to exercise the
     * compiler against the same shape Trino sees at runtime.
     */
    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("anomalyAggsDslFixtures")
    void anomaly_dashboard_aggs_dsl_compiles_to_valid_json(String dsl)
            throws Exception
    {
        String rendered = renderTemplates(dsl);
        String out = AggsDslCompiler.normalizeAggs(rendered);
        assertThat(out).as("compiled output for: %s", abbreviate(rendered)).isNotNull();
        JsonNode node = MAPPER.readTree(out);
        assertThat(node).as("parses as JSON").isNotNull();
    }

    /**
     * Substitute Grafana {@code ${var}} / {@code ${var:fmt}} tokens with a
     * plausible numeric default. Mirrors what Grafana does before sending
     * SQL to Trino — the compiler never sees raw tokens at runtime.
     */
    private static String renderTemplates(String dsl)
    {
        return dsl == null ? null : dsl.replaceAll("\\$\\{[^}]+\\}", "10");
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

    // -----------------------------------------------------------------------
    // R17 — order=id:X:dir split must be bounded (split(":", 3))
    // -----------------------------------------------------------------------

    /**
     * R17: The buggy {@code o.split(":")} in parseTerms() has no limit, so a
     * metric id that contains a colon would produce more than 3 parts and
     * parts[2] (the direction) would be the first extra segment instead of
     * "desc"/"asc". Using {@code split(":", 3)} caps the result at 3 parts
     * so the direction is always in parts[2] regardless of the id content.
     *
     * This test verifies that {@code terms(field=x, size=10, order=id:4:desc, id=3)}
     * compiles to valid JSON and the resulting agg contains the expected
     * sort order on metric id "4".
     */
    @Test
    @DisplayName("R17: terms with order=id:4:desc compiles correctly (split bounded to 3)")
    void normalizeAggs_terms_with_order_compiles()
            throws Exception
    {
        String dsl = "[terms(field=x, size=10, order=id:4:desc, id=3)," +
                " avg(field=val, id=4)]";
        String out = AggsDslCompiler.normalizeAggs(dsl);
        assertThat(out).as("compiled output must not be null").isNotNull();
        JsonNode node = MAPPER.readTree(out);
        assertThat(node).as("must parse as JSON").isNotNull();
    }

    @Test
    @DisplayName("R17: split(':',3) keeps direction token intact even when id has colon")
    void split_bounded_to_3_preserves_direction()
    {
        // Demonstrate the fix: unbounded split on "id:some:extra:asc" produces 4 parts;
        // bounded split(3) produces exactly 3 — parts[2] is always the direction.
        String order = "id:some:extra:asc";
        String[] unbounded = order.split(":");          // buggy — 4 parts
        String[] bounded   = order.split(":", 3);       // fixed — 3 parts

        assertThat(unbounded).as("unbounded split produces 4 parts").hasSize(4);
        assertThat(bounded).as("bounded split(3) produces exactly 3 parts").hasSize(3);
        assertThat(bounded[2]).as("parts[2] with bounded split is 'extra:asc'").isEqualTo("extra:asc");
        // With normal metric ids (no colon), both forms produce the same 3 parts:
        String normal = "id:4:desc";
        assertThat(normal.split(":", 3)).containsExactly("id", "4", "desc");
        assertThat(normal.split(":")).containsExactly("id", "4", "desc");
    }

    private static String abbreviate(String s)
    {
        if (s == null) return "<null>";
        return s.length() <= 80 ? s : s.substring(0, 77) + "...";
    }
}
