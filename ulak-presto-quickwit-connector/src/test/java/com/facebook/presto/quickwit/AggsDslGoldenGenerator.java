package com.facebook.presto.quickwit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads aggs-dsl.json fixture, applies Grafana var substitution (${...} → "10"),
 * calls AggsDslCompiler.normalizeAggs(), and prints one JSON per line to stdout.
 *
 * Used to generate aggs-dsl.golden.jsonl for Rust parity test.
 * Run via: java -cp <classpath> com.facebook.presto.quickwit.AggsDslGoldenGenerator
 */
public class AggsDslGoldenGenerator {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        try (InputStream in = AggsDslGoldenGenerator.class.getResourceAsStream("/fixtures/anomaly/aggs-dsl.json")) {
            if (in == null) {
                throw new IllegalStateException("fixtures/anomaly/aggs-dsl.json missing on classpath");
            }
            JsonNode arr = MAPPER.readTree(in);
            List<String> dsls = new ArrayList<>();
            arr.forEach(n -> dsls.add(n.asText()));

            for (String dsl : dsls) {
                String rendered = dsl.replaceAll("\\$\\{[^}]+\\}", "10");
                String out = AggsDslCompiler.normalizeAggs(rendered);
                System.out.println(out);
            }
        }
    }
}
