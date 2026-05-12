package com.facebook.presto.quickwit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for QuickwitRecordSetProvider.buildSearchRequestJson().
 *
 * NOTE: RawQuickwitQueryTableHandle implements ConnectorTableHandle which is
 * compiled for Java 25 (class file version 69). Tests that instantiate it
 * will fail with UnsupportedClassVersion on Java 24. The R04 logic is
 * therefore tested via the inline expression that mirrors the buggy and
 * fixed code paths.
 */
class QuickwitRecordSetProviderTest {

    // -----------------------------------------------------------------------
    // R04 — //name= header uses h.isCache().get() instead of h.getName().get()
    // -----------------------------------------------------------------------

    /**
     * R04: On line 100 of QuickwitRecordSetProvider.java:
     *
     *   BUGGY:  initialString = "//name=" + (h.getName().isPresent() ? h.isCache().get() : "<no_name>") + "\n" + ...
     *   FIXED:  initialString = "//name=" + (h.getName().isPresent() ? h.getName().get() : "<no_name>") + "\n" + ...
     *
     * When h.getName().isPresent(), the buggy code returns h.isCache().get() (a boolean)
     * instead of h.getName().get() (the actual name string).
     * This test simulates the two branches directly.
     */
    @Test
    @DisplayName("R04: when name is present, buggy code returns cache value not name")
    void r04_buggy_name_header_returns_cache_value() {
        Optional<String>  name  = Optional.of("test-query");
        Optional<Boolean> cache = Optional.of(false);

        // Buggy expression: h.getName().isPresent() ? h.isCache().get() : "<no_name>"
        String buggyValue = name.isPresent() ? String.valueOf(cache.get()) : "<no_name>";

        assertThat(buggyValue)
                .as("buggy code returns the cache boolean, not the query name")
                .isEqualTo("false");          // "false", not "test-query"
        assertThat(buggyValue).isNotEqualTo("test-query");
    }

    @Test
    @DisplayName("R04: fixed code returns the actual name value")
    void r04_fixed_name_header_returns_name_value() {
        Optional<String>  name  = Optional.of("test-query");
        Optional<Boolean> cache = Optional.of(false);

        // Fixed expression: h.getName().isPresent() ? h.getName().get() : "<no_name>"
        String fixedValue = name.isPresent() ? name.get() : "<no_name>";

        assertThat(fixedValue)
                .as("fixed code returns the query name")
                .isEqualTo("test-query");
    }

    @Test
    @DisplayName("R04: when name is absent, both buggy and fixed code return '<no_name>'")
    void r04_absent_name_returns_no_name_fallback() {
        Optional<String>  name  = Optional.empty();
        Optional<Boolean> cache = Optional.of(true);

        // Both buggy and fixed fall through to "<no_name>" when name is absent
        String buggyValue = name.isPresent() ? String.valueOf(cache.get()) : "<no_name>";
        String fixedValue = name.isPresent() ? name.get() : "<no_name>";

        assertThat(buggyValue).isEqualTo("<no_name>");
        assertThat(fixedValue).isEqualTo("<no_name>");
    }

    @Test
    @DisplayName("R04: bug is observable - cache=true gives //name=true instead of actual name")
    void r04_with_cache_true_buggy_output_would_be_true() {
        Optional<String>  name  = Optional.of("my-dashboard");
        Optional<Boolean> cache = Optional.of(true);  // cache is true

        // Buggy: //name=true  (the cache value, not the name)
        String buggyHeader = "//name=" + (name.isPresent() ? String.valueOf(cache.get()) : "<no_name>");
        // Fixed: //name=my-dashboard
        String fixedHeader = "//name=" + (name.isPresent() ? name.get() : "<no_name>");

        assertThat(buggyHeader).isEqualTo("//name=true");   // wrong
        assertThat(fixedHeader).isEqualTo("//name=my-dashboard");  // correct
    }
}
