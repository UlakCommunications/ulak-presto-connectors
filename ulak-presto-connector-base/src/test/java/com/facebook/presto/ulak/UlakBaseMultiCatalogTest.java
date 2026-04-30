package com.facebook.presto.ulak;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Locks in the L04 multi-catalog fix for the base classes shared by
 * the InfluxDB and Postgres connectors. Before L04 a single static
 * {@code single} field meant InfluxDB and Postgres both received the
 * same {@code UlakSplitManager} / {@code UlakRecordSetProvider}.
 *
 * <p><b>Disabled on JDK &lt; 25.</b> See
 * {@code UlakQuickwitMetadataMultiCatalogTest} for context — Trino SPI
 * 479 is class-file v69 (Java 25) and the local build is JDK 24, so
 * any instantiation fails with {@code UnsupportedClassVersionError}.
 * Source-level invariant is locked in by {@link L04StructuralTest}.
 */
@Disabled("requires JDK 25 to load Trino SPI 479 (class file v69)")
class UlakBaseMultiCatalogTest
{
    @Test
    void split_managers_are_independent_instances()
    {
        UlakSplitManager a = new UlakSplitManager();
        UlakSplitManager b = new UlakSplitManager();
        assertThat(a).isNotSameAs(b);
    }

    @Test
    void record_set_providers_keep_their_own_default_params()
    {
        UlakRecordSetProvider a = new UlakRecordSetProvider(
                (q, s) -> null, new String[]{"a"});
        UlakRecordSetProvider b = new UlakRecordSetProvider(
                (q, s) -> null, new String[]{"b"});

        assertThat(a).isNotSameAs(b);
        assertThat(a.getDefaultParams()).containsExactly("a");
        assertThat(b.getDefaultParams()).containsExactly("b");
    }
}
