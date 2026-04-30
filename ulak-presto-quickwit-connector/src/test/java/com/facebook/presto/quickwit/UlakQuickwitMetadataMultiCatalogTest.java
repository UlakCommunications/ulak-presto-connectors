package com.facebook.presto.quickwit;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Locks in the L04 multi-catalog fix. Before L04 the static {@code single}
 * field made the second catalog observe the first catalog's URL/index/id;
 * these tests would have returned the same instance for both calls.
 *
 * <p><b>Disabled on JDK &lt; 25.</b> Trino SPI 479 ships class-file
 * version 69.0 (Java 25). The classes under test
 * ({@link UlakQuickwitMetadata}, {@link QuickwitSplitManager},
 * {@link QuickwitRecordSetProvider}) directly implement Trino SPI
 * interfaces, so any instantiation triggers SPI class loading and
 * {@code UnsupportedClassVersionError} on the local JDK 24 build. Re-enable
 * when CI runs on JDK 25 — the assertions themselves are unconditional.
 * Until then, the L04 structural change is verified by {@code mvn compile}
 * succeeding (every {@code .getInstance()} call site has migrated to
 * {@code new ...()}).
 */
@Disabled("requires JDK 25 to load Trino SPI 479 (class file v69)")
class UlakQuickwitMetadataMultiCatalogTest
{
    @Test
    void two_metadata_instances_keep_distinct_url_index_and_connector_id()
    {
        UlakQuickwitMetadata a = new UlakQuickwitMetadata(
                "quickwit_a", "http://qw-a:7280", "index_a", 1000, 2000, 3000);
        UlakQuickwitMetadata b = new UlakQuickwitMetadata(
                "quickwit_b", "http://qw-b:7280", "index_b", 4000, 5000, 6000);

        assertThat(a).isNotSameAs(b);

        assertThat(a.getQwUrl()).isEqualTo("http://qw-a:7280");
        assertThat(a.getQwIndex()).isEqualTo("index_a");
        assertThat(a.getConnectTimeout()).isEqualTo(1000);
        assertThat(a.getReadTimeout()).isEqualTo(2000);
        assertThat(a.getWriteTimeout()).isEqualTo(3000);
        assertThat(a.getConnectorId()).contains("quickwit_a");

        assertThat(b.getQwUrl()).isEqualTo("http://qw-b:7280");
        assertThat(b.getQwIndex()).isEqualTo("index_b");
        assertThat(b.getConnectTimeout()).isEqualTo(4000);
        assertThat(b.getReadTimeout()).isEqualTo(5000);
        assertThat(b.getWriteTimeout()).isEqualTo(6000);
        assertThat(b.getConnectorId()).contains("quickwit_b");

        assertThat(a.getConnectorId()).isNotEqualTo(b.getConnectorId());
    }

    @Test
    void split_managers_are_independent_instances()
    {
        QuickwitSplitManager a = new QuickwitSplitManager();
        QuickwitSplitManager b = new QuickwitSplitManager();
        assertThat(a).isNotSameAs(b);
    }

    @Test
    void record_set_providers_keep_their_own_default_params()
    {
        QuickwitRecordSetProvider a = new QuickwitRecordSetProvider(
                (q, s) -> null, new String[]{"http://qw-a:7280", "index_a"});
        QuickwitRecordSetProvider b = new QuickwitRecordSetProvider(
                (q, s) -> null, new String[]{"http://qw-b:7280", "index_b"});

        assertThat(a).isNotSameAs(b);
        assertThat(a.getDefaultParams()).containsExactly("http://qw-a:7280", "index_a");
        assertThat(b.getDefaultParams()).containsExactly("http://qw-b:7280", "index_b");
    }
}
