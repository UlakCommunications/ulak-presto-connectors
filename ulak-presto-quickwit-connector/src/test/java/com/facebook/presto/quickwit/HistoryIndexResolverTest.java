package com.facebook.presto.quickwit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Behaviour test for {@link HistoryIndexResolver}. Target minutes here come from
 * {@link HistoryTier} (a range-based decision), not from the query's own bucket width —
 * this resolver's only job is swapping the granularity token in an index name.
 */
class HistoryIndexResolverTest
{
    @Test
    @DisplayName("identity: target tier already matches the index's own tier")
    void resolve_sameTier_returnsUnchanged()
    {
        assertThat(HistoryIndexResolver.resolve("metrics3_15", 15)).isEqualTo("metrics3_15");
    }

    @Test
    @DisplayName("switches metrics3_15 to metrics3_60 for the 60m tier")
    void resolve_metricsIndex_switchesTier()
    {
        assertThat(HistoryIndexResolver.resolve("metrics3_15", 60)).isEqualTo("metrics3_60");
    }

    @Test
    @DisplayName("switches the flow-rollup naming style (rollup_15m_site_app) too")
    void resolve_flowRollupIndex_switchesTier()
    {
        assertThat(HistoryIndexResolver.resolve("rollup_15m_site_app", 60)).isEqualTo("rollup_60m_site_app");
    }

    @Test
    @DisplayName("base index digits (metrics3 / flows3 version number) are never mistaken for the tier")
    void resolve_baseIndexVersionDigit_notTreatedAsGranularity()
    {
        // only "15" is a 2+ digit token here; the "3" in metrics3 must be ignored
        assertThat(HistoryIndexResolver.resolve("metrics3_15", 45)).isEqualTo("metrics3_45");
    }

    @Test
    @DisplayName("a yearly-scale tier is just a larger minute count, no upper bound")
    void resolve_yearlyScaleTier_switchesTier()
    {
        assertThat(HistoryIndexResolver.resolve("metrics3_15", 525600)).isEqualTo("metrics3_525600");
    }

    @Test
    @DisplayName("blank history index -> left unchanged (nothing to derive from)")
    void resolve_blankHistoryIndex_returnsUnchanged()
    {
        assertThat(HistoryIndexResolver.resolve("", 60)).isEqualTo("");
        assertThat(HistoryIndexResolver.resolve(null, 60)).isNull();
    }

    @Test
    @DisplayName("non-positive target minutes -> left unchanged")
    void resolve_nonPositiveTargetMinutes_returnsUnchanged()
    {
        assertThat(HistoryIndexResolver.resolve("metrics3_15", 0)).isEqualTo("metrics3_15");
        assertThat(HistoryIndexResolver.resolve("metrics3_15", -5)).isEqualTo("metrics3_15");
    }

    @Test
    @DisplayName("index name with no 2+ digit token at all -> left unchanged")
    void resolve_noGranularityToken_returnsUnchanged()
    {
        assertThat(HistoryIndexResolver.resolve("historyindex", 60)).isEqualTo("historyindex");
    }

    @Test
    @DisplayName("index name with two candidate granularity tokens is ambiguous -> left unchanged")
    void resolve_ambiguousGranularityTokens_returnsUnchanged()
    {
        assertThat(HistoryIndexResolver.resolve("metrics15_30_legacy", 60)).isEqualTo("metrics15_30_legacy");
    }
}
