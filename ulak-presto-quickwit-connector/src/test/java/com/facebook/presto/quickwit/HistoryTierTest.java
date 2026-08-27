package com.facebook.presto.quickwit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class HistoryTierTest
{
    @Test
    @DisplayName("build: finest tier only, matches today's single-threshold behaviour")
    void build_finestTierOnly()
    {
        List<HistoryTier> tiers = HistoryTier.build(10800L, null);
        assertThat(tiers).extracting(t -> t.minutes).containsExactly(15);
        assertThat(tiers.get(0).thresholdSeconds).isEqualTo(10800L);
    }

    @Test
    @DisplayName("build: additional CSV tiers get merged and sorted by minutes")
    void build_additionalTiersMergedAndSorted()
    {
        List<HistoryTier> tiers = HistoryTier.build(10800L, "1440:31536000, 60:604800");
        assertThat(tiers).extracting(t -> t.minutes).containsExactly(15, 60, 1440);
    }

    @Test
    @DisplayName("build: malformed entries are skipped, not thrown")
    void build_malformedEntriesSkipped()
    {
        List<HistoryTier> tiers = HistoryTier.build(10800L, "60:604800, garbage, 90:notanumber, :123, 45:");
        assertThat(tiers).extracting(t -> t.minutes).containsExactly(15, 60);
    }

    @Test
    @DisplayName("build: null finest threshold and blank csv -> empty list")
    void build_nullAndBlank_returnsEmpty()
    {
        assertThat(HistoryTier.build(null, "")).isEmpty();
        assertThat(HistoryTier.build(null, null)).isEmpty();
    }

    @Test
    @DisplayName("select: range under the finest threshold stays on raw (null)")
    void select_underFinestThreshold_returnsNull()
    {
        List<HistoryTier> tiers = HistoryTier.build(10800L, "60:604800");
        assertThat(HistoryTier.select(tiers, 3600L)).isNull();
    }

    @Test
    @DisplayName("select: range between tiers picks the finest exceeded tier")
    void select_betweenTiers_picksFinest()
    {
        List<HistoryTier> tiers = HistoryTier.build(10800L, "60:604800");
        HistoryTier chosen = HistoryTier.select(tiers, 20000L);
        assertThat(chosen.minutes).isEqualTo(15);
    }

    @Test
    @DisplayName("select: range beyond every threshold picks the coarsest tier")
    void select_beyondEveryThreshold_picksCoarsest()
    {
        List<HistoryTier> tiers = HistoryTier.build(10800L, "60:604800,1440:31536000");
        HistoryTier chosen = HistoryTier.select(tiers, 40000000L);
        assertThat(chosen.minutes).isEqualTo(1440);
    }

    @Test
    @DisplayName("select: empty tier list always returns null")
    void select_emptyTiers_returnsNull()
    {
        assertThat(HistoryTier.select(Collections.emptyList(), Long.MAX_VALUE)).isNull();
    }

    @Test
    @DisplayName("build: a coarser tier configured with a lower threshold than the finest tier gets clamped, not left inverted")
    void build_coarserTierWithLowerThreshold_isClamped()
    {
        // Live yucemonitoring misconfiguration (2026-08-27): finest=10800 (3h) but
        // history-tiers=60:3600 (1h) — without clamping, 60m's lower threshold would
        // silently win select()'s "last exceeded wins" walk for ANY range > 3600s,
        // making the 15m tier completely unreachable.
        List<HistoryTier> tiers = HistoryTier.build(10800L, "60:3600");
        assertThat(tiers).extracting(t -> t.minutes).containsExactly(15, 60);
        assertThat(tiers.get(0).thresholdSeconds).isEqualTo(10800L);
        assertThat(tiers.get(1).thresholdSeconds).isEqualTo(10800L); // clamped up from 3600
    }

    @Test
    @DisplayName("select: with a clamped coarser tier, a mid-range query no longer escalates prematurely")
    void select_clampedTier_noPrematureEscalation()
    {
        List<HistoryTier> tiers = HistoryTier.build(10800L, "60:3600");
        // 5000s clears the misconfigured raw 3600 threshold but not the finest 10800 —
        // pre-fix this returned the 60m tier; post-fix it must stay on raw (null).
        assertThat(HistoryTier.select(tiers, 5000L)).isNull();
    }

    @Test
    @DisplayName("build: three tiers with a middle one under-configured all clamp forward correctly")
    void build_multipleOutOfOrderTiers_clampSequentially()
    {
        List<HistoryTier> tiers = HistoryTier.build(10800L, "60:5000,1440:20000");
        assertThat(tiers).extracting(t -> t.minutes).containsExactly(15, 60, 1440);
        assertThat(tiers.get(0).thresholdSeconds).isEqualTo(10800L);
        assertThat(tiers.get(1).thresholdSeconds).isEqualTo(10800L); // clamped up from 5000
        assertThat(tiers.get(2).thresholdSeconds).isEqualTo(20000L); // already above 10800, untouched
    }
}
