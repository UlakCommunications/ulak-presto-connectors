package com.facebook.presto.quickwit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * One rollup tier a history-enabled query can escalate into: once the query's time
 * range exceeds {@code thresholdSeconds}, route to the index whose granularity token is
 * {@code minutes} (see {@link HistoryIndexResolver}). {@code minutes} has no inherent
 * upper bound — a tier can be daily/weekly/yearly-scale, it's still just a minute count.
 *
 * Selection is driven by the query's date range, not its requested bucket width
 * ({@code resolution_in_seconds}/{@code fixed_interval}) — a dashboard's chosen bucket
 * width is independent of how much underlying data the query has to scan.
 */
public final class HistoryTier
{
    private static final Logger logger = LoggerFactory.getLogger(HistoryTier.class);

    public final int minutes;
    public final long thresholdSeconds;

    public HistoryTier(int minutes, long thresholdSeconds)
    {
        this.minutes = minutes;
        this.thresholdSeconds = thresholdSeconds;
    }

    @Override
    public String toString()
    {
        return minutes + "m@" + thresholdSeconds + "s";
    }

    /**
     * Builds the full, ascending-by-minutes tier list from the classic single
     * {@code history-time-threshold-seconds} value (today's only tier, always 15m) plus
     * an optional {@code history-tiers} CSV of additional {@code minutes:thresholdSeconds}
     * pairs, e.g. {@code "60:604800,1440:31536000"}. A malformed entry is logged and
     * skipped, never thrown — one bad config value can't take history routing down.
     */
    public static List<HistoryTier> build(Long finestTierThresholdSeconds, String additionalTiersCsv)
    {
        List<HistoryTier> tiers = new ArrayList<>();
        if (finestTierThresholdSeconds != null) {
            tiers.add(new HistoryTier(15, finestTierThresholdSeconds));
        }
        if (additionalTiersCsv != null && !additionalTiersCsv.trim().isEmpty()) {
            for (String entry : additionalTiersCsv.split(",")) {
                String e = entry.trim();
                if (e.isEmpty()) {
                    continue;
                }
                try {
                    String[] parts = e.split(":");
                    if (parts.length != 2) {
                        throw new IllegalArgumentException("expected minutes:thresholdSeconds");
                    }
                    int minutes = Integer.parseInt(parts[0].trim());
                    long thresholdSeconds = Long.parseLong(parts[1].trim());
                    if (minutes <= 0 || thresholdSeconds < 0) {
                        throw new IllegalArgumentException("minutes and thresholdSeconds must be positive");
                    }
                    tiers.add(new HistoryTier(minutes, thresholdSeconds));
                }
                catch (RuntimeException ex) {
                    logger.error("Ignoring malformed history-tiers entry '{}': {}", e, ex.getMessage());
                }
            }
        }
        tiers.sort(Comparator.comparingInt(t -> t.minutes));
        return tiers;
    }

    /**
     * Coarsest configured tier whose threshold the range still exceeds, or null if the
     * range doesn't clear even the finest tier (caller should stay on raw). Assumes
     * tiers are configured so coarser (larger-minutes) tiers have larger-or-equal
     * thresholds; {@link #build} sorts by minutes but does not enforce that ordering
     * on thresholds.
     */
    public static HistoryTier select(List<HistoryTier> tiers, long rangeSeconds)
    {
        HistoryTier chosen = null;
        for (HistoryTier t : tiers) {
            if (rangeSeconds > t.thresholdSeconds) {
                chosen = t;
            }
        }
        return chosen;
    }
}
