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
     * {@code history-time-threshold-seconds} value plus an optional {@code history-tiers}
     * CSV of additional {@code minutes:thresholdSeconds} pairs, e.g.
     * {@code "60:604800,1440:31536000"}. {@code finestTierThresholdSeconds} is assumed to
     * be the 15m tier (today's implicit default) UNLESS the catalog config expressed
     * {@code history-time-threshold-seconds} itself as an explicit {@code minutes:seconds}
     * pair — {@link UlakQuickwitConnectorFactory} folds that case straight into
     * {@code additionalTiersCsv} instead and passes {@code null} here, so this hardcoded
     * 15m entry is skipped rather than conflicting with the catalog's own choice. A
     * malformed entry is logged and skipped, never thrown — one bad config value can't
     * take history routing down.
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

        // select() walks tiers in this (ascending-by-minutes) order and keeps
        // overwriting its answer with the last one whose threshold is exceeded — so a
        // coarser tier configured with a *smaller* threshold than a finer tier would
        // win prematurely and make the finer tier unreachable no matter the range
        // (e.g. finest 15m@10800s + additional "60:3600" starves 15m entirely, since
        // any range that clears 10800s already clears 3600s too). Clamp each tier's
        // threshold to be no smaller than the previous (finer) tier's, so a
        // misconfigured entry degrades to "same threshold as the finer tier" instead
        // of silently disabling routing to everything finer than it.
        long minThreshold = 0;
        for (int i = 0; i < tiers.size(); i++) {
            HistoryTier t = tiers.get(i);
            if (t.thresholdSeconds < minThreshold) {
                logger.warn("history tier {}m@{}s has a lower threshold than a finer tier ({}s) — clamping so it can't starve the finer tier",
                        t.minutes, t.thresholdSeconds, minThreshold);
                t = new HistoryTier(t.minutes, minThreshold);
                tiers.set(i, t);
            }
            minThreshold = t.thresholdSeconds;
        }

        return tiers;
    }

    /**
     * Coarsest configured tier whose threshold the range still exceeds, or null if the
     * range doesn't clear even the finest tier (caller should stay on raw). {@link #build}
     * enforces that coarser (larger-minutes) tiers have larger-or-equal thresholds, so
     * iteration order here reliably picks the coarsest applicable tier.
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
