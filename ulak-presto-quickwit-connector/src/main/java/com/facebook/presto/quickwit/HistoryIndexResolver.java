package com.facebook.presto.quickwit;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Given a caller-supplied {@code history_index} name (today always a 15-minute rollup
 * index, e.g. {@code metrics3_15} or {@code rollup_15m_site_app}) and a target tier
 * chosen by {@link HistoryTier} from the query's date range, swaps the granularity
 * number embedded in the index name for the target one — e.g. escalating to the 60m
 * tier against {@code metrics3_15} resolves to {@code metrics3_60}.
 *
 * Every failure mode (ambiguous index name, no candidate token) falls back to the
 * original, unmodified {@code historyIndex} — this must never change behaviour for a
 * name it can't confidently resolve, since it sits in front of every Grafana panel in
 * the anomaly stack.
 */
public final class HistoryIndexResolver
{
    private static final Logger logger = LoggerFactory.getLogger(HistoryIndexResolver.class);

    // A granularity token must be 2+ digits so it can never collide with the version
    // digit embedded in a base index name (the "3" in metrics3 / flows3).
    private static final Pattern GRANULARITY_TOKEN = Pattern.compile("(?<!\\d)\\d{2,}(?!\\d)");

    private HistoryIndexResolver() {}

    public static String resolve(String historyIndex, int targetMinutes)
    {
        if (StringUtils.isBlank(historyIndex) || targetMinutes <= 0) {
            return historyIndex;
        }
        try {
            Matcher tokens = GRANULARITY_TOKEN.matcher(historyIndex);
            int matchCount = 0;
            int start = -1;
            int end = -1;
            String matched = null;
            while (tokens.find()) {
                matchCount++;
                start = tokens.start();
                end = tokens.end();
                matched = tokens.group();
            }
            if (matchCount != 1) {
                logger.debug("Not deriving history index for '{}': expected exactly one granularity token, found {}",
                        historyIndex, matchCount);
                return historyIndex;
            }

            String replacement = String.valueOf(targetMinutes);
            if (replacement.equals(matched)) {
                return historyIndex;
            }

            String derived = historyIndex.substring(0, start) + replacement + historyIndex.substring(end);
            logger.debug("Derived history index '{}' -> '{}' (target {}m)", historyIndex, derived, targetMinutes);
            return derived;
        }
        catch (RuntimeException e) {
            logger.warn("Failed to derive history index for '{}', keeping as-is: {}", historyIndex, e.getMessage());
            return historyIndex;
        }
    }
}
