/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.quickwit;


import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Map;

import static com.facebook.presto.ulak.QueryParameters.replaceEnv;
import static com.facebook.presto.ulak.caching.RedisCacheWorker.DEFAULT_N_THREADS;


public class UlakQuickwitConnectorFactory
        implements ConnectorFactory
{
    public static final String TEXT_CONNECTOR_QW = "quickwit";
    private static Logger logger = LoggerFactory.getLogger(UlakQuickwitConnectorFactory.class);

    public String getName()
    {
        return TEXT_CONNECTOR_QW;
    }
    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        String url = StringUtils.strip(config.get("qw-connection-url")," /");
        String sNumThreads = config.get("number_of_worker_threads");
        int numThreads =DEFAULT_N_THREADS;
        if(sNumThreads != null && !sNumThreads.trim().isEmpty()){
            try {
                numThreads = Integer.parseInt(sNumThreads);
            }catch (Exception e){
                logger.error("Unable to parse sNumThreads: {}", sNumThreads, e);
            }
        }
        String sRunInCoordinatorOnly = config.get("run_in_coordinator_only");
        boolean runInCoordinatorOnly = false;
        if(sRunInCoordinatorOnly != null && !sRunInCoordinatorOnly.trim().isEmpty())
        {
            runInCoordinatorOnly = sRunInCoordinatorOnly.trim().toLowerCase(Locale.ENGLISH).equals("true");
        }
        String sWorkerIndexToRunIn = config.get("worker_id_to_run_in");
        String connectTimeout = config.get("connect-timeout");
        String readTimeout = config.get("read-timeout");
        String writeTimeout = config.get("write-timeout");
        String allowedUrls = config.get("qw-allowed-urls");
        // Additional escalation tiers beyond the finest one, e.g. "60:604800,1440:31536000"
        // (minutes:thresholdSeconds pairs) — see HistoryTier.build() for parsing/validation.
        String historyTiersCsv = config.get("history-tiers");

        String historyThreshold = config.get("history-time-threshold-seconds");
        Long historyTimeThresholdSeconds = null;
        if (historyThreshold != null && !historyThreshold.trim().isEmpty()) {
            String trimmed = historyThreshold.trim();
            if (trimmed.contains(":")) {
                // Explicit "minutes:thresholdSeconds" pair — same format as history-tiers
                // entries, so this catalog can state which granularity the finest tier
                // actually is instead of HistoryTier.build() silently assuming 15m. Folded
                // straight into historyTiersCsv (as just another tier, no special-casing)
                // rather than threading a new parameter through every call site that
                // already carries historyTimeThresholdSeconds/historyTiersCsv unchanged.
                String[] parts = trimmed.split(":", 2);
                try {
                    int minutes = Integer.parseInt(parts[0].trim());
                    long seconds = Long.parseLong(parts[1].trim());
                    if (minutes <= 0 || seconds < 0) {
                        throw new IllegalArgumentException("minutes and thresholdSeconds must be positive");
                    }
                    historyTiersCsv = trimmed + (StringUtils.isBlank(historyTiersCsv) ? "" : "," + historyTiersCsv);
                    // NOT left as null: QwUtil.select() substitutes its own system
                    // default (10800s) for a null Long before HistoryTier.build() ever
                    // sees it, which would silently resurrect a hardcoded 15m entry
                    // conflicting with the one just folded in above. The sentinel
                    // survives that substitution unchanged (it's a real, non-null Long).
                    historyTimeThresholdSeconds = HistoryTier.FINEST_TIER_SUPPRESSED;
                    logger.info("Configured finest history tier as {}m@{}s for catalog '{}' (folded into history-tiers)",
                            minutes, seconds, catalogName);
                } catch (RuntimeException e) {
                    logger.error("Unable to parse history-time-threshold-seconds '{}' as minutes:thresholdSeconds: {}",
                            trimmed, e.getMessage());
                    // historyTimeThresholdSeconds stays null here: malformed pair falls
                    // back to QwUtil.select()'s normal default-threshold behavior rather
                    // than leaving history routing entirely unconfigured.
                }
            } else {
                try {
                    historyTimeThresholdSeconds = Long.parseLong(trimmed);
                    logger.info("Configured historyTimeThresholdSeconds to {} seconds for catalog '{}'", historyTimeThresholdSeconds, catalogName);
                } catch (Exception e) {
                    logger.error("Unable to parse history-time-threshold-seconds: {}", historyThreshold, e);
                }
            }
        }
        return new UlakQuickwitConnector(
            url,
            catalogName,
            replaceEnv(config.get("redis-url"),"REDIS_PASSWORD",true),
            config.get("keywords"),
            runInCoordinatorOnly,
            context.getCurrentNode().getNodeIdentifier(),
            sWorkerIndexToRunIn,
            context.getCurrentNode().isCoordinator(),
            numThreads,
            config.get("qw-index"),
            connectTimeout == null ? null : Integer.parseInt(connectTimeout),
            readTimeout == null ? null : Integer.parseInt(readTimeout),
            writeTimeout == null ? null : Integer.parseInt(writeTimeout),
            allowedUrls,
            historyTimeThresholdSeconds,
            historyTiersCsv);
    }
}
