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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Map;


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
        String url = config.get("qw-connection-url");
        String sNumThreads = config.get("number_of_worker_threads");
        int numThreads =10;// DEFAULT_N_THREADS;
        if(sNumThreads != null && !sNumThreads.trim().isEmpty()){
            try {
                numThreads = Integer.parseInt(sNumThreads);
            }catch (Exception e){
                logger.error("Unable to parse sNumThreads: " + sNumThreads,e);
            }
        }
        String sRunInCoordinatorOnly = config.get("run_in_coordinator_only");
        boolean runInCoordinatorOnly = false;
        if(sRunInCoordinatorOnly != null && !sRunInCoordinatorOnly.trim().isEmpty())
        {
            runInCoordinatorOnly = sRunInCoordinatorOnly.trim().toLowerCase(Locale.ENGLISH).equals("true");
        }
        String sWorkerIndexToRunIn = config.get("worker_id_to_run_in");
        return new UlakQuickwitConnector(
            url,
            catalogName,
            config.get("redis-url"),
            config.get("keywords"),
            runInCoordinatorOnly,
            context.getNodeManager().getCurrentNode().getNodeIdentifier(),
            sWorkerIndexToRunIn,
            context.getNodeManager().getCurrentNode().isCoordinator(),
            numThreads,
            config.get("qw-index"));
    }
}
