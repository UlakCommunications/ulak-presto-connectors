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
package com.facebook.presto.influxdb;


import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Map;

import static com.facebook.presto.influxdb.RedisCacheWorker.DEFAULT_N_THREADS;

public class InfluxdbConnectorFactory
        implements ConnectorFactory
{
    public static final String TEXT_CONNECTOR_INFLUXDB = "influxdb";
//    public static final String TEXT_CONNECTOR_PG = "mayapg";
    private static Logger logger = LoggerFactory.getLogger(RedisCacheWorker.class);

    public String getName()
    {
        return TEXT_CONNECTOR_INFLUXDB;
//        return TEXT_CONNECTOR_PG;
    }

//    @Override
//    public ConnectorHandleResolver getHandleResolver()
//    {
//        return new InfluxdbHandleResolver();
//    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        String url = config.get("connection-url");
        String sNumThreads = config.get("number_of_worker_threads");
        int numThreads = DEFAULT_N_THREADS;
        if(sNumThreads != null && !sNumThreads.trim().equals("")){
            try {
                numThreads = Integer.parseInt(sNumThreads);
            }catch (Throwable e){
                logger.error("Unable to parse sNumThreads: " + sNumThreads,e);
            }
        }
        String sRunInCoordinatorOnly = config.get("run_in_coordinator_only");
        boolean runInCoordinatorOnly = false;
        if(sRunInCoordinatorOnly != null && !sRunInCoordinatorOnly.trim().equals(""))
        {
            runInCoordinatorOnly = sRunInCoordinatorOnly.trim().toLowerCase(Locale.ENGLISH).equals("true");
        }
        String sWorkerIndexToRunIn = config.get("worker_id_to_run_in");
        InfluxdbConnector connector = null;
        switch ( getName()){
            case TEXT_CONNECTOR_INFLUXDB:
                connector = new InfluxdbConnector(
                        url,
                        catalogName,
                        config.get("connection-org"),
                        config.get("connection-token"),
                        config.get("connection-bucket"),
                        config.get("redis-url"),
                        config.get("keywords"),
                        runInCoordinatorOnly,
                        context.getNodeManager().getCurrentNode().getNodeIdentifier(),
                        sWorkerIndexToRunIn,
                        context.getNodeManager().getCurrentNode().isCoordinator(),
                        numThreads,
                        config.get("pg-connection-url"),
                        config.get("pg-connection-user"),
                        config.get("pg-connection-password"),
                        config.get("qw-connection-url"),
                        config.get("qw-index"));
//            case TEXT_CONNECTOR_PG:
//                connector = new InfluxdbConnector(DBType.PG,
//                        url,
//                        catalogName,
//                        config.get("connection-org"),
//                        config.get("connection-token"),
//                        config.get("connection-bucket"),
//                        config.get("redis-url"),
//                        config.get("keywords"),
//                        runInCoordinatorOnly,
//                        context.getNodeManager().getCurrentNode().getNodeIdentifier(),
//                        sWorkerIndexToRunIn,
//                        context.getNodeManager().getCurrentNode().isCoordinator(),
//                        numThreads,
//                        config.get("pg-connection-url"),
//                        config.get("pg-connection-user"),
//                        config.get("pg-connection-password"));
        }

        return connector;
    }
}
