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

import com.facebook.presto.pg.PGUtil;
import com.facebook.presto.quickwit.QwUtil;
import io.trino.spi.connector.*;
import io.trino.spi.transaction.IsolationLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.facebook.presto.influxdb.RedisCacheWorker.setNumThreads;

public class InfluxdbConnector
        implements Connector
{
//    private DBType dbType=DBType.INFLUXDB2;
    static RedisCacheWorker redisCacheWorker = null;

    private static Logger logger = LoggerFactory.getLogger(InfluxdbConnector.class);
    private final InfluxdbMetadata metadata;

    private final InfluxdbSplitManager splitManager;

    private final InfluxdbRecordSetProvider recordSetProvider;

    public InfluxdbConnector(String url,
                             String catalogName,
                             String org,
                             String token,
                             String bucket,
                             String redisUrl,
                             String keywords,
                             boolean runInCoordinatorOnly,
                             String workerId,
                             String workerIndexToRunIn,
                             boolean isCoordinator,
                             int numThreads,
                             String pgUrl,
                             String pgUsername,
                             String pgPassword,
                             String qwUrl,
                             String qwIndex) {
        // need to get database connection here
        logger.debug("Connector by url: " + url);
//        switch (dbType) {
//            case INFLUXDB2:
        try {
            InfluxdbUtil.instance(url, org, token, bucket);
        } catch (IOException e) {
            logger.error("InfluxdbConnector", e);
        }
        if (pgUrl != null && !pgUrl.trim().equals("")) {
            try {
                PGUtil.instance(pgUrl, pgUsername, pgPassword);
            } catch (IOException e) {
                logger.error("InfluxdbConnector", e);
            }
        }

        if (qwUrl != null && !qwUrl.trim().equals("")) {
            QwUtil.instance(qwUrl, qwIndex);
        }
//                break;
//            case PG:
//                try {
//                    PGUtil.instance(pgUrl, pgUsername, pgPassword);
//                } catch (IOException e) {
//                    logger.error("InfluxdbConnector", e);
//                }
//                break;
//        }

        this.metadata = InfluxdbMetadata.getInstance(catalogName);
        this.splitManager = InfluxdbSplitManager.getInstance();
        this.recordSetProvider = InfluxdbRecordSetProvider.getInstance();
        InfluxdbUtil.redisUrl = redisUrl;
        InfluxdbUtil.workerId = workerId;
        InfluxdbUtil.workerIndexToRunIn = workerIndexToRunIn;
        InfluxdbUtil.setKeywords(keywords);
        setNumThreads(numThreads);
        InfluxdbUtil.isCoordinator = true;
        if (isCoordinator && runInCoordinatorOnly) {
            if (redisCacheWorker == null) {
                redisCacheWorker = new RedisCacheWorker();
                redisCacheWorker.start();
            }
        }
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {
        return InfluxdbTransactionHandle.INSTANCE;
    }

    public InfluxdbMetadata getMetadata() {
        return metadata;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle) {
        return metadata;
    }


    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider()
    {
        return recordSetProvider;
    }
}
