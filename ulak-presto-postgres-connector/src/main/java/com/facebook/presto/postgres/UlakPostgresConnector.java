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
package com.facebook.presto.postgres;

import com.facebook.presto.ulak.caching.ConnectorBaseUtil;
import com.facebook.presto.ulak.UlakRecordSetProvider;
import com.facebook.presto.ulak.UlakSplitManager;
import com.facebook.presto.ulak.UlakTransactionHandle;
import com.facebook.presto.ulak.DBType;
import com.facebook.presto.ulak.QueryParameters;
import com.facebook.presto.ulak.caching.RedisCacheWorker;
import io.trino.spi.connector.*;
import io.trino.spi.transaction.IsolationLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;

public class UlakPostgresConnector
        implements Connector
{
    private RedisCacheWorker redisCacheWorker;
    private static Logger logger = LoggerFactory.getLogger(UlakPostgresConnector.class);
    private final UlakPostgresMetadata metadata;
    private final UlakSplitManager splitManager;
    private final UlakRecordSetProvider recordSetProvider;
    private static final String ERRORSTRING = "UlakPostgresConnector Error: {}";

    public UlakPostgresConnector(String url,
                             String catalogName,
                             String redisUrl,
                             String keywords,
                             boolean runInCoordinatorOnly,
                             String workerId,
                             String workerIndexToRunIn,
                             boolean isCoordinator,
                             int numThreads,
                             String pgUrl,
                             String pgUsername,
                             String pgPassword) {
        // need to get database connection here
        logger.debug("Connector by url: {}", url);
        if (pgUrl != null && !pgUrl.trim().isEmpty()) {
            try {
                PGUtil.instance(pgUrl, pgUsername, pgPassword);
            } catch (IOException e) {
                logger.error(ERRORSTRING, e.toString());
            }
        }

        this.metadata = UlakPostgresMetadata.getInstance(catalogName);
        this.splitManager = UlakSplitManager.getInstance();
        this.recordSetProvider = UlakRecordSetProvider.getInstance(s-> {
            try {
                return PGUtil.select(s);
            } catch (IOException | SQLException e) {
                logger.error(ERRORSTRING, e.toString());
                throw new RuntimeException(e);
            }
        });
        ConnectorBaseUtil.redisUrl = redisUrl;
        ConnectorBaseUtil.workerId = workerId;
        ConnectorBaseUtil.workerIndexToRunIn = workerIndexToRunIn;
        ConnectorBaseUtil.setKeywords(keywords);
        redisCacheWorker.setNumThreads(numThreads);
        ConnectorBaseUtil.isCoordinator = true;
        if ((isCoordinator && runInCoordinatorOnly) && redisCacheWorker == null) {
            redisCacheWorker = new RedisCacheWorker((QueryParameters s)-> {
                try {
                    return  PGUtil.select(s) ;
                } catch (IOException | SQLException e) {
                    logger.error(ERRORSTRING, e.toString());
                    throw new RuntimeException(e);
                }
            },numThreads, DBType.PG);
            redisCacheWorker.start();
        }
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {
        return UlakTransactionHandle.INSTANCE;
    }

    public UlakPostgresMetadata getMetadata() {
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
