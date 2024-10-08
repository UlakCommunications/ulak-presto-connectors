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

import com.facebook.presto.ulak.caching.ConnectorBaseUtil;
import com.facebook.presto.ulak.UlakRecordSetProvider;
import com.facebook.presto.ulak.UlakSplitManager;
import com.facebook.presto.ulak.UlakTransactionHandle;
import com.facebook.presto.ulak.DBType;
import com.facebook.presto.ulak.QueryParameters;
import com.facebook.presto.ulak.caching.RedisCacheWorker;
import com.quickwit.javaclient.ApiException;
import io.trino.spi.connector.*;
import io.trino.spi.transaction.IsolationLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;

public class UlakQuickwitConnector
        implements Connector
{
    private RedisCacheWorker redisCacheWorker;
    private String qwUrl;
    private String qwIndex;
    private static Logger logger = LoggerFactory.getLogger(UlakQuickwitConnector.class);
    private final UlakQuickwitMetadata metadata;

    private final UlakSplitManager splitManager;

    private final UlakRecordSetProvider recordSetProvider;

    public UlakQuickwitConnector(String url,
                             String catalogName,
                             String redisUrl,
                             String keywords,
                             boolean runInCoordinatorOnly,
                             String workerId,
                             String workerIndexToRunIn,
                             boolean isCoordinator,
                             int numThreads,
                             String qwIndex) {
        // need to get database connection here
        logger.debug("Connector by url: {}", url);

        if (url != null && !url.trim().isEmpty()) {
            this.setQwUrl(url);
        }
        if (qwIndex != null && !qwIndex.trim().isEmpty()) {
            this.setQwIndex(qwIndex);
        }

        this.setQwIndex(qwIndex);
        this.metadata = UlakQuickwitMetadata.getInstance(catalogName,qwUrl,qwIndex);
        this.splitManager = UlakSplitManager.getInstance();
        this.recordSetProvider = UlakRecordSetProvider.getInstance(((q,s)-> {
            try {
//                logger.error("From connector : {}\n\n\nurl:{}\n\n\nindex:{}",
//                        s[0],
//                        s[1],
//                        s[2]);

                return QwUtil.select(q,s[1],s[2]);
            } catch (ApiException e) {
                logger.error("Connector by url: {}", url, e);
                throw new RuntimeException(e);
                }
        }), new String[]{qwUrl,qwIndex});

        ConnectorBaseUtil.redisUrl = redisUrl;
        ConnectorBaseUtil.workerId = workerId;
        ConnectorBaseUtil.workerIndexToRunIn = workerIndexToRunIn;
        ConnectorBaseUtil.setKeywords(keywords);
        ConnectorBaseUtil.isCoordinator = true;
        if ((isCoordinator && runInCoordinatorOnly) && redisCacheWorker == null) {
                redisCacheWorker = new RedisCacheWorker((q,s)-> {
                    try {
                        return  QwUtil.select(q, qwUrl, qwIndex) ;
                    } catch (ApiException e) {
                        logger.error("InfluxdbConnector", e);
                        throw new RuntimeException(e);
                    }
                },numThreads, DBType.QW);
                redisCacheWorker.start();
        }
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {
        return UlakTransactionHandle.INSTANCE;
    }

    public UlakQuickwitMetadata getMetadata() {
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

    public String getQwUrl() {
        return qwUrl;
    }

    public void setQwUrl(String qwUrl) {
        this.qwUrl = qwUrl;
    }

    public String getQwIndex() {
        return qwIndex;
    }

    public void setQwIndex(String qwIndex) {
        this.qwIndex = qwIndex;
    }
}
