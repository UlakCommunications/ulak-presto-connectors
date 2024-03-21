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

import io.trino.spi.connector.*;
import io.trino.spi.transaction.IsolationLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class InfluxdbConnector
        implements Connector
{
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
                             String keywords)
    {
        // need to get database connection here
        System.out.println("初始化connector by url: " + url);
        //logger.debug("初始化connector by url: {}", url);
        try {
            InfluxdbUtil.instance(url,org,token,bucket);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        this.metadata = InfluxdbMetadata.getInstance(catalogName);
        this.splitManager = InfluxdbSplitManager.getInstance();
        this.recordSetProvider = InfluxdbRecordSetProvider.getInstance();
        InfluxdbUtil.redisUrl = redisUrl;
        InfluxdbUtil.setKeywords(keywords);
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
