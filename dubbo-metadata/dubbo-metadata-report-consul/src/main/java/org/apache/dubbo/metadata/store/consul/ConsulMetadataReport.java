/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dubbo.metadata.store.consul;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.metadata.report.identifier.BaseMetadataIdentifier;
import org.apache.dubbo.metadata.report.identifier.KeyTypeEnum;
import org.apache.dubbo.metadata.report.identifier.MetadataIdentifier;
import org.apache.dubbo.metadata.report.identifier.ServiceMetadataIdentifier;
import org.apache.dubbo.metadata.report.identifier.SubscriberMetadataIdentifier;
import org.apache.dubbo.metadata.report.support.AbstractMetadataReport;
import org.apache.dubbo.rpc.RpcException;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.kv.model.GetValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.dubbo.common.constants.ConsulConstants.DEFAULT_PORT;
import static org.apache.dubbo.common.constants.ConsulConstants.INVALID_PORT;

/**
 * metadata report impl for consul
 */
public class ConsulMetadataReport extends AbstractMetadataReport {

    private ConsulClient client;

    public ConsulMetadataReport(URL url) {
        super(url);

        String host = url.getHost();
        int port = INVALID_PORT != url.getPort() ? url.getPort() : DEFAULT_PORT;
        client = new ConsulClient(host, port);
    }

    @Override
    protected void doStoreProviderMetadata(MetadataIdentifier providerMetadataIdentifier, String serviceDefinitions) {
        this.storeMetadata(providerMetadataIdentifier, serviceDefinitions);
    }

    @Override
    protected void doSaveMetadata(ServiceMetadataIdentifier serviceMetadataIdentifier, URL url) {
        this.storeMetadata(serviceMetadataIdentifier, URL.encode(url.toFullString()));
    }

    @Override
    protected void doRemoveMetadata(ServiceMetadataIdentifier serviceMetadataIdentifier) {
        this.deleteMetadata(serviceMetadataIdentifier);
    }

    @Override
    protected List<String> doGetExportedURLs(ServiceMetadataIdentifier metadataIdentifier) {
        //todo encode and decode
        String content = getMetadata(metadataIdentifier);
        if (StringUtils.isEmpty(content)) {
            return Collections.emptyList();
        }
        return new ArrayList<String>(Arrays.asList(URL.decode(content)));
    }

    @Override
    protected void doSaveSubscriberData(SubscriberMetadataIdentifier subscriberMetadataIdentifier, String urlListStr) {
        this.storeMetadata(subscriberMetadataIdentifier, urlListStr);
    }

    @Override
    protected String doGetSubscribedURLs(SubscriberMetadataIdentifier subscriberMetadataIdentifier) {
        return getMetadata(subscriberMetadataIdentifier);
    }

    private void storeMetadata(BaseMetadataIdentifier identifier, String v) {
        try {
            client.setKVValue(identifier.getUniqueKey(KeyTypeEnum.UNIQUE_KEY), v);
        } catch (Throwable t) {
            logger.error("Failed to put " + identifier + " to consul " + v + ", cause: " + t.getMessage(), t);
            throw new RpcException("Failed to put " + identifier + " to consul " + v + ", cause: " + t.getMessage(), t);
        }
    }

    private void deleteMetadata(BaseMetadataIdentifier identifier) {
        try {
            client.deleteKVValue(identifier.getUniqueKey(KeyTypeEnum.UNIQUE_KEY));
        } catch (Throwable t) {
            logger.error("Failed to delete " + identifier + " from consul , cause: " + t.getMessage(), t);
            throw new RpcException("Failed to delete " + identifier + " from consul , cause: " + t.getMessage(), t);
        }
    }

    private String getMetadata(BaseMetadataIdentifier identifier) {
        try {
            Response<GetValue> value = client.getKVValue(identifier.getUniqueKey(KeyTypeEnum.UNIQUE_KEY));
            //FIXME CHECK
            if (value != null && value.getValue() != null) {
                //todo check decode value and value diff
                return value.getValue().getValue();
            }
            return null;
        } catch (Throwable t) {
            logger.error("Failed to get " + identifier + " from consul , cause: " + t.getMessage(), t);
            throw new RpcException("Failed to get " + identifier + " from consul , cause: " + t.getMessage(), t);
        }
    }

    @Override
    public String getServiceDefinition(MetadataIdentifier metadataIdentifier) {
        return getMetadata(metadataIdentifier);
    }
}
