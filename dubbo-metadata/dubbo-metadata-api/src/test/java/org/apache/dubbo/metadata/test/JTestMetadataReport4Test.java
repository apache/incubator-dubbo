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
package org.apache.dubbo.metadata.test;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.metadata.report.identifier.KeyTypeEnum;
import org.apache.dubbo.metadata.report.identifier.MetadataIdentifier;
import org.apache.dubbo.metadata.report.identifier.ServiceMetadataIdentifier;
import org.apache.dubbo.metadata.report.identifier.SubscriberMetadataIdentifier;
import org.apache.dubbo.metadata.report.support.AbstractMetadataReport;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.dubbo.common.constants.CommonConstants.SIDE_KEY;

/**
 * ZookeeperRegistry
 */
public class JTestMetadataReport4Test extends AbstractMetadataReport {

    public JTestMetadataReport4Test(URL url) {
        super(url);
    }

    public volatile Map<String, String> store = new ConcurrentHashMap<>();


    private static String getProtocol(URL url) {
        String protocol = url.getParameter(SIDE_KEY);
        protocol = protocol == null ? url.getProtocol() : protocol;
        return protocol;
    }

    @Override
    protected void doStoreProviderMetadata(MetadataIdentifier providerMetadataIdentifier, String serviceDefinitions) {
        store.put(providerMetadataIdentifier.getUniqueKey(KeyTypeEnum.UNIQUE_KEY), serviceDefinitions);
    }

    @Override
    protected void doSaveMetadata(ServiceMetadataIdentifier metadataIdentifier, URL url) {
        store.put(metadataIdentifier.getUniqueKey(KeyTypeEnum.UNIQUE_KEY), url.toFullString());
    }

    @Override
    protected void doRemoveMetadata(ServiceMetadataIdentifier metadataIdentifier) {
        store.remove(metadataIdentifier.getUniqueKey(KeyTypeEnum.UNIQUE_KEY));
    }

    @Override
    protected List<String> doGetExportedURLs(ServiceMetadataIdentifier metadataIdentifier) {
        return Arrays.asList(store.getOrDefault(metadataIdentifier.getUniqueKey(KeyTypeEnum.UNIQUE_KEY), ""));
    }

    @Override
    protected void doSaveSubscriberData(SubscriberMetadataIdentifier subscriberMetadataIdentifier, String urls) {
        store.put(subscriberMetadataIdentifier.getUniqueKey(KeyTypeEnum.UNIQUE_KEY), urls);
    }

    @Override
    protected String doGetSubscribedURLs(SubscriberMetadataIdentifier metadataIdentifier) {
        throw new UnsupportedOperationException("This extension does not support working as a remote metadata center.");
    }

    public static String getProviderKey(URL url) {
        return new MetadataIdentifier(url).getUniqueKey(KeyTypeEnum.UNIQUE_KEY);
    }

    public static String getConsumerKey(URL url) {
        return new MetadataIdentifier(url).getUniqueKey(KeyTypeEnum.UNIQUE_KEY);
    }

    @Override
    public String getServiceDefinition(MetadataIdentifier consumerMetadataIdentifier) {
        return store.get(consumerMetadataIdentifier.getUniqueKey(KeyTypeEnum.UNIQUE_KEY));
    }
}
