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
package org.apache.dubbo.metadata.store.failover;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.metadata.MappingListener;
import org.apache.dubbo.metadata.MetadataInfo;
import org.apache.dubbo.metadata.definition.model.ServiceDefinition;
import org.apache.dubbo.metadata.report.MetadataReport;
import org.apache.dubbo.metadata.report.identifier.MetadataIdentifier;
import org.apache.dubbo.metadata.report.identifier.ServiceMetadataIdentifier;
import org.apache.dubbo.metadata.report.identifier.SubscriberMetadataIdentifier;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;

public class MockMetadataReport implements MetadataReport {

    public URL url;

    public ConcurrentMap<MetadataIdentifier, ServiceDefinition>      providerMetadata  = new ConcurrentHashMap<>();
    public ConcurrentMap<SubscriberMetadataIdentifier, MetadataInfo> appMetadata       = new ConcurrentHashMap<>();
    public ConcurrentMap<String, Set<String>>                        appMapping        = new ConcurrentHashMap<>();
    public ConcurrentMap<ServiceMetadataIdentifier, List<String>>    serviceMetadata   = new ConcurrentHashMap<>();
    public ConcurrentMap<SubscriberMetadataIdentifier, Set<String>>  subscribeMetadata = new ConcurrentHashMap<>();

    public MockMetadataReport(URL url) {
        this.url = url;
    }

    @Override
    public void storeProviderMetadata(MetadataIdentifier providerMetadataIdentifier, ServiceDefinition serviceDefinition) {
        providerMetadata.put(providerMetadataIdentifier, serviceDefinition);
    }

    @Override
    public String getServiceDefinition(MetadataIdentifier metadataIdentifier) {
        ServiceDefinition definition = providerMetadata.get(metadataIdentifier);
        return definition == null ? null : definition.toString();
    }

    @Override
    public void publishAppMetadata(SubscriberMetadataIdentifier identifier, MetadataInfo metadataInfo) {
        appMetadata.put(identifier, metadataInfo);
    }

    @Override
    public MetadataInfo getAppMetadata(SubscriberMetadataIdentifier identifier, Map<String, String> instanceMetadata) {
        return appMetadata.get(identifier);
    }

    @Override
    public Set<String> getServiceAppMapping(String serviceKey, MappingListener listener, URL url) {
        return appMapping.get(serviceKey);
    }

    @Override
    public void registerServiceAppMapping(String serviceKey, String application, URL url) {
        appMapping.putIfAbsent(serviceKey, new ConcurrentHashSet<>());
        Set<String> appNames = appMapping.get(serviceKey);
        appNames.add(application);
    }

    @Override
    public List<String> getExportedURLs(ServiceMetadataIdentifier metadataIdentifier) {
        return serviceMetadata.get(metadataIdentifier);
    }

    @Override
    public void saveServiceMetadata(ServiceMetadataIdentifier metadataIdentifier, URL url) {
        serviceMetadata.putIfAbsent(metadataIdentifier, new CopyOnWriteArrayList<>());
        List<String> urls = serviceMetadata.get(metadataIdentifier);
        urls.add(url.toFullString());
    }

    @Override
    public void removeServiceMetadata(ServiceMetadataIdentifier metadataIdentifier) {
        serviceMetadata.remove(metadataIdentifier);
    }

    @Override
    public void saveSubscribedData(SubscriberMetadataIdentifier subscriberMetadataIdentifier, Set<String> urls) {
        subscribeMetadata.putIfAbsent(subscriberMetadataIdentifier, new CopyOnWriteArraySet());
        Set<String> metadataUrls = subscribeMetadata.get(subscriberMetadataIdentifier);
        metadataUrls.addAll(urls);
    }

    @Override
    public List<String> getSubscribedURLs(SubscriberMetadataIdentifier subscriberMetadataIdentifier) {
        Set<String> urls = subscribeMetadata.get(subscriberMetadataIdentifier);
        if (urls == null) { return Collections.EMPTY_LIST; }
        return new ArrayList<>(urls);
    }

    public void reset() {
        providerMetadata.clear();
        appMetadata.clear();
        appMapping.clear();
        serviceMetadata.clear();
        subscribeMetadata.clear();
    }
}