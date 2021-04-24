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
import org.apache.dubbo.common.constants.RemotingConstants;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.metadata.MappingListener;
import org.apache.dubbo.metadata.MetadataInfo;
import org.apache.dubbo.metadata.definition.model.ServiceDefinition;
import org.apache.dubbo.metadata.report.MetadataReport;
import org.apache.dubbo.metadata.report.MetadataReportFactory;
import org.apache.dubbo.metadata.report.identifier.MetadataIdentifier;
import org.apache.dubbo.metadata.report.identifier.ServiceMetadataIdentifier;
import org.apache.dubbo.metadata.report.identifier.SubscriberMetadataIdentifier;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.dubbo.common.constants.CommonConstants.CHECK_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.COMMA_SPLIT_PATTERN;
import static org.apache.dubbo.common.constants.CommonConstants.REGISTRY_SPLIT_PATTERN;

/**
 * @author yiji@apache.org
 */
public class FailoverMetadataReport extends StrategyMetadataReport {

    private static final Logger logger = LoggerFactory.getLogger(FailoverMetadataReport.class);

    // proxy metadata report protocol, eg: zookeeper
    private static final String PROTOCOL_KEY = "protocol";

    private static final String CLUSTER_KEY = "clusters";

    // A cluster may have multiple instances
    private static final String HOST_KEY = "hosts";

    private static final Pattern HOST_SPLIT_PATTERN = Pattern.compile("\\s*[|:]+\\s*");

    // The metadata address of the agent.
    private List<URL> failoverUrls;

    // The metadata report instance.
    private List<MetadataReportHolder> proxyReports;

    // Local priority metadata center
    private MetadataReportHolder localDataCenterReportHolder;

    public FailoverMetadataReport(URL url) {
        super(url);
        this.failoverUrls = fetchBackupUrls();
        this.proxyReports = buildProxyReports();
    }

    protected List<URL> fetchBackupUrls() {
        String protocol = url.getParameter(PROTOCOL_KEY);
        if (protocol == null || !ExtensionLoader.getExtensionLoader(MetadataReportFactory.class).hasExtension(protocol)) {
            throw new IllegalArgumentException(
                    "No '" + protocol
                            + "' metadata report extension found, please check if metadata report module dependencies are included.");
        }

        List<URL> urls = new ArrayList<>();

        String clusters = this.url.getParameter(CLUSTER_KEY);
        String backupHost = this.url.getParameter(HOST_KEY);
        URL url = this.url.removeParameters(CLUSTER_KEY, HOST_KEY, PROTOCOL_KEY).setProtocol(protocol);

        URL metadataURL = url;
        if (backupHost != null && backupHost.length() > 0) {
            metadataURL = metadataURL.addParameter(RemotingConstants.BACKUP_KEY, backupHost);
        }
        urls.add(metadataURL);

        if (clusters != null && (clusters = clusters.trim()).length() > 0) {
            String[] addresses = REGISTRY_SPLIT_PATTERN.split(clusters);
            for (String address : addresses) {
                /**
                 * find multiple cluster hosts, supports multiple
                 * metadata report center read and write operations.
                 */
                String[] hosts = COMMA_SPLIT_PATTERN.split(address);
                if (hosts.length > 0) {
                    String node = hosts[0];
                    // contains user name and password with address ?
                    String username = null, password = null;
                    int index = node.indexOf("@");
                    if (index > 0) {
                        String[] authority = HOST_SPLIT_PATTERN.split(node.substring(0, index));
                        username = authority[0];
                        password = authority[1];
                        node = node.substring(index + 1);
                    }

                    String[] hostInfo = HOST_SPLIT_PATTERN.split(node);
                    String host = hostInfo[0];
                    int port = Integer.parseInt(hostInfo[1]);
                    URL clusterURL = new URL(protocol, username, password, host, port, url.getPath(), url.getParameters());
                    /**
                     * append backup address if required,
                     * the same cluster may have more than one node.
                     */
                    if (hosts.length > 1) {
                        StringBuilder buffer = new StringBuilder();
                        for (int i = 1; i < hosts.length; i++) {
                            if (i > 1) {
                                buffer.append(",");
                            }
                            buffer.append(hosts[i]);
                        }
                        clusterURL = clusterURL.addParameters(RemotingConstants.BACKUP_KEY, buffer.toString());
                    }
                    urls.add(clusterURL);
                }
            }
        }
        return urls;
    }

    protected List<MetadataReportHolder> buildProxyReports() {
        List<MetadataReportHolder> reports = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(this.failoverUrls)) {
            ExtensionLoader<MetadataReportFactory> factoryLoader = ExtensionLoader.getExtensionLoader(MetadataReportFactory.class);
            for (URL url : this.failoverUrls) {
                try {
                    MetadataReportHolder holder = new MetadataReportHolder(url,
                            factoryLoader.getExtension(url.getProtocol()).getMetadataReport(url));
                    reports.add(holder);
                } catch (Exception e) {
                    if (url.getParameter(CHECK_KEY, true)) {
                        throw new RuntimeException("Failed to create + '" + url.getProtocol() + "' metadata report extension instance", e);
                    }
                    if (logger.isWarnEnabled()) {
                        logger.warn("Failed to create + '" + url.getProtocol()
                                + "' metadata report extension instance, check=false found.");
                    }
                }
            }
        }

        Collections.shuffle(reports);

        /**
         * Select the local priority metadata cluster.
         * In order to prevent clients from all connecting
         * to the same cluster, random sorting has been done.
         */
        reports.forEach(holder -> {
            if (isLocalDataCenter(holder.url)) {
                this.localDataCenterReportHolder = holder;
            }
        });

        return reports;
    }

    @Override
    public void storeProviderMetadata(MetadataIdentifier providerMetadataIdentifier, ServiceDefinition serviceDefinition) {
        this.proxyReports.forEach((holder -> {
            if (shouldRegister(holder.url)) {
                try {
                    holder.report.storeProviderMetadata(providerMetadataIdentifier, serviceDefinition);
                } catch (Exception e) {
                    if (url.getParameter(CHECK_KEY, true)) {
                        throw e;
                    }
                }
            } else {
                if (logger.isInfoEnabled()) {
                    logger.info("Cancel to store provider metadata, register is false. url " + holder.url);
                }
            }
        }));
    }

    @Override
    public void publishAppMetadata(SubscriberMetadataIdentifier identifier, MetadataInfo metadataInfo) {
        this.proxyReports.forEach(holder -> {
            if (shouldRegister(holder.url)) {
                try {
                    holder.report.publishAppMetadata(identifier, metadataInfo);
                } catch (Exception e) {
                    if (url.getParameter(CHECK_KEY, true)) {
                        throw e;
                    }
                }
            } else {
                if (logger.isInfoEnabled()) {
                    logger.info("Cancel to publish app metadata, register is false. url " + holder.url);
                }
            }
        });
    }

    @Override
    public String getServiceDefinition(MetadataIdentifier metadataIdentifier) {
        /**
         * Support local region or datacenter to read first,
         * If current region or datacenter failed, it will be demoted to another region or datacenter.
         */
        MetadataReportHolder localReportHolder = this.localDataCenterReportHolder;
        if (localReportHolder != null && shouldQuery(localReportHolder.url)) {
            try {
                String definition = localReportHolder.report.getServiceDefinition(metadataIdentifier);
                if (definition != null && definition.length() > 0) {
                    return definition;
                }
            } catch (Exception e) {
                if (logger.isWarnEnabled()) {
                    logger.warn("Failed to get service definition from local metadata report center, url " + localReportHolder.url);
                }
            }
        }

        for (MetadataReportHolder holder : proxyReports) {
            /**
             * Skip the local region or datacenter read,
             * which was queried already.
             */
            if (localReportHolder != null
                    && holder.url == localReportHolder.url) {
                continue;
            }

            if (shouldQuery(holder.url)) {
                try {
                    String definition = holder.report.getServiceDefinition(metadataIdentifier);
                    if (definition != null && definition.length() > 0) {
                        return definition;
                    }
                } catch (Exception e) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Failed to get service definition from metadata report center, url " + holder.url);
                    }
                }
            }

            // should never happened.
            if (logger.isInfoEnabled()) {
                logger.info("Cancel to get service definition, should query is false. url " + holder.url);
            }
        }

        return null;
    }

    @Override
    public MetadataInfo getAppMetadata(SubscriberMetadataIdentifier identifier, Map<String, String> instanceMetadata) {
        /**
         * Support local region or datacenter to read first,
         * If current region or datacenter failed, it will be demoted to another region or datacenter.
         */
        MetadataReportHolder localReportHolder = this.localDataCenterReportHolder;
        if (localReportHolder != null && shouldQuery(localReportHolder.url)) {
            try {
                MetadataInfo metadataInfo = localReportHolder.report.getAppMetadata(identifier, instanceMetadata);
                if (metadataInfo != null) {
                    return metadataInfo;
                }
            } catch (Exception e) {
                if (logger.isWarnEnabled()) {
                    logger.warn("Failed to get app metadata from local metadata report center, url " + localReportHolder.url);
                }
            }
        }

        for (MetadataReportHolder holder : proxyReports) {
            /**
             * Skip the local region or datacenter read,
             * which was queried already.
             */
            if (localReportHolder != null
                    && holder.url == localReportHolder.url) {
                continue;
            }

            if (shouldQuery(holder.url)) {
                try {
                    MetadataInfo metadataInfo = holder.report.getAppMetadata(identifier, instanceMetadata);
                    if (metadataInfo != null) {
                        return metadataInfo;
                    }
                } catch (Exception e) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Failed to get app metadata from metadata report center, url " + holder.url);
                    }
                }
            }

            // should never happened.
            if (logger.isInfoEnabled()) {
                logger.info("Cancel to get app metadata, should query is false. url " + holder.url);
            }
        }

        return null;
    }

    @Override
    public Set<String> getServiceAppMapping(String serviceKey, MappingListener listener, URL url) {
        /**
         * Support local region or datacenter to read first,
         * If current region or datacenter failed, it will be demoted to another region or datacenter.
         */
        MetadataReportHolder localReportHolder = this.localDataCenterReportHolder;
        if (localReportHolder != null && shouldQuery(localReportHolder.url)) {
            try {
                Set<String> appMapping = localReportHolder.report.getServiceAppMapping(serviceKey, listener, url);
                if (appMapping != null && !appMapping.isEmpty()) {
                    return appMapping;
                }
            } catch (Exception e) {
                if (logger.isWarnEnabled()) {
                    logger.warn("Failed to get service mapping from local metadata report center, url " + localReportHolder.url);
                }
            }
        }

        for (MetadataReportHolder holder : proxyReports) {
            /**
             * Skip the local region or datacenter read,
             * which was queried already.
             */
            if (localReportHolder != null
                    && holder.url == localReportHolder.url) {
                continue;
            }

            if (shouldQuery(holder.url)) {
                try {
                    Set<String> appMapping = holder.report.getServiceAppMapping(serviceKey, listener, url);
                    if (appMapping != null && !appMapping.isEmpty()) {
                        return appMapping;
                    }
                } catch (Exception e) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Failed to get service mapping from metadata report center, url " + holder.url);
                    }
                }
            }

            // should never happened.
            if (logger.isInfoEnabled()) {
                logger.info("Cancel to get service mapping, should query is false. url " + holder.url);
            }
        }

        return Collections.EMPTY_SET;
    }

    @Override
    public void registerServiceAppMapping(String serviceKey, String application, URL url) {
        this.proxyReports.forEach(holder -> {
            if (shouldRegister(holder.url)) {
                try {
                    holder.report.registerServiceAppMapping(serviceKey, application, url);
                } catch (Exception e) {
                    if (url.getParameter(CHECK_KEY, true)) {
                        throw e;
                    }
                }
            } else {
                if (logger.isInfoEnabled()) {
                    logger.info("Cancel to register service app mapping, register is false. url " + holder.url);
                }
            }
        });
    }

    @Override
    public void saveServiceMetadata(ServiceMetadataIdentifier metadataIdentifier, URL url) {
        this.proxyReports.forEach(holder -> {
            if (shouldRegister(holder.url)) {
                try {
                    holder.report.saveServiceMetadata(metadataIdentifier, url);
                } catch (Exception e) {
                    if (url.getParameter(CHECK_KEY, true)) {
                        throw e;
                    }
                }
            } else {
                if (logger.isInfoEnabled()) {
                    logger.info("Cancel to register service app mapping, register is false. url " + holder.url);
                }
            }
        });
    }

    @Override
    public void saveSubscribedData(SubscriberMetadataIdentifier subscriberMetadataIdentifier, Set<String> urls) {
        this.proxyReports.forEach(holder -> {
            if (shouldRegister(holder.url)) {
                try {
                    holder.report.saveSubscribedData(subscriberMetadataIdentifier, urls);
                } catch (Exception e) {
                    if (url.getParameter(CHECK_KEY, true)) {
                        throw e;
                    }
                }
            } else {
                if (logger.isInfoEnabled()) {
                    logger.info("Cancel to register service app mapping, register is false. url " + holder.url);
                }
            }
        });
    }

    @Override
    public void removeServiceMetadata(ServiceMetadataIdentifier metadataIdentifier) {
        this.proxyReports.forEach(holder -> {
            if (shouldRegister(holder.url)) {
                try {
                    holder.report.removeServiceMetadata(metadataIdentifier);
                } catch (Exception e) {
                    if (url.getParameter(CHECK_KEY, true)) {
                        throw e;
                    }
                }
            }
        });
    }

    @Override
    public List<String> getExportedURLs(ServiceMetadataIdentifier metadataIdentifier) {
        /**
         * Support local region or datacenter to read first,
         * If current region or datacenter failed, it will be demoted to another region or datacenter.
         */
        MetadataReportHolder localReportHolder = this.localDataCenterReportHolder;
        if (localReportHolder != null && shouldQuery(localReportHolder.url)) {
            try {
                List<String> exportedURLs = localReportHolder.report.getExportedURLs(metadataIdentifier);
                if (CollectionUtils.isNotEmpty(exportedURLs)) {
                    return exportedURLs;
                }
            } catch (Exception e) {
                if (logger.isWarnEnabled()) {
                    logger.warn("Failed to get exported urls from local metadata report center, url " + localReportHolder.url);
                }
            }
        }

        for (MetadataReportHolder holder : proxyReports) {
            /**
             * Skip the local region or datacenter read,
             * which was queried already.
             */
            if (localReportHolder != null
                    && holder.url == localReportHolder.url) {
                continue;
            }

            if (shouldQuery(holder.url)) {
                try {
                    List<String> exportedURLs = holder.report.getExportedURLs(metadataIdentifier);
                    if (CollectionUtils.isNotEmpty(exportedURLs)) {
                        return exportedURLs;
                    }
                } catch (Exception e) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Failed to get exported urls from metadata report center, url " + holder.url);
                    }
                }
            }

            // should never happened.
            if (logger.isInfoEnabled()) {
                logger.info("Cancel to get exported urls, should query is false. url " + holder.url);
            }
        }

        return Collections.EMPTY_LIST;
    }

    @Override
    public List<String> getSubscribedURLs(SubscriberMetadataIdentifier subscriberMetadataIdentifier) {
        /**
         * Support local region or datacenter to read first,
         * If current region or datacenter failed, it will be demoted to another region or datacenter.
         */
        MetadataReportHolder localReportHolder = this.localDataCenterReportHolder;
        if (localReportHolder != null && shouldQuery(localReportHolder.url)) {
            try {
                List<String> subscribedURLs = localReportHolder.report.getSubscribedURLs(subscriberMetadataIdentifier);
                if (CollectionUtils.isNotEmpty(subscribedURLs)) {
                    return subscribedURLs;
                }
            } catch (Exception e) {
                if (logger.isWarnEnabled()) {
                    logger.warn("Failed to get subscribed urls from local metadata report center, url " + localReportHolder.url);
                }
            }
        }

        for (MetadataReportHolder holder : proxyReports) {
            /**
             * Skip the local region or datacenter read,
             * which was queried already.
             */
            if (localReportHolder != null
                    && holder.url == localReportHolder.url) {
                continue;
            }

            if (shouldQuery(holder.url)) {
                try {
                    List<String> subscribedURLs = holder.report.getSubscribedURLs(subscriberMetadataIdentifier);
                    if (CollectionUtils.isNotEmpty(subscribedURLs)) {
                        return subscribedURLs;
                    }
                } catch (Exception e) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Failed to get subscribed urls from metadata report center, url " + holder.url);
                    }
                }
            }

            // should never happened.
            if (logger.isInfoEnabled()) {
                logger.info("Cancel to get subscribed urls, should query is false. url " + holder.url);
            }
        }

        return Collections.EMPTY_LIST;
    }

    public List<MetadataReportHolder> getProxyReports() {
        return proxyReports;
    }

    class MetadataReportHolder {

        final URL url;
        final MetadataReport report;

        public MetadataReportHolder(URL url, MetadataReport report) {
            this.url = url;
            this.report = report;
        }
    }
}