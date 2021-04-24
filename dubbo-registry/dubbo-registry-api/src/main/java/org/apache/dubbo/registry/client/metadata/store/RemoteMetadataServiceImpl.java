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
package org.apache.dubbo.registry.client.metadata.store;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.metadata.MetadataInfo;
import org.apache.dubbo.metadata.WritableMetadataService;
import org.apache.dubbo.metadata.definition.ServiceDefinitionBuilder;
import org.apache.dubbo.metadata.definition.model.FullServiceDefinition;
import org.apache.dubbo.metadata.report.MetadataReport;
import org.apache.dubbo.metadata.report.MetadataReportInstance;
import org.apache.dubbo.metadata.report.identifier.MetadataIdentifier;
import org.apache.dubbo.metadata.report.identifier.SubscriberMetadataIdentifier;
import org.apache.dubbo.registry.client.ServiceInstance;
import org.apache.dubbo.registry.client.metadata.ServiceInstanceMetadataUtils;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.rpc.RpcException;

import java.util.Map;

import static org.apache.dubbo.common.constants.CommonConstants.APPLICATION_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.CONSUMER_SIDE;
import static org.apache.dubbo.common.constants.CommonConstants.GROUP_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.INTERFACE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PID_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PROVIDER_SIDE;
import static org.apache.dubbo.common.constants.CommonConstants.SIDE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.TIMESTAMP_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.VERSION_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.REGISTRY_CLUSTER_KEY;

public class RemoteMetadataServiceImpl {
    protected final Logger logger = LoggerFactory.getLogger(getClass());
    private WritableMetadataService localMetadataService;

    public RemoteMetadataServiceImpl(WritableMetadataService writableMetadataService) {
        this.localMetadataService = writableMetadataService;
    }

    public Map<String, MetadataReport> getMetadataReports() {
        return MetadataReportInstance.getMetadataReports(false);
    }

    public void publishMetadata(String serviceName) {
        Map<String, MetadataInfo> metadataInfos = localMetadataService.getMetadataInfos();
        metadataInfos.forEach((registryCluster, metadataInfo) -> {
            if (!metadataInfo.hasReported()) {
                SubscriberMetadataIdentifier identifier = new SubscriberMetadataIdentifier(serviceName, metadataInfo.calAndGetRevision());
                metadataInfo.calAndGetRevision();
                metadataInfo.getExtendParams().put(REGISTRY_CLUSTER_KEY, registryCluster);
                MetadataReport metadataReport = getMetadataReports().get(registryCluster);
                if (metadataReport == null) {
                    metadataReport = getMetadataReports().entrySet().iterator().next().getValue();
                }
                logger.info("Publishing metadata to " + metadataReport.getClass().getSimpleName());
                if (logger.isDebugEnabled()) {
                    logger.debug(metadataInfo.toString());
                }
                metadataReport.publishAppMetadata(identifier, metadataInfo);
                metadataInfo.markReported();
            }
        });
    }

    public MetadataInfo getMetadata(ServiceInstance instance) {
        SubscriberMetadataIdentifier identifier = new SubscriberMetadataIdentifier(instance.getServiceName(),
                ServiceInstanceMetadataUtils.getExportedServicesRevision(instance));

        String registryCluster = instance.getExtendParams().get(REGISTRY_CLUSTER_KEY);

        MetadataReport metadataReport = getMetadataReports().get(registryCluster);
        if (metadataReport == null) {
            metadataReport = getMetadataReports().entrySet().iterator().next().getValue();
        }
        return metadataReport.getAppMetadata(identifier, instance.getExtendParams());
    }

    public void publishServiceDefinition(URL url) {
        String side = url.getParameter(SIDE_KEY);
        if (PROVIDER_SIDE.equalsIgnoreCase(side)) {
            //TODO, the params part is duplicate with that stored by exportURL(url), can be further optimized in the future.
            publishProvider(url);
        }
    }

    private void publishProvider(URL providerUrl) throws RpcException {
        //first add into the list
        // remove the individual param
        providerUrl = providerUrl.removeParameters(PID_KEY, TIMESTAMP_KEY, Constants.BIND_IP_KEY,
                Constants.BIND_PORT_KEY, TIMESTAMP_KEY);

        try {
            String interfaceName = providerUrl.getParameter(INTERFACE_KEY);
            if (StringUtils.isNotEmpty(interfaceName)) {
                Class interfaceClass = Class.forName(interfaceName);
                FullServiceDefinition fullServiceDefinition = ServiceDefinitionBuilder.buildFullDefinition(interfaceClass,
                        providerUrl.getParameters());
                for (Map.Entry<String, MetadataReport> entry : getMetadataReports().entrySet()) {
                    MetadataReport metadataReport = entry.getValue();
                    metadataReport.storeProviderMetadata(new MetadataIdentifier(providerUrl.getServiceInterface(),
                            providerUrl.getParameter(VERSION_KEY), providerUrl.getParameter(GROUP_KEY),
                            PROVIDER_SIDE, providerUrl.getParameter(APPLICATION_KEY)), fullServiceDefinition);
                }
                return;
            }
            logger.error("publishProvider interfaceName is empty . providerUrl: " + providerUrl.toFullString());
        } catch (ClassNotFoundException e) {
            //ignore error
            logger.error("publishProvider getServiceDescriptor error. providerUrl: " + providerUrl.toFullString(), e);
        }
    }
}
