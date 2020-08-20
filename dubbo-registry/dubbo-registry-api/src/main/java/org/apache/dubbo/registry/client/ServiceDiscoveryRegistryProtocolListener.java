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
package org.apache.dubbo.registry.client;

import org.apache.dubbo.common.config.configcenter.ConfigChangedEvent;
import org.apache.dubbo.common.config.configcenter.ConfigurationListener;
import org.apache.dubbo.common.config.configcenter.DynamicConfiguration;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.registry.integration.InterfaceCompatibleRegistryProtocol;
import org.apache.dubbo.registry.integration.MigrationRuleListener;
import org.apache.dubbo.registry.integration.RegistryProtocolListener;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.model.ApplicationModel;

import java.util.Set;

public class ServiceDiscoveryRegistryProtocolListener implements RegistryProtocolListener, ConfigurationListener {
    private static final Logger logger = LoggerFactory.getLogger(ServiceDiscoveryRegistryProtocolListener.class);
    private static final String RULE_KEY = ApplicationModel.getName() + ".MIGRATION";
    private static final String DUBBO_SERVICEDISCOVERY_MIGRATION = "DUBBO_SERVICEDISCOVERY_MIGRATION";

    private Set<MigrationRuleListener> listeners = new ConcurrentHashSet<>();
    private DynamicConfiguration configuration;

    private volatile String rawRule;

    public ServiceDiscoveryRegistryProtocolListener() {
        this.configuration = ApplicationModel.getEnvironment().getDynamicConfiguration().orElseGet(null);

        configuration.addListener(RULE_KEY, this);

        String rawRule = configuration.getConfig(RULE_KEY, DUBBO_SERVICEDISCOVERY_MIGRATION);
        process(new ConfigChangedEvent(RULE_KEY, DUBBO_SERVICEDISCOVERY_MIGRATION, rawRule));
    }

    @Override
    public synchronized void process(ConfigChangedEvent event) {
        rawRule = event.getContent();
        if (StringUtils.isEmpty(rawRule)) {
            logger.warn("Received empty migration rule, will ignore.");
            return;
        }

        if (CollectionUtils.isNotEmpty(listeners)) {
            listeners.forEach(listener -> listener.doMigrate(rawRule));
        }
    }

    @Override
    public synchronized void onExport(RegistryProtocol registryProtocol, Exporter<?> exporter) {

    }

    @Override
    public synchronized <T> void onRefer(RegistryProtocol registryProtocol, Invoker<T> invoker) {
        InterfaceCompatibleRegistryProtocol.MigrationInvoker<T> migrationInvoker
                = (InterfaceCompatibleRegistryProtocol.MigrationInvoker<T>) invoker;

        MigrationRuleListener<T> migrationListener = new MigrationRuleListener<T>(migrationInvoker);
        listeners.add(migrationListener);

        migrationListener.doMigrate(rawRule);
    }

    @Override
    public void onDestroy() {
        configuration.removeListener(RULE_KEY, this);
    }
}
