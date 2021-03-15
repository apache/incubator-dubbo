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

package org.apache.dubbo.rpc.cluster.router.mesh.route;

import org.apache.dubbo.common.config.configcenter.DynamicConfiguration;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.rpc.model.ApplicationModel;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;


public final class MeshRuleManager {

    public static final Logger logger = LoggerFactory.getLogger(MeshRuleManager.class);

    private static final String MESH_RULE_DATA_ID_SUFFIX = ".MESHAPPRULE";
    private static final String GROUP = "DEFAULT_GROUP";

    private static ConcurrentHashMap<String, MeshAppRuleListener> appRuleListeners = new ConcurrentHashMap<>();

    public static void subscribeAppRule(String app) {

        MeshAppRuleListener meshAppRuleListener = new MeshAppRuleListener(app);
        String appRuleDataId = app + MESH_RULE_DATA_ID_SUFFIX;
        DynamicConfiguration configuration = ApplicationModel.getEnvironment().getDynamicConfiguration()
                .orElseGet(null);

        if (configuration == null) {
            logger.warn("Doesn't support DynamicConfiguration!");
            return;
        }

        try {
            String rawConfig = configuration.getConfig(appRuleDataId, GROUP, 5000L);
            if (rawConfig != null) {
                meshAppRuleListener.receiveConfigInfo(rawConfig);
            }
        } catch (Throwable throwable) {
            logger.error("get MeshRuleManager app rule failed.", throwable);
        }

        configuration.addListener(appRuleDataId, GROUP, meshAppRuleListener);
        appRuleListeners.put(app, meshAppRuleListener);
    }

    public static void register(String app, MeshRuleRouter subscriber) {
        MeshAppRuleListener meshAppRuleListener = appRuleListeners.get(app);
        if (meshAppRuleListener == null) {
            logger.warn("appRuleListener can't find when Router register");
            return;
        }
        meshAppRuleListener.register(subscriber);
    }

    public static void unregister(MeshRuleRouter subscriber) {
        Collection<MeshAppRuleListener> listeners = appRuleListeners.values();
        for (MeshAppRuleListener listener : listeners) {
            listener.unregister(subscriber);
        }
    }

}
