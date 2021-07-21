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
package org.apache.dubbo.qos.command.util;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.registry.Registry;
import org.apache.dubbo.registry.support.AbstractRegistry;
import org.apache.dubbo.registry.support.AbstractRegistryFactory;
import org.apache.dubbo.rpc.model.ConsumerModel;
import org.apache.dubbo.rpc.model.ProviderModel;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class ServiceCheckUtils {

    public static boolean isRegistered(ProviderModel providerModel) {
        // TODO, only check the status of one registry and no protocol now.
        Collection<Registry> registries = AbstractRegistryFactory.getRegistries();
        if (CollectionUtils.isNotEmpty(registries)) {
            AbstractRegistry abstractRegistry = (AbstractRegistry) registries.iterator().next();
            if (abstractRegistry.getRegistered().stream().anyMatch(url -> url.getServiceKey().equals(providerModel.getServiceKey()))) {
                return true;
            }
        }
        return false;
    }

    public static int getConsumerAddressNum(ConsumerModel consumerModel) {
        // TODO, only check one registry by default.
        int num = 0;

        Collection<Registry> registries = AbstractRegistryFactory.getRegistries();
        if (CollectionUtils.isNotEmpty(registries)) {
            AbstractRegistry abstractRegistry = (AbstractRegistry) registries.iterator().next();
            for (Map.Entry<URL, Map<String, List<URL>>> entry : abstractRegistry.getNotified().entrySet()) {
                if (entry.getKey().getServiceKey().equals(consumerModel.getServiceKey())) {
                    if (CollectionUtils.isNotEmptyMap(entry.getValue())) {
                        num = entry.getValue().size();
                    }
                }
            }
        }
        return num;
    }

    //for the situation that QoS wants to query the address number of a consumer in all Registries
    public static int getConsumerAddressNumInAllRegistries(ConsumerModel consumerModel){
        int num = 0;

        Collection<Registry> registries = AbstractRegistryFactory.getRegistries();
        if(CollectionUtils.isNotEmpty(registries)){
            for(Registry registry: registries){
                AbstractRegistry abstractRegistry = (AbstractRegistry) registry;
                for(Map.Entry<URL, Map<String, List<URL>>> entry: abstractRegistry.getNotified().entrySet()){
                    if(entry.getKey().getServiceKey().equals(consumerModel.getServiceKey())){
                        if(CollectionUtils.isNotEmptyMap(entry.getValue())){
                            num += entry.getValue().size();
                        }
                    }
                }
            }
        }
        return num;
    }
}
