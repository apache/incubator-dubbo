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
package org.apache.dubbo.rpc.model;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.config.ServiceConfigBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ProviderModelTest {

    @Test
    public void test() {
        String serviceKey = Demo.class.getName();
        Object serviceInstance = new DemoImpl();
        ServiceDescriptor serviceModel = new ServiceDescriptor(Demo.class);
        ServiceConfigBase<?> serviceConfig = Mockito.mock(ServiceConfigBase.class);
        ServiceMetadata serviceMetadata = new ServiceMetadata();
        serviceMetadata.setServiceKey(serviceKey);

        ProviderModel providerModel = new ProviderModel(serviceKey, serviceInstance, serviceModel, serviceConfig, serviceMetadata);

        Assertions.assertEquals(providerModel.getServiceKey(), serviceKey);
        Assertions.assertEquals(providerModel.getServiceInstance(), serviceInstance);
        Assertions.assertEquals(providerModel.getServiceModel(), serviceModel);
        Assertions.assertEquals(providerModel.getServiceConfig(), serviceConfig);
        Assertions.assertEquals(providerModel.getServiceMetadata(), serviceMetadata);
        Assertions.assertEquals(providerModel.getServiceName(), serviceKey);
        Assertions.assertEquals(providerModel.getAllMethods().size(), 1);
        Assertions.assertEquals(providerModel.getStatedUrl().size(), 0);

        // test RegisterStatedURL
        URL registryUrl = URL.valueOf("zookeeper://127.0.0.1:2181");
        URL registeredProviderUrl = URL.valueOf("dubbo://1.1.1.1:8080/test");
        boolean registered = true;
        ProviderModel.RegisterStatedURL statedURL = new ProviderModel.RegisterStatedURL(registeredProviderUrl, registryUrl, registered);
        providerModel.addStatedUrl(statedURL);

        Assertions.assertEquals(providerModel.getStatedUrl().size(), 1);
        Assertions.assertEquals(providerModel.getStatedUrl().get(0), statedURL);
        Assertions.assertEquals(providerModel.getStatedUrl().get(0).getRegistryUrl(), registryUrl);
        Assertions.assertEquals(providerModel.getStatedUrl().get(0).getProviderUrl(), registeredProviderUrl);
        Assertions.assertEquals(providerModel.getStatedUrl().get(0).isRegistered(), registered);

    }

    interface Demo {
        String hello(String arg);
    }

    class DemoImpl implements Demo {
        @Override
        public String hello(String arg) {
            return "bc";
        }
    }
}
