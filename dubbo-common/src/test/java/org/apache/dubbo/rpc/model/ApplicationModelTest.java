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

import org.apache.dubbo.common.config.Environment;
import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.ReferenceConfigBase;
import org.apache.dubbo.config.ServiceConfigBase;
import org.apache.dubbo.config.context.ConfigManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;


public class ApplicationModelTest {

    @Test
    public void test() {
        ConfigManager configManager = ApplicationModel.getConfigManager();
        Assertions.assertNotNull(configManager);
        Environment environment = ApplicationModel.getEnvironment();
        Assertions.assertNotNull(environment);
        ServiceRepository serviceRepository = ApplicationModel.getServiceRepository();
        Assertions.assertNotNull(serviceRepository);


        ApplicationConfig applicationConfig = new ApplicationConfig("demo-provider");
        configManager.setApplication(applicationConfig);
        Assertions.assertNotNull(ApplicationModel.getApplicationConfig());
        Assertions.assertEquals(ApplicationModel.getName(), "demo-provider");

        ApplicationModel.initFrameworkExts();

        ApplicationModel.setApplication("appName");
        Assertions.assertEquals(ApplicationModel.getApplication(), "appName");

        Assertions.assertEquals(ApplicationModel.allProviderModels().size(), 0);
        Assertions.assertEquals(ApplicationModel.allConsumerModels().size(), 0);

        ServiceDescriptor serviceDescriptor = serviceRepository.registerService(Demo.class);
        ServiceConfigBase serviceConfigBase = mock(ServiceConfigBase.class);
        ReferenceConfigBase referenceConfigBase = mock(ReferenceConfigBase.class);
        ServiceMetadata serviceMetadata = new ServiceMetadata();
        String serviceKey = Demo.class.getName();
        serviceMetadata.setServiceKey(serviceKey);

        serviceRepository.registerProvider(serviceKey, new DemoImpl(), serviceDescriptor, serviceConfigBase, serviceMetadata);
        serviceRepository.registerConsumer(serviceKey, serviceDescriptor, referenceConfigBase, new DemoImpl(), serviceMetadata);

        Assertions.assertNotNull(ApplicationModel.getConsumerModel(serviceKey));
        Assertions.assertNotNull(ApplicationModel.getProviderModel(serviceKey));

        Assertions.assertEquals(ApplicationModel.allProviderModels().size(), 1);
        Assertions.assertEquals(ApplicationModel.allConsumerModels().size(), 1);

        ApplicationModel.reset();
    }

    interface Demo {
        String hello(String arg);
    }

    class DemoImpl implements ProviderModelTest.Demo {
        @Override
        public String hello(String arg) {
            return "bc";
        }
    }

}
