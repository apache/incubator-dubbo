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

import org.apache.dubbo.config.ReferenceConfigBase;
import org.apache.dubbo.config.ServiceConfigBase;
import org.apache.dubbo.rpc.model.support.MockBuiltinService;
import org.apache.dubbo.rpc.service.EchoService;
import org.apache.dubbo.rpc.service.GenericService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;

public class ServiceRepositoryTest {

    @Test
    public void test() {
        // test auto registered BuiltinServiceDetector
        ServiceRepository serviceRepository = new ServiceRepository();
        Assertions.assertEquals(serviceRepository.getAllServices().size(), 3);
        Assertions.assertNotNull(serviceRepository.lookupService(GenericService.class.getName()));
        Assertions.assertNotNull(serviceRepository.lookupService(EchoService.class.getName()));
        Assertions.assertNotNull(serviceRepository.lookupService(MockBuiltinService.class.getName()));

        // test registerService
        ServiceDescriptor serviceDescriptor = serviceRepository.registerService(Demo.class);
        ServiceDescriptor serviceDescriptor2 = serviceRepository.registerService("demo", Demo.class);
        Assertions.assertEquals(serviceDescriptor, serviceDescriptor2);

        // test lookupService
        Assertions.assertNotNull(serviceRepository.lookupService("demo"));
        Assertions.assertNotNull(serviceRepository.lookupService(Demo.class.getName()));
        Assertions.assertEquals(serviceRepository.getAllServices().size(), 5);

        // test unregisterService
        serviceRepository.unregisterService("demo");
        serviceRepository.unregisterService(Demo.class);
        Assertions.assertNull(serviceRepository.lookupService("demo"));
        Assertions.assertNull(serviceRepository.lookupService(Demo.class.getName()));
        Assertions.assertEquals(serviceRepository.getAllServices().size(), 3);


        // mock data
        ServiceDescriptor descriptor = serviceRepository.registerService(Demo.class);
        ServiceConfigBase serviceConfigBase = mock(ServiceConfigBase.class);
        ReferenceConfigBase referenceConfigBase = mock(ReferenceConfigBase.class);
        ServiceMetadata serviceMetadata = new ServiceMetadata();
        String serviceKey = "GroupA/" + Demo.class.getName() + ":1.0.0";
        serviceMetadata.setServiceKey(serviceKey);

        // test registerProvider 、registerConsumer
        serviceRepository.registerProvider(serviceKey, new DemoImpl(), descriptor, serviceConfigBase, serviceMetadata);
        serviceRepository.registerConsumer(serviceKey, descriptor, referenceConfigBase, new DemoImpl(), serviceMetadata);

        // test ProviderModel
        Assertions.assertEquals(serviceRepository.getExportedServices().size(), 1);
        ProviderModel providerModel = serviceRepository.lookupExportedService(serviceKey);
        Assertions.assertNotNull(providerModel);
        Assertions.assertEquals(providerModel.getServiceKey(), serviceKey);

        // test withoutGroupProviderModel
        String serviceKeyWithoutGroup = Demo.class.getName() + ":1.0.0";
        ProviderModel withoutGroupProviderModel = serviceRepository.lookupExportedServiceWithoutGroup(serviceKeyWithoutGroup);
        Assertions.assertEquals(withoutGroupProviderModel, providerModel);

        // test ConsumerModel
        Assertions.assertEquals(serviceRepository.getReferredServices().size(),1);
        ConsumerModel consumerModel = serviceRepository.lookupReferredService(serviceKey);
        Assertions.assertNotNull(consumerModel);
        Assertions.assertEquals(consumerModel.getServiceKey(), serviceKey);

        // destroy
        serviceRepository.destroy();
    }


    interface Demo {

    }

    class DemoImpl implements ServiceMetadataTest.Demo {

    }
}
