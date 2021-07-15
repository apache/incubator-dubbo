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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.apache.dubbo.common.constants.CommonConstants.CONSUMER_SIDE;
import static org.apache.dubbo.common.constants.CommonConstants.SIDE_KEY;

public class ServiceMetadataTest {

    @Test
    public void test() {
        ServiceMetadata serviceMetadata = new ServiceMetadata();
        serviceMetadata.setServiceType(Demo.class);
        serviceMetadata.setVersion("1.0.0");
        serviceMetadata.setGroup("GroupA");
        serviceMetadata.setDefaultGroup("GroupA");
        serviceMetadata.setServiceInterfaceName(Demo.class.getName());
        DemoImpl demo = new DemoImpl();
        serviceMetadata.setTarget(demo);
        serviceMetadata.addAttachment(SIDE_KEY, CONSUMER_SIDE);
        serviceMetadata.addAttribute("ORIGIN_CONFIG", this);

        Assertions.assertEquals(serviceMetadata.getServiceType(), Demo.class);
        Assertions.assertEquals(serviceMetadata.getVersion(), "1.0.0");
        Assertions.assertEquals(serviceMetadata.getGroup(), "GroupA");
        Assertions.assertEquals(serviceMetadata.getDefaultGroup(), "GroupA");
        Assertions.assertEquals(serviceMetadata.getServiceInterfaceName(), Demo.class.getName());
        Assertions.assertEquals(serviceMetadata.getTarget(), demo);
        Assertions.assertEquals(serviceMetadata.getAttachments().get(SIDE_KEY), CONSUMER_SIDE);
        Assertions.assertEquals(serviceMetadata.getAttribute("ORIGIN_CONFIG"), this);

    }

    interface Demo {

    }

    class DemoImpl implements Demo {

    }
}
