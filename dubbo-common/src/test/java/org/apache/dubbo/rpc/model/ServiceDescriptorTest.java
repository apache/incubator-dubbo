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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class ServiceDescriptorTest {

    @Test
    public void test() {
        ServiceDescriptor serviceDescriptor = new ServiceDescriptor(Demo.class);

        Assertions.assertEquals(serviceDescriptor.getServiceName(), Demo.class.getName());
        Assertions.assertEquals(serviceDescriptor.getServiceInterfaceClass(), Demo.class);

        Assertions.assertEquals(serviceDescriptor.getAllMethods().size(), 3);

        Assertions.assertEquals(serviceDescriptor.getMethods("methodA").size(), 1);
        Assertions.assertEquals(serviceDescriptor.getMethods("methodB").size(), 2);

        Assertions.assertNotNull(serviceDescriptor.getMethod("methodA", "Ljava/util/List;Ljava/util/Map;"));
        Assertions.assertNotNull(serviceDescriptor.getMethod("methodB", "Ljava/lang/String;"));
        Assertions.assertNotNull(serviceDescriptor.getMethod("methodB", "Ljava/lang/Integer;"));

        Assertions.assertNotNull(serviceDescriptor.getMethod("methodA", new Class[]{List.class, Map.class}));
        Assertions.assertNotNull(serviceDescriptor.getMethod("methodB", new Class[]{String.class}));
        Assertions.assertNotNull(serviceDescriptor.getMethod("methodB", new Class[]{Integer.class}));
    }

    interface Demo {

        CompletableFuture<List<String>> methodA(List<String> list, Map<String, Integer> map);

        String methodB(String arg);

        String methodB(Integer arg);
    }
}
