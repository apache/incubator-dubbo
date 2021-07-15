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

import org.apache.dubbo.common.utils.MethodUtils;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.rpc.service.GenericService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.dubbo.common.constants.CommonConstants.$INVOKE;
import static org.apache.dubbo.common.constants.CommonConstants.GENERIC_PARAMETER_DESC;
import static org.apache.dubbo.common.constants.CommonConstants.$INVOKE_ASYNC;

public class MethodDescriptorTest {

    @Test
    public void test() {
        Method method = MethodUtils.findMethod(Demo.class, "sayHello", List.class, Map.class);
        MethodDescriptor methodDescriptor = new MethodDescriptor(method);
        Assertions.assertEquals(methodDescriptor.getMethod(), method);
        Assertions.assertEquals(methodDescriptor.getParamDesc(), "Ljava/util/List;Ljava/util/Map;");
        Assertions.assertArrayEquals(methodDescriptor.getParameterClasses(), new Class[]{List.class, Map.class});
        Assertions.assertEquals(methodDescriptor.getReturnClass(), CompletableFuture.class);
        Assertions.assertEquals(methodDescriptor.isGeneric(), false);
        Assertions.assertEquals(methodDescriptor.getMethodName(), "sayHello");
        // 0 = {Class@222} "interface java.util.List"
        // 1 = {ParameterizedTypeImpl@2005} "java.util.List<java.lang.String>"
        Assertions.assertArrayEquals(methodDescriptor.getReturnTypes(), ReflectUtils.getReturnTypes(method));
    }

    @Test
    public void testGeneric(){
        // Object $invoke(String method, String[] parameterTypes, Object[] args)
        Method method = MethodUtils.findMethod(GenericService.class, $INVOKE, String.class, String[].class,Object[].class);
        MethodDescriptor methodDescriptor = new MethodDescriptor(method);
        Assertions.assertEquals(methodDescriptor.getMethod(), method);
        Assertions.assertEquals(methodDescriptor.getParamDesc(), GENERIC_PARAMETER_DESC);
        Assertions.assertArrayEquals(methodDescriptor.getParameterClasses(), new Class[]{String.class, String[].class,Object[].class});
        Assertions.assertArrayEquals(methodDescriptor.getReturnTypes(), ReflectUtils.getReturnTypes(method));
        Assertions.assertEquals(methodDescriptor.getReturnClass(), Object.class);
        Assertions.assertEquals(methodDescriptor.isGeneric(), true);
        Assertions.assertEquals(methodDescriptor.getMethodName(), $INVOKE);

        // default CompletableFuture<Object> $invokeAsync(String method, String[] parameterTypes, Object[] args)
        Method method2 = MethodUtils.findMethod(GenericService.class, $INVOKE_ASYNC, String.class, String[].class,Object[].class);
        MethodDescriptor methodDescriptor2 = new MethodDescriptor(method2);
        Assertions.assertEquals(methodDescriptor2.getMethod(), method2);
        Assertions.assertEquals(methodDescriptor2.getParamDesc(), GENERIC_PARAMETER_DESC);
        Assertions.assertArrayEquals(methodDescriptor2.getParameterClasses(), new Class[]{String.class, String[].class,Object[].class});
        Assertions.assertArrayEquals(methodDescriptor2.getReturnTypes(), ReflectUtils.getReturnTypes(method2));
        Assertions.assertEquals(methodDescriptor2.getReturnClass(), CompletableFuture.class);
        Assertions.assertEquals(methodDescriptor2.isGeneric(), true);
        Assertions.assertEquals(methodDescriptor2.getMethodName(), $INVOKE_ASYNC);
    }

    interface Demo {
        CompletableFuture<List<String>> sayHello(List<String> list, Map<String, Integer> map);
    }
}
