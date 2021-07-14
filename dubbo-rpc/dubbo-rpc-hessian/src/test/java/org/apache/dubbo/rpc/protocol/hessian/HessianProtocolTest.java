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
package org.apache.dubbo.rpc.protocol.hessian;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.beanutil.JavaBeanDescriptor;
import org.apache.dubbo.common.beanutil.JavaBeanSerializeUtil;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.serialize.ObjectInput;
import org.apache.dubbo.common.serialize.ObjectOutput;
import org.apache.dubbo.common.serialize.Serialization;
import org.apache.dubbo.common.serialize.nativejava.NativeJavaSerialization;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.ProxyFactory;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.protocol.hessian.HessianServiceImpl.MyException;
import org.apache.dubbo.rpc.service.GenericService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * HessianProtocolTest
 */
public class HessianProtocolTest {

    @Test
    public void testHessianProtocol() {
        HessianServiceImpl server = new HessianServiceImpl();
        Assertions.assertFalse(server.isCalled());
        ProxyFactory proxyFactory = ExtensionLoader.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();
        Protocol protocol = ExtensionLoader.getExtensionLoader(Protocol.class).getAdaptiveExtension();
        int port = NetUtils.getAvailablePort();
        URL url = URL.valueOf("hessian://127.0.0.1:" + port + "/" + HessianService.class.getName() + "?version=1.0.0&hessian.overload.method=true");
        Exporter<HessianService> exporter = protocol.export(proxyFactory.getInvoker(server, HessianService.class, url));
        Invoker<HessianService> invoker = protocol.refer(HessianService.class, url);
        HessianService client = proxyFactory.getProxy(invoker);
        String result = client.sayHello("haha");
        Assertions.assertTrue(server.isCalled());
        Assertions.assertEquals("Hello, haha", result);
        invoker.destroy();
        exporter.unexport();
    }

    @Test
    public void testGenericInvoke() {
        HessianServiceImpl server = new HessianServiceImpl();
        Assertions.assertFalse(server.isCalled());
        ProxyFactory proxyFactory = ExtensionLoader.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();
        Protocol protocol = ExtensionLoader.getExtensionLoader(Protocol.class).getAdaptiveExtension();
        int port = NetUtils.getAvailablePort();
        URL url = URL.valueOf("hessian://127.0.0.1:" + port + "/" + HessianService.class.getName() + "?version=1.0.0");
        Exporter<HessianService> exporter = protocol.export(proxyFactory.getInvoker(server, HessianService.class, url));
        Invoker<GenericService> invoker = protocol.refer(GenericService.class, url);
        GenericService client = proxyFactory.getProxy(invoker, true);
        String result = (String) client.$invoke("sayHello", new String[]{"java.lang.String"}, new Object[]{"haha"});
        Assertions.assertTrue(server.isCalled());
        Assertions.assertEquals("Hello, haha", result);
        invoker.destroy();
        exporter.unexport();
    }

    @Test
    public void testGenericAsyncInvoke() throws ExecutionException, InterruptedException {
        HessianServiceImpl server = new HessianServiceImpl();
        Assertions.assertFalse(server.isCalled());
        ProxyFactory proxyFactory = ExtensionLoader.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();
        Protocol protocol = ExtensionLoader.getExtensionLoader(Protocol.class).getAdaptiveExtension();
        int port = NetUtils.getAvailablePort();
        URL url = URL.valueOf("hessian://127.0.0.1:" + port + "/" + HessianService.class.getName() + "?version=1.0.0");
        Exporter<HessianService> exporter = protocol.export(proxyFactory.getInvoker(server, HessianService.class, url));
        Invoker<GenericService> invoker = protocol.refer(GenericService.class, url);
        GenericService client = proxyFactory.getProxy(invoker, true);
        CompletableFuture future = client.$invokeAsync("sayHello", new String[]{"java.lang.String"}, new Object[]{"haha"});
        Assertions.assertFalse(server.isCalled());
        Assertions.assertEquals("Hello, haha", future.get());
        Assertions.assertTrue(server.isCalled());
        invoker.destroy();
        exporter.unexport();
    }

    @Test
    public void testGenericInvokeWithNativeJava() throws IOException, ClassNotFoundException {
        // temporary enable native java generic serialize
        System.setProperty(CommonConstants.ENABLE_NATIVE_JAVA_GENERIC_SERIALIZE, "true");
        HessianServiceImpl server = new HessianServiceImpl();
        Assertions.assertFalse(server.isCalled());
        ProxyFactory proxyFactory = ExtensionLoader.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();
        Protocol protocol = ExtensionLoader.getExtensionLoader(Protocol.class).getAdaptiveExtension();
        int port = NetUtils.getAvailablePort();
        URL url = URL.valueOf("hessian://127.0.0.1:" + port + "/" + HessianService.class.getName() + "?version=1.0.0&generic=nativejava");
        Exporter<HessianService> exporter = protocol.export(proxyFactory.getInvoker(server, HessianService.class, url));
        Invoker<GenericService> invoker = protocol.refer(GenericService.class, url);
        GenericService client = proxyFactory.getProxy(invoker);

        Serialization serialization = new NativeJavaSerialization();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        ObjectOutput objectOutput = serialization.serialize(url, byteArrayOutputStream);
        objectOutput.writeObject("haha");
        objectOutput.flushBuffer();

        Object result = client.$invoke("sayHello", new String[]{"java.lang.String"}, new Object[]{byteArrayOutputStream.toByteArray()});
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream((byte[]) result);
        ObjectInput objectInput = serialization.deserialize(url, byteArrayInputStream);
        Assertions.assertTrue(server.isCalled());
        Assertions.assertEquals("Hello, haha", objectInput.readObject());
        invoker.destroy();
        exporter.unexport();
        System.clearProperty(CommonConstants.ENABLE_NATIVE_JAVA_GENERIC_SERIALIZE);
    }

    @Test
    public void testGenericInvokeWithRpcContext() {
        RpcContext.getContext().setAttachment("myContext", "123");

        HessianServiceImpl server = new HessianServiceImpl();
        ProxyFactory proxyFactory = ExtensionLoader.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();
        Protocol protocol = ExtensionLoader.getExtensionLoader(Protocol.class).getAdaptiveExtension();
        int port = NetUtils.getAvailablePort();
        URL url = URL.valueOf("hessian://127.0.0.1:" + port + "/" + HessianService.class.getName() + "?version=1.0.0");
        Exporter<HessianService> exporter = protocol.export(proxyFactory.getInvoker(server, HessianService.class, url));
        Invoker<GenericService> invoker = protocol.refer(GenericService.class, url);
        GenericService client = proxyFactory.getProxy(invoker, true);
        String result = (String) client.$invoke("context", new String[]{"java.lang.String"}, new Object[]{"haha"});
        Assertions.assertEquals("Hello, haha context, 123", result);
        invoker.destroy();
        exporter.unexport();
    }

    @Test
    public void testGenericInvokeWithBean() {
        HessianServiceImpl server = new HessianServiceImpl();
        Assertions.assertFalse(server.isCalled());
        ProxyFactory proxyFactory = ExtensionLoader.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();
        Protocol protocol = ExtensionLoader.getExtensionLoader(Protocol.class).getAdaptiveExtension();
        int port = NetUtils.getAvailablePort();
        URL url = URL.valueOf("hessian://127.0.0.1:" + port + "/" + HessianService.class.getName() + "?version=1.0.0&generic=bean");
        Exporter<HessianService> exporter = protocol.export(proxyFactory.getInvoker(server, HessianService.class, url));
        Invoker<GenericService> invoker = protocol.refer(GenericService.class, url);
        GenericService client = proxyFactory.getProxy(invoker);

        JavaBeanDescriptor javaBeanDescriptor = JavaBeanSerializeUtil.serialize("haha");

        Object result = client.$invoke("sayHello", new String[]{"java.lang.String"}, new Object[]{javaBeanDescriptor});
        Assertions.assertTrue(server.isCalled());
        Assertions.assertEquals("Hello, haha", JavaBeanSerializeUtil.deserialize((JavaBeanDescriptor) result));
        invoker.destroy();
        exporter.unexport();
    }

    @Test
    public void testOverload() {
        HessianServiceImpl server = new HessianServiceImpl();
        Assertions.assertFalse(server.isCalled());
        ProxyFactory proxyFactory = ExtensionLoader.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();
        Protocol protocol = ExtensionLoader.getExtensionLoader(Protocol.class).getAdaptiveExtension();
        int port = NetUtils.getAvailablePort();
        URL url = URL.valueOf("hessian://127.0.0.1:" + port + "/" + HessianService.class.getName() + "?version=1.0.0&hessian.overload.method=true&hessian2.request=false");
        Exporter<HessianService> exporter = protocol.export(proxyFactory.getInvoker(server, HessianService.class, url));
        Invoker<HessianService> invoker = protocol.refer(HessianService.class, url);
        HessianService client = proxyFactory.getProxy(invoker);
        String result = client.sayHello("haha");
        Assertions.assertEquals("Hello, haha", result);
        result = client.sayHello("haha", 1);
        Assertions.assertEquals("Hello, haha. ", result);
        invoker.destroy();
        exporter.unexport();
    }

    @Test
    public void testHttpClient() {
        HessianServiceImpl server = new HessianServiceImpl();
        Assertions.assertFalse(server.isCalled());
        ProxyFactory proxyFactory = ExtensionLoader.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();
        Protocol protocol = ExtensionLoader.getExtensionLoader(Protocol.class).getAdaptiveExtension();
        int port = NetUtils.getAvailablePort();
        URL url = URL.valueOf("hessian://127.0.0.1:" + port + "/" + HessianService.class.getName() + "?version=1.0.0&client=httpclient&hessian.overload.method=true");
        Exporter<HessianService> exporter = protocol.export(proxyFactory.getInvoker(server, HessianService.class, url));
        Invoker<HessianService> invoker = protocol.refer(HessianService.class, url);
        HessianService client = proxyFactory.getProxy(invoker);
        String result = client.sayHello("haha");
        Assertions.assertTrue(server.isCalled());
        Assertions.assertEquals("Hello, haha", result);
        invoker.destroy();
        exporter.unexport();
    }

    @Test
    public void testTimeOut() {
        HessianServiceImpl server = new HessianServiceImpl();
        ProxyFactory proxyFactory = ExtensionLoader.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();
        Protocol protocol = ExtensionLoader.getExtensionLoader(Protocol.class).getAdaptiveExtension();
        int port = NetUtils.getAvailablePort();
        URL url = URL.valueOf("hessian://127.0.0.1:" + port + "/" + HessianService.class.getName() + "?version=1.0.0&timeout=10");
        Exporter<HessianService> exporter = protocol.export(proxyFactory.getInvoker(server, HessianService.class, url));
        Invoker<HessianService> invoker = protocol.refer(HessianService.class, url);
        HessianService client = proxyFactory.getProxy(invoker);
        try {
            client.timeOut(6000);
            fail();
        } catch (RpcException expected) {
            Assertions.assertTrue(expected.isTimeout());
        } finally {
            invoker.destroy();
            exporter.unexport();
        }

    }

    @Test
    public void testCustomException() {
        HessianServiceImpl server = new HessianServiceImpl();
        ProxyFactory proxyFactory = ExtensionLoader.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();
        Protocol protocol = ExtensionLoader.getExtensionLoader(Protocol.class).getAdaptiveExtension();
        int port = NetUtils.getAvailablePort();
        URL url = URL.valueOf("hessian://127.0.0.1:" + port + "/" + HessianService.class.getName() + "?version=1.0.0");
        Exporter<HessianService> exporter = protocol.export(proxyFactory.getInvoker(server, HessianService.class, url));
        Invoker<HessianService> invoker = protocol.refer(HessianService.class, url);
        HessianService client = proxyFactory.getProxy(invoker);
        try {
            client.customException();
            fail();
        } catch (MyException expected) {

        }
        invoker.destroy();
        exporter.unexport();
    }


    @Test
    public void testRemoteApplicationName() {
        HessianServiceImpl server = new HessianServiceImpl();
        ProxyFactory proxyFactory = ExtensionLoader.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();
        Protocol protocol = ExtensionLoader.getExtensionLoader(Protocol.class).getAdaptiveExtension();
        int port = NetUtils.getAvailablePort();
        URL url = URL.valueOf("hessian://127.0.0.1:" + port + "/" + HessianService.class.getName() + "?version=1.0.0&hessian.overload.method=true").addParameter("application", "consumer");
        Exporter<HessianService> exporter = protocol.export(proxyFactory.getInvoker(server, HessianService.class, url));
        Invoker<HessianService> invoker = protocol.refer(HessianService.class, url);
        HessianService client = proxyFactory.getProxy(invoker);
        String result = client.getRemoteApplicationName();
        Assertions.assertEquals("consumer", result);
        invoker.destroy();
        exporter.unexport();
    }

}
