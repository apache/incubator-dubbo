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
package org.apache.dubbo.rpc.protocol.dubbo;


import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.ProxyFactory;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.protocol.dubbo.support.DemoService;
import org.apache.dubbo.rpc.protocol.dubbo.support.DemoServiceImpl;
import org.apache.dubbo.rpc.protocol.dubbo.support.NonSerialized;
import org.apache.dubbo.rpc.protocol.dubbo.support.ProtocolUtils;
import org.apache.dubbo.rpc.protocol.dubbo.support.RemoteService;
import org.apache.dubbo.rpc.protocol.dubbo.support.RemoteServiceImpl;
import org.apache.dubbo.rpc.protocol.dubbo.support.Type;
import org.apache.dubbo.rpc.service.EchoService;

import org.apache.dubbo.rpc.service.GenericService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * <code>ProxiesTest</code>
 */

public class DubboProtocolTest {
    private Protocol protocol = ExtensionLoader.getExtensionLoader(Protocol.class).getAdaptiveExtension();
    private ProxyFactory proxy = ExtensionLoader.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();

    @AfterAll
    public static void after() {
        ProtocolUtils.closeAll();
        ApplicationModel.getServiceRepository().unregisterService(DemoService.class);
    }

    @BeforeAll
    public static void setup() {
        ApplicationModel.getServiceRepository().registerService(DemoService.class);
    }

    @Test
    public void testDemoProtocol() throws Exception {
        DemoService service = new DemoServiceImpl();
        int port = NetUtils.getAvailablePort();
        protocol.export(proxy.getInvoker(service, DemoService.class, URL.valueOf("dubbo://127.0.0.1:" + port + "/" + DemoService.class.getName() + "?codec=exchange")));
        service = proxy.getProxy(protocol.refer(DemoService.class, URL.valueOf("dubbo://127.0.0.1:" + port + "/" + DemoService.class.getName() + "?codec=exchange").addParameter("timeout",
                3000L)));
        assertEquals(service.getSize(new String[]{"", "", ""}), 3);
    }

    @Test
    public void testGetDubboProtocol(){
        DemoService service = new DemoServiceImpl();
        int port = NetUtils.getAvailablePort();
        protocol.export(proxy.getInvoker(service, DemoService.class, URL.valueOf("dubbo://127.0.0.1:" + port + "/" + DemoService.class.getName())));
        Assertions.assertTrue(DubboProtocol.getDubboProtocol().getServers().size() > 0);
    }

    @Test
    public void testDubboProtocol() throws Exception {
        DemoServiceImpl serviceImpl = new DemoServiceImpl();
        int port = NetUtils.getAvailablePort();
        protocol.export(proxy.getInvoker(serviceImpl, DemoService.class, URL.valueOf("dubbo://127.0.0.1:" + port + "/" + DemoService.class.getName())));
        DemoService service = proxy.getProxy(protocol.refer(DemoService.class, URL.valueOf("dubbo://127.0.0.1:" + port + "/" + DemoService.class.getName()).addParameter("timeout",
                3000L)));
        assertEquals(service.enumlength(new Type[]{}), Type.Lower);
        assertEquals(service.getSize(null), -1);
        assertEquals(service.getSize(new String[]{"", "", ""}), 3);
        Map<String, String> map = new HashMap<String, String>();
        map.put("aa", "bb");
        Set<String> set = service.keys(map);
        assertEquals(set.size(), 1);
        assertEquals(set.iterator().next(), "aa");
        service.invoke("dubbo://127.0.0.1:" + port + "/" + DemoService.class.getName() + "", "invoke");

        service = proxy.getProxy(protocol.refer(DemoService.class, URL.valueOf("dubbo://127.0.0.1:" + port + "/" + DemoService.class.getName() + "?client=netty").addParameter("timeout",
                3000L)));
        // test netty client
        StringBuilder buf = new StringBuilder();
        for (int i = 0; i < 1024 * 32 + 32; i++)
            buf.append('A');
        System.out.println(service.stringLength(buf.toString()));

        // cast to EchoService
        EchoService echo = proxy.getProxy(protocol.refer(EchoService.class, URL.valueOf("dubbo://127.0.0.1:" + port + "/" + DemoService.class.getName() + "?client=netty").addParameter("timeout",
                3000L)));
        assertEquals(echo.$echo(buf.toString()), buf.toString());
        assertEquals(echo.$echo("test"), "test");
        assertEquals(echo.$echo("abcdefg"), "abcdefg");
        assertEquals(echo.$echo(1234), 1234);
    }

    @Disabled("Mina has been moved to a separate project")
    @Test
    public void testAsyncCall() throws Exception {
        DemoServiceImpl serviceImpl = new DemoServiceImpl();
        int port = NetUtils.getAvailablePort();
        protocol.export(proxy.getInvoker(serviceImpl, DemoService.class, URL.valueOf("dubbo://127.0.0.1:" + port + "/" + DemoService.class.getName())));
        DemoService service = proxy.getProxy(protocol.refer(DemoService.class, URL.valueOf("dubbo://127.0.0.1:" + port + "/" + DemoService.class.getName()).addParameter("timeout",
                3000L)));

        String name = "haha";
        String expected = "Hello, haha";

        // test async
        serviceImpl.resetCalled();
        CompletableFuture<String> future = service.sayHelloAsync(name);
        assertFalse(serviceImpl.isCalled());
        assertEquals(expected, future.get());
        assertTrue(serviceImpl.isCalled());
    }

        @Test
    public void testGenericInvoke() throws Exception {
        DemoServiceImpl serviceImpl = new DemoServiceImpl();
        int port = NetUtils.getAvailablePort();
        protocol.export(proxy.getInvoker(serviceImpl, DemoService.class, URL.valueOf("dubbo://127.0.0.1:" + port + "/" + DemoService.class.getName())));
        DemoService service = proxy.getProxy(protocol.refer(DemoService.class, URL.valueOf("dubbo://127.0.0.1:" + port + "/" + DemoService.class.getName()).addParameter("timeout",
                3000L)));

        String name = "haha";
        String expected = "Hello, haha";

        // test GenericService.$invoke
        GenericService genericService = proxy.getProxy(protocol.refer(GenericService.class, URL.valueOf("dubbo://127.0.0.1:" + port + "/" + DemoService.class.getName()).addParameter("timeout",
                3000L)));
        Object echoResult = genericService.$invoke("sayHello", new String[]{"java.lang.String"}, new Object[]{name});
        assertEquals(expected, echoResult);

        serviceImpl.resetCalled();
        echoResult = genericService.$invoke("sayHelloAsync", new String[]{"java.lang.String"}, new Object[]{name});
        assertTrue(serviceImpl.isCalled());
        assertEquals(expected, echoResult);

        // test GenericService.$invokeAsync
        CompletableFuture<Object> echoFeature = genericService.$invokeAsync("sayHello", new String[]{"java.lang.String"}, new Object[]{name});
        assertEquals(expected, echoFeature.get());

        serviceImpl.resetCalled();
        echoFeature = genericService.$invokeAsync("sayHelloAsync", new String[]{"java.lang.String"}, new Object[]{name});
        assertFalse(serviceImpl.isCalled());
        assertEquals(expected, echoFeature.get());
        assertTrue(serviceImpl.isCalled());

    }

    @Test
    public void testDubboProtocolWithMina() throws Exception {
        DemoService service = new DemoServiceImpl();
        int port = NetUtils.getAvailablePort();
        protocol.export(proxy.getInvoker(service, DemoService.class, URL.valueOf("dubbo://127.0.0.1:" + port + "/" + DemoService.class.getName()).addParameter(Constants.SERVER_KEY, "mina")));
        service = proxy.getProxy(protocol.refer(DemoService.class, URL.valueOf("dubbo://127.0.0.1:" + port + "/" + DemoService.class.getName()).addParameter(Constants.CLIENT_KEY, "mina").addParameter("timeout",
                3000L)));
        for (int i = 0; i < 10; i++) {
            assertEquals(service.enumlength(new Type[]{}), Type.Lower);
            assertEquals(service.getSize(null), -1);
            assertEquals(service.getSize(new String[]{"", "", ""}), 3);
        }
        Map<String, String> map = new HashMap<String, String>();
        map.put("aa", "bb");
        for (int i = 0; i < 10; i++) {
            Set<String> set = service.keys(map);
            assertEquals(set.size(), 1);
            assertEquals(set.iterator().next(), "aa");
            service.invoke("dubbo://127.0.0.1:" + port + "/" + DemoService.class.getName() + "", "invoke");
        }

        service = proxy.getProxy(protocol.refer(DemoService.class, URL.valueOf("dubbo://127.0.0.1:" + port + "/" + DemoService.class.getName() + "?client=mina").addParameter("timeout",
                3000L)));
        // test netty client
        StringBuilder buf = new StringBuilder();
        for (int i = 0; i < 1024 * 32 + 32; i++)
            buf.append('A');
        System.out.println(service.stringLength(buf.toString()));

        // cast to EchoService
        EchoService echo = proxy.getProxy(protocol.refer(EchoService.class, URL.valueOf("dubbo://127.0.0.1:" + port + "/" + DemoService.class.getName() + "?client=mina").addParameter("timeout",
                3000L)));
        for (int i = 0; i < 10; i++) {
            assertEquals(echo.$echo(buf.toString()), buf.toString());
            assertEquals(echo.$echo("test"), "test");
            assertEquals(echo.$echo("abcdefg"), "abcdefg");
            assertEquals(echo.$echo(1234), 1234);
        }
    }

    @Test
    public void testDubboProtocolMultiService() throws Exception {
//        DemoService service = new DemoServiceImpl();
//        protocol.export(proxy.getInvoker(service, DemoService.class, URL.valueOf("dubbo://127.0.0.1:9010/" + DemoService.class.getName())));
//        service = proxy.getProxy(protocol.refer(DemoService.class, URL.valueOf("dubbo://127.0.0.1:9010/" + DemoService.class.getName()).addParameter("timeout",
//                3000L)));

        RemoteService remote = new RemoteServiceImpl();

        ApplicationModel.getServiceRepository().registerService(RemoteService.class);

        int port = NetUtils.getAvailablePort();
        protocol.export(proxy.getInvoker(remote, RemoteService.class, URL.valueOf("dubbo://127.0.0.1:" + port + "/" + RemoteService.class.getName())));
        remote = proxy.getProxy(protocol.refer(RemoteService.class, URL.valueOf("dubbo://127.0.0.1:" + port + "/" + RemoteService.class.getName()).addParameter("timeout",
                3000L)));

//        service.sayHello("world");

        // test netty client
//        assertEquals("world", service.echo("world"));
        assertEquals("hello world@" + RemoteServiceImpl.class.getName(), remote.sayHello("world"));

//       can't find target service addresses
        EchoService remoteEecho = (EchoService) remote;
        assertEquals(remoteEecho.$echo("ok"), "ok");
    }

    @Test
    public void testPerm() throws Exception {
        DemoService service = new DemoServiceImpl();
        int port = NetUtils.getAvailablePort();
        protocol.export(proxy.getInvoker(service, DemoService.class, URL.valueOf("dubbo://127.0.0.1:" + port + "/" + DemoService.class.getName() + "?codec=exchange")));
        service = proxy.getProxy(protocol.refer(DemoService.class, URL.valueOf("dubbo://127.0.0.1:" + port + "/" + DemoService.class.getName() + "?codec=exchange").addParameter("timeout",
                3000L)));
        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++)
            service.getSize(new String[]{"", "", ""});
        System.out.println("take:" + (System.currentTimeMillis() - start));
    }

    @Test
    public void testNonSerializedParameter() throws Exception {
        DemoService service = new DemoServiceImpl();
        int port = NetUtils.getAvailablePort();
        protocol.export(proxy.getInvoker(service, DemoService.class, URL.valueOf("dubbo://127.0.0.1:" + port + "/" + DemoService.class.getName() + "?codec=exchange")));
        service = proxy.getProxy(protocol.refer(DemoService.class, URL.valueOf("dubbo://127.0.0.1:" + port + "/" + DemoService.class.getName() + "?codec=exchange").addParameter("timeout",
                3000L)));
        try {
            service.nonSerializedParameter(new NonSerialized());
            Assertions.fail();
        } catch (RpcException e) {
            assertTrue(e.getMessage().contains("org.apache.dubbo.rpc.protocol.dubbo.support.NonSerialized must implement java.io.Serializable"));
        }
    }

    @Test
    public void testReturnNonSerialized() throws Exception {
        DemoService service = new DemoServiceImpl();
        int port = NetUtils.getAvailablePort();
        protocol.export(proxy.getInvoker(service, DemoService.class, URL.valueOf("dubbo://127.0.0.1:" + port + "/" + DemoService.class.getName() + "?codec=exchange")));
        service = proxy.getProxy(protocol.refer(DemoService.class, URL.valueOf("dubbo://127.0.0.1:" + port + "/" + DemoService.class.getName() + "?codec=exchange").addParameter("timeout",
                3000L)));
        try {
            service.returnNonSerialized();
            Assertions.fail();
        } catch (RpcException e) {
            e.printStackTrace();
            Assertions.assertTrue(e.getMessage().contains("org.apache.dubbo.rpc.protocol.dubbo.support.NonSerialized must implement java.io.Serializable"));
        }
    }

    @Test
    public void testRemoteApplicationName() throws Exception {
        DemoService service = new DemoServiceImpl();
        int port = NetUtils.getAvailablePort();
        URL url = URL.valueOf("dubbo://127.0.0.1:" + port + "/" + DemoService.class.getName() + "?codec=exchange").addParameter("timeout",
                3000L).addParameter("application", "consumer");
        protocol.export(proxy.getInvoker(service, DemoService.class, url));
        service = proxy.getProxy(protocol.refer(DemoService.class, url));
        assertEquals(service.getRemoteApplicationName(), "consumer");
    }

    @Test
    public void testPayloadOverException() throws Exception {
        DemoService service = new DemoServiceImpl();
        int port = NetUtils.getAvailablePort();
        protocol.export(proxy.getInvoker(service, DemoService.class,
                URL.valueOf("dubbo://127.0.0.1:" + port + "/" + DemoService.class.getName()).addParameter("payload", 10 * 1024)));
        service = proxy.getProxy(protocol.refer(DemoService.class,
                URL.valueOf("dubbo://127.0.0.1:" + port + "/" + DemoService.class.getName()).addParameter("timeout",
                        6000L).addParameter("payload", 160)));
        try {
            service.download(300);
            Assertions.fail();
        } catch (Exception expected) {
            Assertions.assertTrue(expected.getMessage().contains("Data length too large"));
        }

    }
}
