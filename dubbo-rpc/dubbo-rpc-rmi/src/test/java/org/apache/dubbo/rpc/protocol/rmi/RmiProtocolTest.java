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
package org.apache.dubbo.rpc.protocol.rmi;


import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.ProxyFactory;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.service.EchoService;
import org.apache.dubbo.rpc.service.GenericService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Disabled(value = "Disable for Github Actions Environment Issue")
public class RmiProtocolTest {
    private Protocol protocol = ExtensionLoader.getExtensionLoader(Protocol.class).getAdaptiveExtension();
    private ProxyFactory proxy = ExtensionLoader.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();

    /*
    @Test
    public void test_getRemoteClass() throws Exception {
        Class<NonStdRmiInterface> clazz = RmiProtocol.getRemoteClass(NonStdRmiInterface.class);
        assertEquals(clazz, RmiProtocol.getRemoteClass(NonStdRmiInterface.class));
    }
    */
    @Test
    public void testRmiProtocolTimeout() throws Exception {
        int availablePort = NetUtils.getAvailablePort();
        System.setProperty("sun.rmi.transport.tcp.responseTimeout", "1000");
        DemoService service = new DemoServiceImpl();
        Exporter<?> rpcExporter = protocol.export(proxy.getInvoker(service, DemoService.class, URL.valueOf("rmi://127.0.0.1:" + availablePort + "/TestService")));
        service = proxy.getProxy(protocol.refer(DemoService.class, URL.valueOf("rmi://127.0.0.1:" + availablePort + "/TestService")));
        try {
            try {
                service.throwTimeout();
            } catch (RpcException e) {
                assertTrue(e.isTimeout());
                assertTrue(e.getMessage().contains("Read timed out"));
            }
        } finally {
            rpcExporter.unexport();
        }
    }

    @Test
    public void testRmiProtocol() throws Exception {
        {
            int availablePort = NetUtils.getAvailablePort();
            DemoService service = new DemoServiceImpl();
            Exporter<?> rpcExporter = protocol.export(proxy.getInvoker(service, DemoService.class, URL.valueOf("rmi://127.0.0.1:" + availablePort + "/TestService")));

            service = proxy.getProxy(protocol.refer(DemoService.class, URL.valueOf("rmi://127.0.0.1:" + availablePort + "/TestService")));
            assertEquals(service.getSize(null), -1);
            assertEquals(service.getSize(new String[]{"", "", ""}), 3);
            Object result = service.invoke("rmi://127.0.0.1:" + availablePort + "/TestService", "invoke");
            assertEquals("rmi://127.0.0.1:" + availablePort + "/TestService:invoke", result);

            rpcExporter.unexport();
        }

        {
            int port = NetUtils.getAvailablePort();
            RemoteService remoteService = new RemoteServiceImpl();
            Exporter<?> rpcExporter = protocol.export(proxy.getInvoker(remoteService, RemoteService.class, URL.valueOf("rmi://127.0.0.1:" + port + "/remoteService")));

            remoteService = proxy.getProxy(protocol.refer(RemoteService.class, URL.valueOf("rmi://127.0.0.1:" + port + "/remoteService")));
            remoteService.getThreadName();
            for (int i = 0; i < 100; i++) {
                String say = remoteService.sayHello("abcd");
                assertEquals("hello abcd@" + RemoteServiceImpl.class.getName(), say);
            }
            rpcExporter.unexport();
        }
    }

    // FIXME RMI protocol doesn't support casting to EchoService yet.
    @Disabled
    @Test
    public void testRmiProtocol_echoService() throws Exception {
        int availablePort = NetUtils.getAvailablePort();
        DemoService service = new DemoServiceImpl();
        Exporter<?> rpcExporter = protocol.export(proxy.getInvoker(service, DemoService.class, URL.valueOf("rmi://127.0.0.1:" + availablePort + "/TestService")));

        // cast to EchoService
        EchoService echo = proxy.getProxy(protocol.refer(EchoService.class, URL.valueOf("rmi://127.0.0.1:" + availablePort + "/TestService")));
        assertEquals(echo.$echo("test"), "test");
        assertEquals(echo.$echo("abcdefg"), "abcdefg");
        assertEquals(echo.$echo(1234), 1234);

        rpcExporter.unexport();

        availablePort = NetUtils.getAvailablePort();
        RemoteService remoteService = new RemoteServiceImpl();
        rpcExporter = protocol.export(proxy.getInvoker(remoteService, RemoteService.class, URL.valueOf("rmi://127.0.0.1:" + availablePort + "/remoteService")));

        // cast to EchoService
        echo = proxy.getProxy(protocol.refer(EchoService.class, URL.valueOf("rmi://127.0.0.1:" + availablePort + "/remoteService")));
        assertEquals(echo.$echo("test"), "test");
        assertEquals(echo.$echo("abcdefg"), "abcdefg");
        assertEquals(echo.$echo(1234), 1234);

        rpcExporter.unexport();
    }

    @Test
    public void testGenericInvoke() {
        int availablePort = NetUtils.getAvailablePort();
        DemoService service = new DemoServiceImpl();
        URL url = URL.valueOf("rmi://127.0.0.1:" + availablePort + "/" + DemoService.class.getName() + "?release=2.7.0");
        Exporter<DemoService> exporter = protocol.export(proxy.getInvoker(service, DemoService.class, url));
        Invoker<GenericService> invoker = protocol.refer(GenericService.class, url);
        GenericService client = proxy.getProxy(invoker, true);
        String result = (String) client.$invoke("sayHi", new String[]{"java.lang.String"}, new Object[]{"haha"});
        Assertions.assertEquals("Hi, haha", result);
        invoker.destroy();
        exporter.unexport();
    }

    @Test
    public void testGenericAsyncInvoke() throws ExecutionException, InterruptedException {
        DemoService service = new DemoServiceImpl();
        URL url = URL.valueOf("rmi://127.0.0.1:" + availablePort + "/" + DemoService.class.getName() + "?release=2.7.0");
        Exporter<DemoService> exporter = protocol.export(proxy.getInvoker(service, DemoService.class, url));
        Invoker<GenericService> invoker = protocol.refer(GenericService.class, url);
        GenericService client = proxy.getProxy(invoker, true);
        CompletableFuture<Object> future = client.$invokeAsync("sayHi", new String[]{"java.lang.String"}, new Object[]{"haha"});
        Assertions.assertEquals("Hi, haha", future.get());
        invoker.destroy();
        exporter.unexport();
    }


    @Test
    public void testRemoteApplicationName() throws Exception {
        int availablePort = NetUtils.getAvailablePort();
        DemoService service = new DemoServiceImpl();
        URL url = URL.valueOf("rmi://127.0.0.1:" + availablePort + "/TestService?release=2.7.0").addParameter("application", "consumer");
        Exporter<?> rpcExporter = protocol.export(proxy.getInvoker(service, DemoService.class, url));

        service = proxy.getProxy(protocol.refer(DemoService.class, url));
        assertEquals(service.getRemoteApplicationName(), "consumer");

        rpcExporter.unexport();

    }

    public interface NonStdRmiInterface {
        void bark();
    }

	/*@Test
    public void testRpcInvokerGroup() throws Exception
	{
		DemoService service = new DemoServiceImpl();
		RpcUtils.export("demo://127.0.0.1:9030/org.apache.dubbo.rpc.TestService",DemoService.class,service);
		RpcUtils.export("dubbo://127.0.0.1:9031/TestService",DemoService.class,service);
		RpcUtils.export("rmi://127.0.0.1:9032/org.apache.dubbo.rpc.TestService",DemoService.class,service);
		RpcUtils.export("rmi://127.0.0.1:9033/org.apache.dubbo.rpc.TestService",DemoService.class,service);

		service = RpcUtils.createProxy(DemoService.class,
				new String[]{
					"demo://127.0.0.1:9030/org.apache.dubbo.rpc.TestService?weight=20",
					"dubbo://127.0.0.1:9031/TestService?weight=20",
					"rmi://127.0.0.1:9032/org.apache.dubbo.rpc.TestService",
				});
		assertEquals(service.getSize(null), -1);
		assertEquals(service.getSize(new String[]{"","",""}), 3);

		// cast to EchoService
		EchoService echo = RpcUtils.createProxy(EchoService.class,
				new String[]{
			"demo://127.0.0.1:9030/org.apache.dubbo.rpc.TestService?weight=20",
			"dubbo://127.0.0.1:9031/TestService?weight=20",
			"rmi://127.0.0.1:9032/org.apache.dubbo.rpc.TestService",
		});
		assertEquals(echo.$echo("test"), "test");
		assertEquals(echo.$echo("abcdefg"), "abcdefg");
		assertEquals(echo.$echo(1234), 1234);
	}*/

	/*public void testForkInvoke() throws Exception
	{
		DemoService service = new DemoServiceImpl();
		protocol.export(proxy.createInvoker("dubbo://127.0.0.1:9040/TestService", DemoService.class, service);
		protocol.export(proxy.createInvoker("dubbo://127.0.0.1:9041/TestService", DemoService.class, service);
		protocol.export(proxy.createInvoker("rmi://127.0.0.1:9042/org.apache.dubbo.rpc.TestService", DemoService.class, service);
		protocol.export(proxy.createInvoker("rmi://127.0.0.1:9043/org.apache.dubbo.rpc.TestService", DemoService.class, service);

		RpcInvokerGroup group = Proxies.createInvoker(DemoService.class, new String[]{
			"dubbo://127.0.0.1:9040/TestService",
			"dubbo://127.0.0.1:9041/TestService",
			"rmi://127.0.0.1:9042/org.apache.dubbo.rpc.TestService",
			"rmi://127.0.0.1:9043/org.apache.dubbo.rpc.TestService",
		});
		group.getMethodSettings("echo").setFork(true);
		group.getMethodSettings("echo").setForkInvokeCallback(new ForkInvokeCallback(){
			public Object merge(RpcInvocation invocation, RpcResult[] results) throws Throwable
			{
				System.out.println("merge result begin:");
				for( RpcResult result : results )
				{
					if( result.hasException() )
						System.out.println("exception:"+result.getException().getMessage());
					else
						System.out.println("result:"+result.getResult());
				}
				System.out.println("merge result end:");
				return "aaaa";
			}
		});

		service = proxy.createProxy(protocol.refer(DemoService.class, group);
		service.echo("test");

		// cast to EchoService
		EchoService echo = proxy.createProxy(protocol.refer(EchoService.class, group);
		assertEquals(echo.$echo("test"), "test");
		assertEquals(echo.$echo("abcdefg"), "abcdefg");
		assertEquals(echo.$echo(1234), 1234);
	}*/

}