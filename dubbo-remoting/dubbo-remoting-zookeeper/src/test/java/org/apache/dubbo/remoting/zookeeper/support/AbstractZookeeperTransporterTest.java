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
package org.apache.dubbo.remoting.zookeeper.support;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.remoting.zookeeper.ZookeeperClient;
import org.apache.dubbo.remoting.zookeeper.curator.CuratorZookeeperTransporter;

import com.google.common.collect.Lists;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsNull.nullValue;

/**
 * AbstractZookeeperTransporterTest
 */
public class AbstractZookeeperTransporterTest {
    private TestingServer zkServer;
    private ZookeeperClient zookeeperClient;
    private AbstractZookeeperTransporter abstractZookeeperTransporter;
    private int zkServerPort;

    @BeforeEach
    public void setUp() throws Exception {
        zkServerPort = NetUtils.getAvailablePort();
        zkServer = new TestingServer(zkServerPort, true);
        zookeeperClient = new CuratorZookeeperTransporter().connect(URL.valueOf("zookeeper://127.0.0.1:" +
                zkServerPort + "/service"));
        abstractZookeeperTransporter = new CuratorZookeeperTransporter();
    }


    @AfterEach
    public void tearDown() throws Exception {
        zkServer.stop();
    }

    @Test
    public void testZookeeperClient() {
        assertThat(zookeeperClient, not(nullValue()));
        zookeeperClient.close();
    }

    @Test
    public void testGetURLBackupAddress() {
        URL url = URL.valueOf("zookeeper://127.0.0.1:" + zkServerPort + "/org.apache.dubbo.registry.RegistryService?backup=127.0.0.1:" + 9099 + "&application=metadatareport-local-xml-provider2&dubbo=2.0.2&interface=org.apache.dubbo.registry.RegistryService&pid=47418&specVersion=2.7.0-SNAPSHOT&timestamp=1547102428828");
        List<String> stringList = abstractZookeeperTransporter.getURLBackupAddress(url);
        Assertions.assertEquals(stringList.size(), 2);
        Assertions.assertEquals(stringList.get(0), "127.0.0.1:" + zkServerPort);
        Assertions.assertEquals(stringList.get(1), "127.0.0.1:9099");
    }

    @Test
    public void testGetURLBackupAddressNoBack() {
        URL url = URL.valueOf("zookeeper://127.0.0.1:" + zkServerPort + "/org.apache.dubbo.registry.RegistryService?application=metadatareport-local-xml-provider2&dubbo=2.0.2&interface=org.apache.dubbo.registry.RegistryService&pid=47418&specVersion=2.7.0-SNAPSHOT&timestamp=1547102428828");
        List<String> stringList = abstractZookeeperTransporter.getURLBackupAddress(url);
        Assertions.assertEquals(stringList.size(), 1);
        Assertions.assertEquals(stringList.get(0), "127.0.0.1:" + zkServerPort);
    }

    @Test
    public void testGetCachedZookeeperClient() throws Exception {
        int zkServerPort2 = NetUtils.getAvailablePort();
        TestingServer zkServer2 = new TestingServer(zkServerPort2, true);

        int zkServerPort3 = NetUtils.getAvailablePort();
        TestingServer zkServer3 = new TestingServer(zkServerPort3, true);

        URL url = URL.valueOf("zookeeper://127.0.0.1:" + zkServerPort + "/org.apache.dubbo.registry.RegistryService?backup=127.0.0.1:" + zkServerPort3 + ",127.0.0.1:" + zkServerPort2 + "&application=metadatareport-local-xml-provider2&dubbo=2.0.2&interface=org.apache.dubbo.registry.RegistryService&pid=47418&specVersion=2.7.0-SNAPSHOT&timestamp=1547102428828");
        ZookeeperClient newZookeeperClient = abstractZookeeperTransporter.connect(url);
        //just for connected
        newZookeeperClient.getContent("/dubbo/test");
        Assertions.assertEquals(abstractZookeeperTransporter.getZookeeperClientMap().size(), 1);

        List<Integer> ports = Lists.newArrayList(zkServerPort, zkServerPort2, zkServerPort3);
        Collections.sort(ports);

        String cacheAddressStr = "127.0.0.1:" + ports.get(0) + "," + "127.0.0.1:" + ports.get(1) + "," + "127.0.0.1:" + ports.get(2);
        Assertions.assertEquals(abstractZookeeperTransporter.getZookeeperClientMap().get(cacheAddressStr), newZookeeperClient);

        URL url2 = URL.valueOf("zookeeper://127.0.0.1:" + zkServerPort3 + "/org.apache.dubbo.metadata.store.MetadataReport?backup=127.0.0.1:" + zkServerPort + ",127.0.0.1:" + zkServerPort2 + "&address=zookeeper://127.0.0.1:2181&application=metadatareport-local-xml-provider2&cycle-report=false&interface=org.apache.dubbo.metadata.store.MetadataReport&retry-period=4590&retry-times=23&sync-report=true");
        checkGetCacheConnectedClientNotNull(url2);

        URL url3 = URL.valueOf("zookeeper://127.0.0.1:" + zkServerPort2 + "/org.apache.dubbo.metadata.store.MetadataReport?backup=127.0.0.1:" + zkServerPort + ",127.0.0.1:" + zkServerPort3 + "&address=zookeeper://127.0.0.1:2181&application=metadatareport-local-xml-provider2&cycle-report=false&interface=org.apache.dubbo.metadata.store.MetadataReport&retry-period=4590&retry-times=23&sync-report=true");
        checkGetCacheConnectedClientNotNull(url3);

        zkServer2.stop();
        zkServer3.stop();
    }

    private void checkGetCacheConnectedClientNotNull(URL url) {
        String addressList = abstractZookeeperTransporter.getURLBackupAddressStr(url);
        ZookeeperClient zookeeperClient = abstractZookeeperTransporter.getCachedConnectedClient(addressList);
        Assertions.assertNotNull(zookeeperClient);
    }

    @Test
    public void testRepeatConnect() {
        URL url = URL.valueOf("zookeeper://127.0.0.1:" + zkServerPort + "/org.apache.dubbo.registry.RegistryService?application=metadatareport-local-xml-provider2&dubbo=2.0.2&interface=org.apache.dubbo.registry.RegistryService&pid=47418&specVersion=2.7.0-SNAPSHOT&timestamp=1547102428828");
        URL url2 = URL.valueOf("zookeeper://127.0.0.1:" + zkServerPort + "/org.apache.dubbo.metadata.store.MetadataReport?address=zookeeper://127.0.0.1:2181&application=metadatareport-local-xml-provider2&cycle-report=false&interface=org.apache.dubbo.metadata.store.MetadataReport&retry-period=4590&retry-times=23&sync-report=true");
        ZookeeperClient newZookeeperClient = abstractZookeeperTransporter.connect(url);
        //just for connected
        newZookeeperClient.getContent("/dubbo/test");
        Assertions.assertEquals(abstractZookeeperTransporter.getZookeeperClientMap().size(), 1);
        Assertions.assertEquals(abstractZookeeperTransporter.getZookeeperClientMap().get("127.0.0.1:" + zkServerPort), newZookeeperClient);
        Assertions.assertTrue(newZookeeperClient.isConnected());

        ZookeeperClient newZookeeperClient2 = abstractZookeeperTransporter.connect(url2);
        //just for connected
        newZookeeperClient2.getContent("/dubbo/test");
        Assertions.assertEquals(newZookeeperClient, newZookeeperClient2);
        Assertions.assertEquals(abstractZookeeperTransporter.getZookeeperClientMap().size(), 1);
        Assertions.assertEquals(abstractZookeeperTransporter.getZookeeperClientMap().get("127.0.0.1:" + zkServerPort), newZookeeperClient);
    }

    @Test
    public void testNotRepeatConnect() throws Exception {
        int zkServerPort2 = NetUtils.getAvailablePort();
        TestingServer zkServer2 = new TestingServer(zkServerPort2, true);

        URL url = URL.valueOf("zookeeper://127.0.0.1:" + zkServerPort + "/org.apache.dubbo.registry.RegistryService?application=metadatareport-local-xml-provider2&dubbo=2.0.2&interface=org.apache.dubbo.registry.RegistryService&pid=47418&specVersion=2.7.0-SNAPSHOT&timestamp=1547102428828");
        URL url2 = URL.valueOf("zookeeper://127.0.0.1:" + zkServerPort + "/org.apache.dubbo.metadata.store.MetadataReport?backup=127.0.0.1:" + zkServerPort2 + "&address=zookeeper://127.0.0.1:2181&application=metadatareport-local-xml-provider2&cycle-report=false&interface=org.apache.dubbo.metadata.store.MetadataReport&retry-period=4590&retry-times=23&sync-report=true");
        ZookeeperClient newZookeeperClient = abstractZookeeperTransporter.connect(url);
        //just for connected
        newZookeeperClient.getContent("/dubbo/test");
        Assertions.assertEquals(abstractZookeeperTransporter.getZookeeperClientMap().size(), 1);
        Assertions.assertEquals(abstractZookeeperTransporter.getZookeeperClientMap().get("127.0.0.1:" + zkServerPort), newZookeeperClient);

        ZookeeperClient newZookeeperClient2 = abstractZookeeperTransporter.connect(url2);
        //just for connected
        newZookeeperClient2.getContent("/dubbo/test");
        Assertions.assertNotEquals(newZookeeperClient, newZookeeperClient2);
        Assertions.assertEquals(abstractZookeeperTransporter.getZookeeperClientMap().size(), 2);

        String cacheAddressStr = "127.0.0.1:" + Math.min(zkServerPort, zkServerPort2) + "," + "127.0.0.1:" + Math.max(zkServerPort, zkServerPort2);
        Assertions.assertEquals(abstractZookeeperTransporter.getZookeeperClientMap().get(cacheAddressStr), newZookeeperClient2);

        zkServer2.stop();
    }

    @Test
    public void testRepeatConnectForBackUpAdd() throws Exception {
        int zkServerPort2 = NetUtils.getAvailablePort();
        TestingServer zkServer2 = new TestingServer(zkServerPort2, true);

        int zkServerPort3 = NetUtils.getAvailablePort();
        TestingServer zkServer3 = new TestingServer(zkServerPort3, true);

        URL url = URL.valueOf("zookeeper://127.0.0.1:" + zkServerPort + "/org.apache.dubbo.registry.RegistryService?backup=127.0.0.1:" + zkServerPort2 + "&application=metadatareport-local-xml-provider2&dubbo=2.0.2&interface=org.apache.dubbo.registry.RegistryService&pid=47418&specVersion=2.7.0-SNAPSHOT&timestamp=1547102428828");
        URL url2 = URL.valueOf("zookeeper://127.0.0.1:" + zkServerPort2 + "/org.apache.dubbo.metadata.store.MetadataReport?backup=127.0.0.1:" + zkServerPort + "&address=zookeeper://127.0.0.1:2181&application=metadatareport-local-xml-provider2&cycle-report=false&interface=org.apache.dubbo.metadata.store.MetadataReport&retry-period=4590&retry-times=23&sync-report=true");
        ZookeeperClient newZookeeperClient = abstractZookeeperTransporter.connect(url);
        //just for connected
        newZookeeperClient.getContent("/dubbo/test");
        Assertions.assertEquals(abstractZookeeperTransporter.getZookeeperClientMap().size(), 1);

        String cacheAddressStr = "127.0.0.1:" + Math.min(zkServerPort, zkServerPort2) + "," + "127.0.0.1:" + Math.max(zkServerPort, zkServerPort2);
        Assertions.assertEquals(abstractZookeeperTransporter.getZookeeperClientMap().get(cacheAddressStr), newZookeeperClient);

        ZookeeperClient newZookeeperClient2 = abstractZookeeperTransporter.connect(url2);
        //just for connected
        newZookeeperClient2.getContent("/dubbo/test");
        Assertions.assertEquals(newZookeeperClient, newZookeeperClient2);
        Assertions.assertEquals(abstractZookeeperTransporter.getZookeeperClientMap().size(), 1);

        String cacheAddressStr1 = "127.0.0.1:" + Math.min(zkServerPort, zkServerPort2) + "," + "127.0.0.1:" + Math.max(zkServerPort, zkServerPort2);
        Assertions.assertEquals(abstractZookeeperTransporter.getZookeeperClientMap().get(cacheAddressStr1), newZookeeperClient2);

        zkServer2.stop();
        zkServer3.stop();
    }

    @Test
    public void testRepeatConnectForNoMatchBackUpAdd() throws Exception {
        int zkServerPort2 = NetUtils.getAvailablePort();
        TestingServer zkServer2 = new TestingServer(zkServerPort2, true);

        int zkServerPort3 = NetUtils.getAvailablePort();
        TestingServer zkServer3 = new TestingServer(zkServerPort3, true);

        URL url = URL.valueOf("zookeeper://127.0.0.1:" + zkServerPort + "/org.apache.dubbo.registry.RegistryService?backup=127.0.0.1:" + zkServerPort3 + "&application=metadatareport-local-xml-provider2&dubbo=2.0.2&interface=org.apache.dubbo.registry.RegistryService&pid=47418&specVersion=2.7.0-SNAPSHOT&timestamp=1547102428828");

        ZookeeperClient newZookeeperClient = abstractZookeeperTransporter.connect(url);
        //just for connected
        newZookeeperClient.getContent("/dubbo/test");
        Assertions.assertEquals(abstractZookeeperTransporter.getZookeeperClientMap().size(), 1);

        //
        URL url1 = URL.valueOf("zookeeper://127.0.0.1:" + zkServerPort3 + "/org.apache.dubbo.registry.RegistryService?backup=127.0.0.1:" + zkServerPort + "&application=metadatareport-local-xml-provider2&dubbo=2.0.2&interface=org.apache.dubbo.registry.RegistryService&pid=47418&specVersion=2.7.0-SNAPSHOT&timestamp=1547102428828");
        URL url2 = URL.valueOf("zookeeper://127.0.0.1:" + zkServerPort2 + "/org.apache.dubbo.metadata.store.MetadataReport?address=zookeeper://127.0.0.1:2181&application=metadatareport-local-xml-provider2&cycle-report=false&interface=org.apache.dubbo.metadata.store.MetadataReport&retry-period=4590&retry-times=23&sync-report=true");

        ZookeeperClient newZookeeperClient1 = abstractZookeeperTransporter.connect(url1);
        //just for connected
        newZookeeperClient1.getContent("/dubbo/test");

        Assertions.assertEquals(abstractZookeeperTransporter.getZookeeperClientMap().size(), 1);

        Assertions.assertEquals(newZookeeperClient, newZookeeperClient1);

        String cacheAddressStr = "127.0.0.1:" + Math.min(zkServerPort, zkServerPort3) + "," + "127.0.0.1:" + Math.max(zkServerPort, zkServerPort3);
        Assertions.assertEquals(abstractZookeeperTransporter.getZookeeperClientMap().get(cacheAddressStr), newZookeeperClient);

        ZookeeperClient newZookeeperClient2 = abstractZookeeperTransporter.connect(url2);
        //just for connected
        newZookeeperClient2.getContent("/dubbo/test");

        Assertions.assertNotEquals(newZookeeperClient, newZookeeperClient2);
        Assertions.assertEquals(abstractZookeeperTransporter.getZookeeperClientMap().size(), 2);

        Assertions.assertEquals(abstractZookeeperTransporter.getZookeeperClientMap().get("127.0.0.1:" + zkServerPort2), newZookeeperClient2);

        zkServer2.stop();
        zkServer3.stop();
    }

    @Test
    public void testSameHostWithDifferentUser() throws Exception {
        int zkPort1 = NetUtils.getAvailablePort();
        int zkPort2 = NetUtils.getAvailablePort();
        try (TestingServer zkServer1 = new TestingServer(zkPort1, true)) {
            try (TestingServer zkServer2 = new TestingServer(zkPort2, true)) {
                URL url1 = URL.valueOf("zookeeper://us1:pw1@127.0.0.1:" + zkPort1 + "/path1");
                URL url2 = URL.valueOf("zookeeper://us2:pw2@127.0.0.1:" + zkPort1 + "/path2");

                ZookeeperClient client1 = abstractZookeeperTransporter.connect(url1);
                ZookeeperClient client2 = abstractZookeeperTransporter.connect(url2);

                assertThat(client1, not(client2));
            }
        }
    }
}
