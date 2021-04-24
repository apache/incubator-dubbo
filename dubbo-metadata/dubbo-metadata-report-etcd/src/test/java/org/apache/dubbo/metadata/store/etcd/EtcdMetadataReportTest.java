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

package org.apache.dubbo.metadata.store.etcd;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.metadata.definition.ServiceDefinitionBuilder;
import org.apache.dubbo.metadata.definition.model.FullServiceDefinition;
import org.apache.dubbo.metadata.report.identifier.MetadataIdentifier;
import org.apache.dubbo.metadata.report.identifier.ServiceMetadataIdentifier;
import org.apache.dubbo.metadata.report.identifier.SubscriberMetadataIdentifier;

import com.google.gson.Gson;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.launcher.EtcdCluster;
import io.etcd.jetcd.launcher.EtcdClusterFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.dubbo.common.constants.CommonConstants.CONSUMER_SIDE;
import static org.apache.dubbo.common.constants.CommonConstants.PROVIDER_SIDE;

/**
 * Unit test for etcd metadata report
 */
@Disabled
public class EtcdMetadataReportTest {

    private static final String TEST_SERVICE = "org.apache.dubbo.metadata.store.etcd.EtcdMetadata4TstService";

    private EtcdCluster etcdCluster = EtcdClusterFactory.buildCluster(getClass().getSimpleName(), 1, false);
    private Client etcdClientForTest;
    private EtcdMetadataReport etcdMetadataReport;
    private URL registryUrl;
    private EtcdMetadataReportFactory etcdMetadataReportFactory;

    @BeforeEach
    public void setUp() {
        etcdCluster.start();
        etcdClientForTest = Client.builder().endpoints(etcdCluster.getClientEndpoints()).build();
        List<URI> clientEndPoints = etcdCluster.getClientEndpoints();
        this.registryUrl = URL.valueOf("etcd://" + clientEndPoints.get(0).getHost() + ":" + clientEndPoints.get(0).getPort());
        etcdMetadataReportFactory = new EtcdMetadataReportFactory();
        this.etcdMetadataReport = (EtcdMetadataReport) etcdMetadataReportFactory.createMetadataReport(registryUrl);
    }

    @AfterEach
    public void tearDown() throws Exception {
        etcdCluster.close();
    }

    @Test
    public void testStoreProvider() throws Exception {
        String version = "1.0.0";
        String group = null;
        String application = "etcd-metdata-report-test";

        String r = etcdMetadataReport.getServiceDefinition(new MetadataIdentifier(TEST_SERVICE, version, group, "provider", application));
        Assertions.assertNull(r);
        MetadataIdentifier providerIdentifier =
                storeProvider(etcdMetadataReport, TEST_SERVICE, version, group, application);

        CompletableFuture<GetResponse> response = etcdClientForTest.getKVClient().get(ByteSequence.from(
                etcdMetadataReport.getNodeKey(providerIdentifier), StandardCharsets.UTF_8));
        String fileContent = response.get().getKvs().get(0).getValue().toString(StandardCharsets.UTF_8);
        Assertions.assertNotNull(fileContent);

        Gson gson = new Gson();
        FullServiceDefinition fullServiceDefinition = gson.fromJson(fileContent, FullServiceDefinition.class);
        Assertions.assertEquals(fullServiceDefinition.getParameters().get("paramTest"), "etcdTest");

        r = etcdMetadataReport.getServiceDefinition(new MetadataIdentifier(TEST_SERVICE, version, group, "provider", application));
        Assertions.assertNotNull(r);
    }

    @Test
    public void testDoSaveMetadata() throws ExecutionException, InterruptedException {
        String version = "1.0.0";
        String group = null;
        String application = "etc-metadata-report-consumer-test";
        String revision = "90980";
        String protocol = "xxx";
        URL url = generateURL(TEST_SERVICE, version, group, application);
        ServiceMetadataIdentifier serviceMetadataIdentifier = new ServiceMetadataIdentifier(TEST_SERVICE, version,
                group, "provider", revision, protocol);
        etcdMetadataReport.doSaveMetadata(serviceMetadataIdentifier, url);

        CompletableFuture<GetResponse> response = etcdClientForTest.getKVClient().get(ByteSequence.from(
                etcdMetadataReport.getNodeKey(serviceMetadataIdentifier), StandardCharsets.UTF_8));
        String fileContent = response.get().getKvs().get(0).getValue().toString(StandardCharsets.UTF_8);
        Assertions.assertNotNull(fileContent);

        Assertions.assertEquals(fileContent, URL.encode(url.toFullString()));
    }

    @Test
    public void testDoRemoveMetadata() throws ExecutionException, InterruptedException {
        String version = "1.0.0";
        String group = null;
        String application = "etc-metadata-report-consumer-test";
        String revision = "90980";
        String protocol = "xxx";
        URL url = generateURL(TEST_SERVICE, version, group, application);
        ServiceMetadataIdentifier serviceMetadataIdentifier = new ServiceMetadataIdentifier(TEST_SERVICE, version,
                group, "provider", revision, protocol);
        etcdMetadataReport.doSaveMetadata(serviceMetadataIdentifier, url);
        CompletableFuture<GetResponse> response = etcdClientForTest.getKVClient().get(ByteSequence.from(
                etcdMetadataReport.getNodeKey(serviceMetadataIdentifier), StandardCharsets.UTF_8));
        String fileContent = response.get().getKvs().get(0).getValue().toString(StandardCharsets.UTF_8);
        Assertions.assertNotNull(fileContent);


        etcdMetadataReport.doRemoveMetadata(serviceMetadataIdentifier);

        response = etcdClientForTest.getKVClient().get(ByteSequence.from(
                etcdMetadataReport.getNodeKey(serviceMetadataIdentifier), StandardCharsets.UTF_8));
        Assertions.assertTrue(response.get().getKvs().isEmpty());
    }

    @Test
    public void testDoGetExportedURLs() throws ExecutionException, InterruptedException {
        String version = "1.0.0";
        String group = null;
        String application = "etc-metadata-report-consumer-test";
        String revision = "90980";
        String protocol = "xxx";
        URL url = generateURL(TEST_SERVICE, version, group, application);
        ServiceMetadataIdentifier serviceMetadataIdentifier = new ServiceMetadataIdentifier(TEST_SERVICE, version,
                group, "provider", revision, protocol);
        etcdMetadataReport.doSaveMetadata(serviceMetadataIdentifier, url);

        List<String> r = etcdMetadataReport.doGetExportedURLs(serviceMetadataIdentifier);
        Assertions.assertTrue(r.size() == 1);

        String fileContent = r.get(0);
        Assertions.assertNotNull(fileContent);

        Assertions.assertEquals(fileContent, url.toFullString());
    }

    @Test
    public void testDoSaveSubscriberData() throws ExecutionException, InterruptedException {
        String version = "1.0.0";
        String group = null;
        String application = "etc-metadata-report-consumer-test";
        String revision = "90980";
        String protocol = "xxx";
        URL url = generateURL(TEST_SERVICE, version, group, application);
        SubscriberMetadataIdentifier subscriberMetadataIdentifier = new SubscriberMetadataIdentifier(application, revision);
        Gson gson = new Gson();
        String r = gson.toJson(Arrays.asList(url));
        etcdMetadataReport.doSaveSubscriberData(subscriberMetadataIdentifier, r);

        CompletableFuture<GetResponse> response = etcdClientForTest.getKVClient().get(ByteSequence.from(
                etcdMetadataReport.getNodeKey(subscriberMetadataIdentifier), StandardCharsets.UTF_8));
        String fileContent = response.get().getKvs().get(0).getValue().toString(StandardCharsets.UTF_8);
        Assertions.assertNotNull(fileContent);

        Assertions.assertEquals(fileContent, r);
    }

    @Test
    public void testDoGetSubscribedURLs() throws ExecutionException, InterruptedException {
        String version = "1.0.0";
        String group = null;
        String application = "etc-metadata-report-consumer-test";
        String revision = "90980";
        String protocol = "xxx";
        URL url = generateURL(TEST_SERVICE, version, group, application);
        SubscriberMetadataIdentifier subscriberMetadataIdentifier = new SubscriberMetadataIdentifier(application, revision);
        Gson gson = new Gson();
        String r = gson.toJson(Arrays.asList(url));
        etcdMetadataReport.doSaveSubscriberData(subscriberMetadataIdentifier, r);

        CompletableFuture<GetResponse> response = etcdClientForTest.getKVClient().get(ByteSequence.from(
                etcdMetadataReport.getNodeKey(subscriberMetadataIdentifier), StandardCharsets.UTF_8));
        String fileContent = etcdMetadataReport.doGetSubscribedURLs(subscriberMetadataIdentifier);
        Assertions.assertNotNull(fileContent);

        Assertions.assertEquals(fileContent, r);
    }

    private MetadataIdentifier storeProvider(EtcdMetadataReport etcdMetadataReport, String interfaceName, String version,
                                             String group, String application)
            throws ClassNotFoundException, InterruptedException {
        URL url = URL.valueOf("xxx://" + NetUtils.getLocalAddress().getHostName() + ":4444/" + interfaceName +
                "?paramTest=etcdTest&version=" + version + "&application="
                + application + (group == null ? "" : "&group=" + group));

        MetadataIdentifier providerMetadataIdentifier =
                new MetadataIdentifier(interfaceName, version, group, PROVIDER_SIDE, application);
        Class interfaceClass = Class.forName(interfaceName);
        FullServiceDefinition fullServiceDefinition =
                ServiceDefinitionBuilder.buildFullDefinition(interfaceClass, url.getParameters());

        etcdMetadataReport.storeProviderMetadata(providerMetadataIdentifier, fullServiceDefinition);
        Thread.sleep(1000);
        return providerMetadataIdentifier;
    }

    private URL generateURL(String interfaceName, String version, String group, String application) {
        URL url = URL.valueOf("xxx://" + NetUtils.getLocalAddress().getHostName() + ":8989/" + interfaceName +
                "?paramTest=etcdTest&version=" + version + "&application="
                + application + (group == null ? "" : "&group=" + group));
        return url;
    }

}
